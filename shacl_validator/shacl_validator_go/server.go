// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package shacl_validator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

// The largest request body accepted by /validate; matches the
// max message size of the python gRPC server
const MaxRequestBodySize = 32 * 1024 * 1024 // 32 MB

// The response body of a successful /validate request
type ValidationResponse struct {
	Valid   bool   `json:"valid"`
	Message string `json:"message"`
}

// The response body of a failed request
type ErrorResponse struct {
	Detail string `json:"detail"`
}

// NewHttpHandler returns an http handler that serves the same
// /validate and /shape routes as the python SHACL validator server
func NewHttpHandler(validator *ShaclValidator) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /validate", func(w http.ResponseWriter, r *http.Request) {
		validateHandler(validator, w, r)
	})
	mux.HandleFunc("GET /shape", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/turtle; charset=utf-8")
		_, _ = io.WriteString(w, validator.ShapeTurtle())
	})
	return corsMiddleware(mux)
}

func writeJson(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	// match python's json output which does not escape <, >, and &
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(body); err != nil {
		log.Errorf("failed to write response: %v", err)
	}
}

// jsonldFromBody extracts the jsonld document from the request body.
// Clients may send the jsonld either as a JSON object or array, or as a
// JSON string containing the serialized jsonld; both are accepted
func jsonldFromBody(body []byte) (jsonld string, empty bool, err error) {
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", false, err
	}
	switch v := decoded.(type) {
	case nil:
		return "", true, nil
	case string:
		return v, v == "", nil
	case map[string]any:
		return string(body), len(v) == 0, nil
	case []any:
		return string(body), len(v) == 0, nil
	case bool:
		return "", !v, fmt.Errorf("expected JSON-LD but got a boolean")
	case float64:
		return "", v == 0, fmt.Errorf("expected JSON-LD but got a number")
	default:
		return "", false, fmt.Errorf("unexpected JSON type %T", v)
	}
}

func validateHandler(validator *ShaclValidator, w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxRequestBodySize))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeJson(w, http.StatusRequestEntityTooLarge, ErrorResponse{Detail: err.Error()})
			return
		}
		writeJson(w, http.StatusInternalServerError, ErrorResponse{Detail: err.Error()})
		return
	}

	jsonld, empty, err := jsonldFromBody(body)
	if empty {
		writeJson(w, http.StatusBadRequest, ErrorResponse{Detail: "Missing JSON data in request"})
		return
	}
	// the python server returns a 500 for all errors, including malformed input,
	// so we do the same to be a drop in replacement
	if err != nil {
		log.Errorf("Validation failed: %v", err)
		writeJson(w, http.StatusInternalServerError, ErrorResponse{Detail: err.Error()})
		return
	}

	report, err := validator.ValidateJsonldString(jsonld)
	if err != nil {
		log.Errorf("Validation failed: %v", err)
		writeJson(w, http.StatusInternalServerError, ErrorResponse{Detail: err.Error()})
		return
	}
	writeJson(w, http.StatusOK, ValidationResponse{Valid: report.Conforms, Message: ReportText(report)})
}

// corsMiddleware allows requests from all origins, mirroring
// the CORS configuration of the python server
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		isPreflight := r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""
		if !isPreflight {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		if requested := r.Header.Get("Access-Control-Request-Headers"); requested != "" {
			w.Header().Set("Access-Control-Allow-Headers", requested)
		}
		w.Header().Set("Access-Control-Max-Age", "600")
		w.Header().Add("Vary", "Origin")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "OK")
	})
}

// Serve the SHACL validation endpoints on the given address until the context is cancelled
func Serve(ctx context.Context, validator *ShaclValidator, addr string) error {
	server := &http.Server{
		Addr:              addr,
		Handler:           NewHttpHandler(validator),
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.ListenAndServe()
	}()
	log.Infof("HTTP server started on %s; validate data by sending JSON-LD in the body of a POST to /validate", addr)

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		log.Info("Shutting down SHACL validation server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}
