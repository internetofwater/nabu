package trace

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync"
	"time"
)

type contextKey string

const urlContextKey contextKey = "requested_url"

var (
	csvFile   *os.File
	csvWriter *csv.Writer
	mu        sync.Mutex
)

func init() {
	var err error
	csvFile, err = os.OpenFile("http_trace.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open CSV file: %v", err)
	}

	csvWriter = csv.NewWriter(csvFile)

	// Write CSV header
	_ = csvWriter.Write([]string{
		"Timestamp", "Event", "Duration (µs)", "Target Address", "Connection Reused", "Requested URL", "Error",
	})
	csvWriter.Flush()
}

func logTraceToCSV(event, addr string, duration time.Duration, reused bool, url string, err error) {
	mu.Lock()
	defer mu.Unlock()

	reusedStr := ""
	if reused {
		reusedStr = "true"
	}

	errStr := ""
	if err != nil {
		errStr = err.Error()
	}

	err = csvWriter.Write([]string{
		time.Now().Format(time.RFC3339Nano), // Timestamp
		event,                               // Event Type
		time.Duration(duration).String(),    // Duration in microseconds
		addr,                                // Target Address
		reusedStr,                           // Connection Reused
		url,                                 // Requested URL
		errStr,                              // Error Message
	})

	if err != nil {
		log.Fatal(err)
	}

	csvWriter.Flush()
}

// getHttpTrace creates an httptrace.ClientTrace that tracks request timing.
func getHttpTrace(ctx context.Context) *httptrace.ClientTrace {
	var (
		dnsStart, dnsEnd, connStart,
		connEnd, connectStart, connectEnd,
		tlsHandShakeStart, tlsHandShakeEnd time.Time
	)

	// Retrieve the requested URL from the context
	requestedURL, ok := ctx.Value(urlContextKey).(string)
	if !ok || requestedURL == "" {
		log.Fatal("Failed to retrieve requested URL from context") // <- This was causing the crash
	}

	trace := &httptrace.ClientTrace{
		GetConn: func(hostPort string) {
			connStart = time.Now()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			connEnd = time.Now()
			logTraceToCSV("GotConn", info.Conn.RemoteAddr().String(), connEnd.Sub(connStart), info.Reused, requestedURL, nil)
		},
		ConnectStart: func(network, addr string) {
			connectStart = time.Now()
		},
		ConnectDone: func(network, addr string, err error) {
			connectEnd = time.Now()
			logTraceToCSV("ConnectDone", addr, connectEnd.Sub(connectStart), false, requestedURL, err)
		},
		DNSStart: func(info httptrace.DNSStartInfo) {
			dnsStart = time.Now()
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			dnsEnd = time.Now()
			logTraceToCSV("DNS", info.Addrs[0].String(), dnsEnd.Sub(dnsStart), false, requestedURL, info.Err)
		},
		TLSHandshakeStart: func() {
			tlsHandShakeStart = time.Now()
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			tlsHandShakeEnd = time.Now()
			logTraceToCSV("TLSHandshake", state.ServerName, tlsHandShakeEnd.Sub(tlsHandShakeStart), false, requestedURL, err)
		},
		PutIdleConn: func(err error) {
			logTraceToCSV("PutIdleConn", "", 0, false, requestedURL, err)
		},
	}

	return trace
}

// NewRequestWithContext returns a new HTTP request and stores the URL for future logging
func NewRequestWithContext(method, url string, body io.Reader) (*http.Request, error) {
	if body == nil {
		return nil, fmt.Errorf("body is nil")
	}

	ctx := context.WithValue(context.Background(), urlContextKey, url)

	trace := getHttpTrace(ctx)

	traceCtx := httptrace.WithClientTrace(ctx, trace)

	return http.NewRequestWithContext(traceCtx, method, url, body)
}
