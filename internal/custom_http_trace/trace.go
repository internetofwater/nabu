package custom_http_trace

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
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Define a custom type for context keys to avoid collisions.
type contextKey string

const (
	urlContextKey    contextKey = "url"
	callerContextKey contextKey = "caller"
)

const trace_file = "http_trace.csv"

var (
	csvFile   *os.File
	csvWriter *csv.Writer
	mu        sync.Mutex
)

func init() {
	var err error

	// Open the file with O_TRUNC to clear it if it exists
	csvFile, err = os.OpenFile(trace_file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("Failed to open CSV file: %v", err)
	}

	csvWriter = csv.NewWriter(csvFile)

	// Write CSV header
	_ = csvWriter.Write([]string{
		"Timestamp", "Event", "Duration (µs)", "Target Address", "Connection Reused", "Requested URL", "Error", "Caller",
	})
	csvWriter.Flush()
}

func logTraceToCSV(event, addr string, duration time.Duration, reused bool, url string, err error, parentFuncName string) {
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
		time.Now().Format(time.RFC3339), // Timestamp
		event,                           // Event Type
		fmt.Sprintf("%d", duration/time.Microsecond), // time as microseconds
		addr,           // Target Address
		reusedStr,      // Connection Reused
		url,            // Requested URL
		errStr,         // Error Message
		parentFuncName, // The function that initiated the request
	})

	if err != nil {
		log.Fatal(err)
	}

	csvWriter.Flush()
}

// create an httptrace.ClientTrace that tracks request timing.
func getHttpTrace(ctx context.Context) *httptrace.ClientTrace {
	var (
		dnsStart,
		connEnd, connStart,
		tlsHandShakeStart time.Time
	)

	// Retrieve the requested URL from the context
	requestedURL, ok := ctx.Value(urlContextKey).(string)
	if !ok || requestedURL == "" {
		log.Fatal("Failed to retrieve requested URL from context")
	}
	caller, ok := ctx.Value(callerContextKey).(string)
	if !ok || caller == "" {
		log.Fatal("Failed to retrieve parent function name from context")
	}

	trace := &httptrace.ClientTrace{
		// Runs when a connection is newly created OR reused
		GetConn: func(hostPort string) {
			connStart = time.Now()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			connEnd = time.Now()
			logTraceToCSV("GotConn", info.Conn.RemoteAddr().String(), connEnd.Sub(connStart), info.Reused, requestedURL, nil, caller)
		},

		DNSStart: func(info httptrace.DNSStartInfo) {
			dnsStart = time.Now()
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			logTraceToCSV("DNS", info.Addrs[0].String(), time.Since(dnsStart), false, requestedURL, info.Err, caller)
		},
		TLSHandshakeStart: func() {
			tlsHandShakeStart = time.Now()
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			logTraceToCSV("TLSHandshake", state.ServerName, time.Since(tlsHandShakeStart), false, requestedURL, err, caller)
		},
		PutIdleConn: func(err error) {
			logTraceToCSV("PutIdleConn", "", 0, false, requestedURL, err, caller)
		},
		GotFirstResponseByte: func() {
			logTraceToCSV("GotFirstResponseByte", "", time.Since(connEnd), false, requestedURL, nil, caller)
		},
	}

	return trace
}

// NewRequestWithContext returns a new HTTP request and stores the URL and caller for future logging.
func NewRequestWithContext(method, url string, body io.Reader) (*http.Request, error) {
	if body == nil {
		return nil, fmt.Errorf("body is nil")
	}

	ctx := context.Background()

	ctx = context.WithValue(ctx, urlContextKey, url)

	pc, file, line, ok := runtime.Caller(1)
	if !ok {
		return nil, fmt.Errorf("unable to get caller information")
	}
	callerName := runtime.FuncForPC(pc).Name()
	callerInfo := fmt.Sprintf("%s (%s:%d)", callerName, file, line)

	ctx = context.WithValue(ctx, callerContextKey, callerInfo)

	trace := getHttpTrace(ctx)
	traceCtx := httptrace.WithClientTrace(ctx, trace)

	return http.NewRequestWithContext(traceCtx, method, url, body)
}

func SortTraceHttpInCurrentDir() error {
	// Open the CSV file
	file, err := os.Open(trace_file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No need to sort if the file doesn't exist
		}
		return fmt.Errorf("failed to open CSV file: %v", err)
	}
	defer file.Close()

	// Read the CSV content
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV file: %v", err)
	}

	// Ensure there's at least one row besides the header
	if len(records) <= 1 {
		return nil
	}

	sort.SliceStable(records[1:], func(i, j int) bool {
		// Convert the duration values to integers for sorting
		d1, err1 := strconv.Atoi(records[i+1][2])
		d2, err2 := strconv.Atoi(records[j+1][2])

		if err1 != nil || err2 != nil {
			log.Fatal(fmt.Errorf("failed to convert duration to integer: %v, %v", err1, err2))
		}

		// Sort in **descending order** (biggest duration first)
		return d1 > d2
	})

	file, err = os.OpenFile(trace_file, os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to open CSV file for writing: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)

	err = writer.WriteAll(records)
	if err != nil {
		return fmt.Errorf("failed to write sorted records: %v", err)
	}

	writer.Flush()
	return nil
}
