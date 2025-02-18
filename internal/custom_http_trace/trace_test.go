package custom_http_trace

import (
	"encoding/csv"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Helper function to create a test CSV file
func createTestCSV(t *testing.T, filePath string, records [][]string) {
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Failed to create test CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	err = writer.WriteAll(records)
	if err != nil {
		t.Fatalf("Failed to write test CSV file: %v", err)
	}
	writer.Flush()
}

// Helper function to read the CSV file for validation
func readCSV(t *testing.T, filePath string) [][]string {
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open test CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read test CSV file: %v", err)
	}
	return records
}

func TestSortTraceHttpInCurrentDir(t *testing.T) {

	// Test data with shuffled durations
	testData := [][]string{
		{"Timestamp", "Event", "Duration (µs)", "Target Address", "Connection Reused", "Requested URL", "Error", "Caller"},
		{"01-01 11:01:01", "ConnectDone", "300", "192.168.1.1", "false", "http://example.com", "", "caller1"},
		{"01-01 11:01:02", "ConnectDone", "100", "192.168.1.2", "false", "http://example.com", "", "caller2"},
		{"01-01 11:01:03", "ConnectDone", "200", "192.168.1.3", "false", "http://example.com", "", "caller3"},
		{"01-01 11:01:04", "ConnectDone", "900", "192.168.1.3", "false", "http://example.com", "", "caller3"},
		{"01-01 11:01:04", "ConnectDone", "400", "192.168.1.3", "false", "http://example.com", "", "caller3"},
	}

	// Create test CSV file
	createTestCSV(t, trace_file, testData)
	defer os.Remove(trace_file) // Cleanup after test

	// Run the sort function
	err := SortTraceHttpInCurrentDir()
	require.NoError(t, err)

	// Read the sorted file
	sortedData := readCSV(t, trace_file)

	require.Equal(t, testData[0], sortedData[0])
	require.Len(t, sortedData, len(testData))

	// Validate sorting order (descending by duration)
	const durationColumn = 2
	require.Equal(t, "900", sortedData[1][durationColumn])
	require.Equal(t, "400", sortedData[2][durationColumn])
	require.Equal(t, "300", sortedData[3][durationColumn])
	require.Equal(t, "200", sortedData[4][durationColumn])
	require.Equal(t, "100", sortedData[5][durationColumn])
}
