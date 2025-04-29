package gleaner

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBasename(t *testing.T) {
	testCases := []struct {
		url        string
		expected   string
		shouldFail bool
	}{
		{
			url:      "https://geoconnex.us/sitemap.xml",
			expected: "https://geoconnex.us",
		},
		{
			url:      "https://google.com",
			expected: "https://google.com",
		},
		{
			url:      "https://geoconnex.us/",
			expected: "https://geoconnex.us",
		},
	}

	for _, tc := range testCases {
		basename, err := getHostname(tc.url)
		if tc.shouldFail {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			assert.Equal(t, tc.expected, basename)
		}
	}

}

func TestRobots(t *testing.T) {
	testCases := []struct {
		url            string
		allowsCrawling bool
		shouldFail     bool
	}{
		{
			url:            "https://waterdata.usgs.gov/robots.txt",
			allowsCrawling: true,
		},
		{
			url:            "https://google.com",
			allowsCrawling: true,
		},
		{
			url:            "https://geoconnex.us/",
			allowsCrawling: true,
		},
		{
			url:            "https://geoconnex.us/usgs/monitoring-location/430208087543202",
			allowsCrawling: true,
		},
	}

	for _, tc := range testCases {
		robotstxt, err := newRobots(tc.url)
		if tc.shouldFail {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			allowed := robotstxt.Test(gleanerAgent)
			assert.Equal(t, tc.allowsCrawling, allowed)
		}
	}

}

func FuzzCopyReaderAndReturnHash(f *testing.F) {
	// Seed with example inputs
	f.Add([]byte("test data"))
	f.Add([]byte("test data2"))

	f.Fuzz(func(t *testing.T, input []byte) {
		// Run function with fuzz input
		reader := bytes.NewReader(input)

		readerCopy, hash, err := copyReaderAndGenerateHashFilename(reader)
		require.NoError(t, err)

		// Read copied data
		copiedData, err := io.ReadAll(readerCopy)
		require.NoError(t, err)

		// The copied data should equal the input
		require.Equal(t, string(copiedData), string(input), "copied data should match input")

		// rehash to verify correctness
		expectedHash := fmt.Sprintf("%x.jsonld", md5.Sum(input))
		require.Equal(t, hash, expectedHash, "hash should match expected MD5")
	})
}
