package gleaner

import (
	"io"
	"os"
	"path/filepath"
)

type CrawlStorage interface {
	Store(string, io.Reader) error
	Get(string) (io.ReadCloser, error)
	Exists(string) (bool, error)
}

// Storage for crawl data where the files
// are stored on disk; useful for debugging and
// and local tests
type LocalTempFSCrawlStorage struct {
	// the directory used for storing all tmp files
	baseDir string
}

// NewLocalTempFSCrawlStorage creates a new storage with a temporary base directory
func NewLocalTempFSCrawlStorage() (*LocalTempFSCrawlStorage, error) {
	dir, err := os.MkdirTemp("", "nabu-gleaner-")
	if err != nil {
		return nil, err
	}
	return &LocalTempFSCrawlStorage{baseDir: dir}, nil
}

// Store saves the contents from the reader into a file named after `object`
func (l *LocalTempFSCrawlStorage) Store(object string, reader io.Reader) error {
	destPath := filepath.Join(l.baseDir, object)

	// Make sure directory exists
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return err
	}

	destFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, reader)
	return err
}

// Get returns a reader to the stored file
func (l *LocalTempFSCrawlStorage) Get(object string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(l.baseDir, object))
}

// Exists checks if the file Exists
func (l *LocalTempFSCrawlStorage) Exists(object string) (bool, error) {
	_, err := os.Stat(filepath.Join(l.baseDir, object))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

type DiscardCrawlStorage struct {
}

func (DiscardCrawlStorage) Store(string, io.Reader) error {
	return nil
}
func (DiscardCrawlStorage) Get(string) (io.ReadCloser, error) {
	return nil, nil
}
func (DiscardCrawlStorage) Exists(string) (bool, error) {
	return false, nil
}
