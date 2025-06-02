// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

type BatchFileObject struct {
	Path   string
	Reader io.Reader
}

type BatchCrawlStorage interface {
	CrawlStorage
	BatchStore(chan BatchFileObject) error
}

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
func (l *LocalTempFSCrawlStorage) Store(name string, reader io.Reader) error {

	if l.baseDir == "" {
		return fmt.Errorf("baseDir is empty")
	}

	destPath := filepath.Join(l.baseDir, name)

	log.Debugf("saving data to %s", destPath)

	// Make sure directory exists
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return err
	}

	destFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer func() { _ = destFile.Close() }()

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
