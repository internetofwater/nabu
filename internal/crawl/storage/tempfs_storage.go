// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

// Storage for crawl data where the files
// are stored on disk; useful for debugging and
// and local tests
type LocalTempFSCrawlStorage struct {
	// the directory used for storing all tmp files
	baseDir string
}

var _ CrawlStorage = &LocalTempFSCrawlStorage{}

// NewLocalTempFSCrawlStorage creates a new storage with a temporary base directory
func NewLocalTempFSCrawlStorage() (*LocalTempFSCrawlStorage, error) {
	dir, err := os.MkdirTemp("", "nabu-crawl-storage-*")
	if err != nil {
		return nil, err
	}
	return &LocalTempFSCrawlStorage{baseDir: dir}, nil
}

// Storing metadata locally is the same as storing data
func (l *LocalTempFSCrawlStorage) StoreMetadata(name string, reader io.Reader) error {
	return l.StoreWithHash(name, reader, -1)
}

func (l *LocalTempFSCrawlStorage) StoreWithoutServersideHash(name string, reader io.Reader) error {
	return l.StoreWithHash(name, reader, -1)
}

// StoreWithServersideHash saves the contents from the reader into a file named after `object`.
// The data is written to a temporary file first so that a failed write never leaves a
// partial file in place of a previous one
func (l *LocalTempFSCrawlStorage) StoreWithHash(name string, reader io.Reader, sizeInBytes int) error {

	if l.baseDir == "" {
		return fmt.Errorf("baseDir is empty")
	}

	destPath := filepath.Join(l.baseDir, name)

	log.Tracef("saving data to %s", destPath)

	// Make sure directory exists
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(destPath), ".tmp-"+filepath.Base(destPath)+"-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	if _, err = io.Copy(tmpFile, reader); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	return os.Rename(tmpFile.Name(), destPath)
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

func (l *LocalTempFSCrawlStorage) ListDir(prefix string) (Set, error) {
	dirPath := filepath.Join(l.baseDir, prefix)

	entries, err := os.ReadDir(dirPath)
	if errors.Is(err, os.ErrNotExist) {
		return make(Set), nil
	}
	if err != nil {
		return nil, err
	}

	set := make(Set)
	for _, entry := range entries {
		fullPath := filepath.Join(dirPath, entry.Name())
		set.Add(fullPath)
	}

	return set, nil
}
func (l *LocalTempFSCrawlStorage) Remove(object string) error {
	return os.Remove(filepath.Join(l.baseDir, object))
}
