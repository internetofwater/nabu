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
	dir, err := os.MkdirTemp("", "nabu-gleaner-")
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

// StoreWithServersideHash saves the contents from the reader into a file named after `object`
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

func (l *LocalTempFSCrawlStorage) ListDir(prefix string) (Set, error) {
	dirPath := filepath.Join(l.baseDir, prefix)

	entries, err := os.ReadDir(dirPath)
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

func (l *LocalTempFSCrawlStorage) IsEmptyDir(dir ObjectPath) (bool, error) {
	files, err := os.ReadDir(filepath.Join(l.baseDir, dir))
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	} else if err != nil {
		return false, err
	}

	return len(files) == 0, nil
}

func (l *LocalTempFSCrawlStorage) GetHash(object string) (Md5Hash, error) {
	return "", nil
}
