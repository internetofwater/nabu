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

type BatchFileObject struct {
	Path   string
	Reader io.Reader
}

type BatchCrawlStorage interface {
	CrawlStorage
	BatchStore(chan BatchFileObject) error
}

// a path delimited by /
type objectPath = string

// a unique set of object paths with quick lookup
type Set map[objectPath]struct{}

func (s Set) Contains(key objectPath) bool {
	_, ok := s[key]
	return ok
}

func (s Set) Add(key objectPath) {
	s[key] = struct{}{}
}

// A storage interface that stores crawl data
type CrawlStorage interface {
	// Store metadata about the crawl into a named destination
	// This may be in a different place than normal storage since it is intended to be
	// read publicly and drive UIs
	StoreMetadata(objectPath, io.Reader) error
	// Store saves the contents from the reader into a named destination
	Store(objectPath, io.Reader) error
	// Get returns a reader to the stored file
	Get(objectPath) (io.ReadCloser, error)
	// Exists returns true if the file exists
	Exists(objectPath) (bool, error)
	// ListDir returns a list of objects in the directory
	ListDir(objectPath) (Set, error)
	// Remove removes the file
	Remove(objectPath) error
	// IsEmptyDir returns true if the directory is empty
	IsEmptyDir(objectPath) (bool, error)
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

// Storing metadata locally is the same as storing data
func (l *LocalTempFSCrawlStorage) StoreMetadata(name string, reader io.Reader) error {
	return l.Store(name, reader)
}

// Store saves the contents from the reader into a file named after `object`
func (l *LocalTempFSCrawlStorage) Store(name string, reader io.Reader) error {

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
	filepath := filepath.Join(l.baseDir, prefix)

	files, err := os.ReadDir(filepath)
	if err != nil {
		return nil, err
	}

	set := make(Set)
	for _, file := range files {
		set[objectPath(file.Name())] = struct{}{}
	}

	return set, nil
}

func (l *LocalTempFSCrawlStorage) Remove(object string) error {
	return os.Remove(filepath.Join(l.baseDir, object))
}

func (l *LocalTempFSCrawlStorage) IsEmptyDir(dir objectPath) (bool, error) {
	files, err := os.ReadDir(filepath.Join(l.baseDir, dir))
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	} else if err != nil {
		return false, err
	}

	return len(files) == 0, nil
}

// DiscardCrawlStorage is a CrawlStorage that stores nothing and is useful for testing
type DiscardCrawlStorage struct {
}

func (DiscardCrawlStorage) StoreMetadata(string, io.Reader) error {
	return nil
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

func (DiscardCrawlStorage) Remove(string) error {
	return nil
}

func (DiscardCrawlStorage) ListDir(string) (Set, error) {
	return make(Set), nil
}

func (DiscardCrawlStorage) IsEmptyDir(objectPath) (bool, error) {
	return true, nil
}

var _ CrawlStorage = DiscardCrawlStorage{}
var _ CrawlStorage = &LocalTempFSCrawlStorage{}
