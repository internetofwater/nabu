// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// a path delimited by /
type ObjectPath = string

// a unique set of object paths with quick lookup
type Set map[ObjectPath]struct{}

// Returns true if the key is in the set
func (s Set) Contains(key ObjectPath) bool {
	_, ok := s[key]
	return ok
}

// Add a key to the set
func (s Set) Add(key ObjectPath) {
	s[key] = struct{}{}
}

// A hash of a file generated from the md5 algorithm
type Md5Hash = string

// A storage interface that stores crawl data
type CrawlStorage interface {
	// Store metadata about the crawl into a named destination
	// This may be in a different place than normal storage since it is intended to be
	// read publicly and drive UIs
	StoreMetadata(ObjectPath, io.Reader) error
	// StoreWithServersideHash saves the contents from the reader into a named destination
	// and guarantees that the storage provider will create a hash for it that can be retrieved
	StoreWithHash(path ObjectPath, data io.Reader, byteLength int) error
	// StoreWithoutServersideHash saves the contents from the reader into a named destination
	// but does not guarantee that the storage provider will create a hash for it
	StoreWithoutServersideHash(ObjectPath, io.Reader) error
	// Get returns a reader to the stored file
	Get(ObjectPath) (io.ReadCloser, error)
	// Exists returns true if the file exists
	Exists(ObjectPath) (bool, error)
	// ListDir returns a list of objects in the directory
	ListDir(ObjectPath) (Set, error)
	// Remove removes the file
	Remove(ObjectPath) error
	// IsEmptyDir returns true if the directory is empty
	IsEmptyDir(ObjectPath) (bool, error)
	// Get the hash of the file
	GetHash(ObjectPath) (hash Md5Hash, file_exists bool, err error)
}

// Given a storage path, iterate through it and remove any files that aren't in sitesToKeep
func CleanupFiles(pathInStorage string, sitesToKeep Set, storage CrawlStorage) ([]string, error) {
	if pathInStorage == "" {
		return nil, fmt.Errorf("path is empty")
	}
	if !strings.Contains(pathInStorage, "/") {
		return nil, fmt.Errorf("path should not be just one filename but got: %s", pathInStorage)
	}
	if strings.HasPrefix(pathInStorage, "/") {
		return nil, fmt.Errorf("path should not be absolute and start with / but got %s", pathInStorage)
	}
	if len(sitesToKeep) == 0 {
		return nil, fmt.Errorf("sitesToKeep is empty")
	}

	files, err := storage.ListDir(pathInStorage)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	var (
		pathsDeleted []string
		mu           sync.Mutex // protect shared slice
	)

	eg, ctx := errgroup.WithContext(context.Background())
	const maxConcurrency = 10
	eg.SetLimit(maxConcurrency)

	exitingEarly := atomic.Bool{}
	exitingEarly.Store(false)

	for absPath := range files {

		index := strings.Index(absPath, pathInStorage)
		if index == -1 {
			return nil, fmt.Errorf("unexpected path format: %s", absPath)
		}
		relativePath := absPath[index:]

		if sitesToKeep.Contains(relativePath) {
			continue
		}

		eg.Go(func() error {
			// Check if context is already canceled due to another error
			if ctx.Err() != nil {
				// Only log the ctx cancell error once
				if !exitingEarly.Load() {
					log.Error("Context was cancelled; exiting early")
					exitingEarly.Store(true)
				}
				return ctx.Err()
			}

			if err := storage.Remove(relativePath); err != nil {
				log.Errorf("Error cleaning up outdated file %s: %v", absPath, err)
				return err
			}

			mu.Lock()
			pathsDeleted = append(pathsDeleted, absPath)
			mu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		// At this point, all other goroutines that haven't started will be canceled
		return pathsDeleted, err
	}

	return pathsDeleted, nil
}
