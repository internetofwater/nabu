// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"io"
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
	// StoreWithoutServersideHash streams the contents from the reader into a named destination
	// but does not guarantee that the storage provider will create a hash for it.
	// If reading from the reader fails, the destination is left unchanged
	StoreWithoutServersideHash(ObjectPath, io.Reader) error
	// Get returns a reader to the stored file
	Get(ObjectPath) (io.ReadCloser, error)
	// Exists returns true if the file exists
	Exists(ObjectPath) (bool, error)
	// ListDir returns a list of objects in the directory
	ListDir(ObjectPath) (Set, error)
	// Remove removes the file
	Remove(ObjectPath) error
}
