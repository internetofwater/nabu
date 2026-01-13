// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package storage

import "io"

var _ CrawlStorage = DiscardCrawlStorage{}

// DiscardCrawlStorage is a CrawlStorage that stores nothing and is useful for testing
type DiscardCrawlStorage struct{}

func (DiscardCrawlStorage) StoreMetadata(string, io.Reader) error {
	return nil
}

func (DiscardCrawlStorage) StoreWithHash(string, io.Reader, int) error {
	return nil
}

func (DiscardCrawlStorage) StoreWithoutServersideHash(string, io.Reader) error {
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

func (DiscardCrawlStorage) IsEmptyDir(ObjectPath) (bool, error) {
	return true, nil
}

func (DiscardCrawlStorage) GetHash(string) (Md5Hash, bool, error) {
	return "", false, nil
}
