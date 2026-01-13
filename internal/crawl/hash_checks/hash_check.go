// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package hashchecks

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/crawl/url_info"
	"github.com/internetofwater/nabu/pkg"
	log "github.com/sirupsen/logrus"
)

type hashChecker struct {
	httpClient *http.Client
	storage    storage.CrawlStorage
}

func NewHashChecker(httpClient *http.Client, storage storage.CrawlStorage) *hashChecker {
	return &hashChecker{
		httpClient: httpClient,
		storage:    storage,
	}
}

// Get the hash of the remote jsonld by using the Content-Digest header
// This gets us metadata about the file without needing to download it fully
func (hc *hashChecker) getJsonldHashFromAPI(url url_info.URL) (storage.Md5Hash, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodHead, url.Loc, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("User-Agent", common.HarvestAgent)
	// We request md5 since it is the same hash
	// as what is provided by the ETag in minio
	req.Header.Set("Want-Content-Digest", "md5")
	req.Header.Set("Accept", "application/ld+json")

	resp, err := hc.httpClient.Do(req)
	if err != nil {
		return "", err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return "", nil
	}
	hash := resp.Header.Get("content-digest")

	// make sure we get just the hash value
	// not the metadata about the hash itself
	trimmed := strings.TrimPrefix(hash, "md5=")
	trimmed = strings.TrimPrefix(trimmed, "md5-")
	trimmed = strings.TrimSpace(trimmed)

	return trimmed, nil
}

type HashCheckResult struct {
	// the server responded with a hash in the response headers of the HEAD request
	ServerProvidedHash bool
	// a file with the requested hash was found in the storage
	FileAlreadyExists bool
	// the resulting path in which the hash was found
	PathInStorage storage.ObjectPath
}

// Check to determine if the file with the hash already exists in storage
func (hc *hashChecker) CheckIfAlreadyExists(url url_info.URL, sitemapId string) (HashCheckResult, error) {

	remoteHash, err := hc.getJsonldHashFromAPI(url)
	var maxErr *common.MaxRetryError
	if errors.As(err, &maxErr) {
		return HashCheckResult{}, pkg.UrlCrawlError{Url: url.Loc, Message: err.Error()}
	}
	if err != nil {
		return HashCheckResult{}, fmt.Errorf("failed to get hash for %s: %w", url.Loc, err)
	}
	var expectedLocationInStorage string
	if remoteHash == "" {
		log.Tracef("%s did not provide a hash to compare for caching", url.Loc)
		return HashCheckResult{
			ServerProvidedHash: false,
		}, nil
	}

	hashCheckMetadata := HashCheckResult{
		ServerProvidedHash: true,
	}

	// the location in storage is the base64 encoded URL with .jsonld extension
	expectedLocationInStorage = "summoned/" + sitemapId + "/" + url.Base64Loc + ".jsonld"
	storageHash, file_exists, err := hc.storage.GetHash(expectedLocationInStorage)
	if err != nil {
		return hashCheckMetadata, err
	}
	if !file_exists {
		log.Tracef("%s does not exist in %s", expectedLocationInStorage, "storage")
		return HashCheckResult{
			ServerProvidedHash: true,
			FileAlreadyExists:  false,
		}, nil
	}

	if storageHash == remoteHash {
		log.Tracef("skipping %s because it already exists in %s", url.Loc, expectedLocationInStorage)
		hashCheckMetadata.PathInStorage = expectedLocationInStorage
		hashCheckMetadata.FileAlreadyExists = true
		return hashCheckMetadata, nil
	}
	log.Tracef("%s does not exist in the bucket", expectedLocationInStorage)

	return hashCheckMetadata, nil
}
