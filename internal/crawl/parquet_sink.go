// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/parquettable"
	"github.com/internetofwater/nabu/pkg"
	log "github.com/sirupsen/logrus"
)

// The path in storage of the parquet file containing all
// JSON-LD documents harvested from a sitemap
func SummonedParquetPath(sitemapId string) storage.ObjectPath {
	return fmt.Sprintf("summoned/%s.parquet", sitemapId)
}

// The footer metadata key of a summoned parquet file holding the
// crawl stats, as JSON, of the harvest that wrote the file
const CrawlStatsMetadataKey = "nabu_crawl_stats"

// parquetSink collects features from many goroutines into a single parquet file.
// The file is spooled to a local temporary file and only uploaded once it is complete;
// streaming it directly to storage would keep an upload open for the entire harvest
// which the storage server may drop if a slow document leaves it idle for too long.
// Nothing replaces the previous file in storage unless Commit succeeds
type parquetSink struct {
	mu          sync.Mutex
	writer      *parquettable.Writer
	tmpFile     *os.File
	destination storage.CrawlStorage
	finished    bool
	path        storage.ObjectPath
}

// returned by Commit if no features were added; the upload is skipped so that
// a harvest where nothing succeeded does not replace the previous file with an empty one
var errNoFeaturesHarvested = errors.New("no documents were harvested")

func newParquetSink(destination storage.CrawlStorage, path storage.ObjectPath) (*parquetSink, error) {
	tmpFile, err := os.CreateTemp("", "nabu-*.parquet")
	if err != nil {
		return nil, fmt.Errorf("failed to create a temporary file for %s: %w", path, err)
	}
	writer, err := parquettable.NewWriter(tmpFile)
	if err != nil {
		removeTempFile(tmpFile)
		return nil, err
	}
	return &parquetSink{writer: writer, tmpFile: tmpFile, destination: destination, path: path}, nil
}

// close and delete a temporary file, logging instead of returning errors
// since the file is no longer needed
func removeTempFile(file *os.File) {
	_ = file.Close()
	if err := os.Remove(file.Name()); err != nil {
		log.Errorf("failed to remove temporary file %s: %v", file.Name(), err)
	}
}

// Add a feature to the parquet file; safe for concurrent use
func (p *parquetSink) Add(feature parquettable.Feature) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return fmt.Errorf("cannot add features to %s after it was finished", p.path)
	}
	if err := p.writer.Write(feature); err != nil {
		return fmt.Errorf("failed to write to temporary file %s: %w", p.tmpFile.Name(), err)
	}
	return nil
}

// Finish the parquet file with the crawl stats in its footer and upload it;
// returns the number of rows written. If no features were added, nothing
// is uploaded and errNoFeaturesHarvested is returned
func (p *parquetSink) Commit(stats pkg.SitemapCrawlStats) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return 0, fmt.Errorf("%s was already finished", p.path)
	}
	if p.writer.Rows() == 0 {
		p.abort(errNoFeaturesHarvested)
		return 0, errNoFeaturesHarvested
	}
	statsJson, err := json.Marshal(stats)
	if err != nil {
		p.abort(err)
		return 0, fmt.Errorf("failed to serialize the crawl stats for %s: %w", p.path, err)
	}
	if err := p.writer.SetKeyValueMetadata(CrawlStatsMetadataKey, string(statsJson)); err != nil {
		p.abort(err)
		return 0, err
	}
	p.finished = true
	defer removeTempFile(p.tmpFile)
	log.Infof("Finishing the parquet file for %s with %d rows", p.path, p.writer.Rows())
	if err := p.writer.Close(); err != nil {
		return 0, fmt.Errorf("failed to finish temporary file %s: %w", p.tmpFile.Name(), err)
	}
	size, err := p.tmpFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if _, err := p.tmpFile.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	log.Infof("Uploading %d bytes to %s", size, p.path)
	if err := p.destination.StoreWithHash(p.path, p.tmpFile, int(size)); err != nil {
		return 0, fmt.Errorf("failed to upload %s: %w", p.path, err)
	}
	return p.writer.Rows(), nil
}

// Stop writing the parquet file without uploading it so the previous file in storage is kept
func (p *parquetSink) Abort(cause error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.abort(cause)
}

// Abort without taking the lock; the caller must hold it
func (p *parquetSink) abort(cause error) {
	if p.finished {
		return
	}
	p.finished = true
	if cause == nil {
		cause = errors.New("aborted")
	}
	removeTempFile(p.tmpFile)
	log.Warnf("Did not upload %s and kept the previous file: %v", p.path, cause)
}
