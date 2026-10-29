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

// The path in storage of the structured GeoParquet file with
// the tabular fields of the documents harvested from a sitemap
func SummonedStructuredParquetPath(sitemapId string) storage.ObjectPath {
	return fmt.Sprintf("summoned/structured/%s.parquet", sitemapId)
}

// The path in storage of the parquet file with the JSON-LD
// documents, including enrichments, harvested from a sitemap
func SummonedRDFParquetPath(sitemapId string) storage.ObjectPath {
	return fmt.Sprintf("summoned/rdf/%s.parquet", sitemapId)
}

// The footer metadata key of a summoned parquet file holding the
// crawl stats, as JSON, of the harvest that wrote the file
const CrawlStatsMetadataKey = "nabu_crawl_stats"

// the parts of the parquet writers that the sink needs
type parquetFileWriter interface {
	Write(parquettable.Feature) error
	SetKeyValueMetadata(key, value string) error
	Rows() int
	Close() error
}

// A parquet file being written to a local temporary file
type spooledParquetFile struct {
	writer  parquetFileWriter
	tmpFile *os.File
	path    storage.ObjectPath
}

// parquetSink collects features from many goroutines into the structured and RDF parquet files
// of a sitemap. The files are spooled to local temporary files and only uploaded once they are
// complete; streaming them directly to storage would keep an upload open for the entire harvest
// which the storage server may drop if a slow document leaves it idle for too long.
// Nothing replaces the previous files in storage unless Commit succeeds
type parquetSink struct {
	mu          sync.Mutex
	files       []spooledParquetFile
	destination storage.CrawlStorage
	finished    bool
	// describes the files in messages
	name string
}

// returned by Commit if no features were added; the upload is skipped so that
// a harvest where nothing succeeded does not replace the previous file with an empty one
var errNoFeaturesHarvested = errors.New("no documents were harvested")

// Create a sink that writes the parquet files of a sitemap
func newSitemapParquetSink(destination storage.CrawlStorage, sitemapId string) (*parquetSink, error) {
	return newParquetSink(destination, SummonedStructuredParquetPath(sitemapId), SummonedRDFParquetPath(sitemapId))
}

func newParquetSink(destination storage.CrawlStorage, structuredPath, rdfPath storage.ObjectPath) (*parquetSink, error) {
	sink := &parquetSink{destination: destination, name: fmt.Sprintf("%s and %s", structuredPath, rdfPath)}
	newWriters := []struct {
		path      storage.ObjectPath
		newWriter func(io.Writer) (parquetFileWriter, error)
	}{
		{structuredPath, func(w io.Writer) (parquetFileWriter, error) { return parquettable.NewStructuredWriter(w) }},
		{rdfPath, func(w io.Writer) (parquetFileWriter, error) { return parquettable.NewRDFWriter(w) }},
	}
	for _, newWriter := range newWriters {
		tmpFile, err := os.CreateTemp("", "nabu-*.parquet")
		if err != nil {
			sink.removeTempFiles()
			return nil, fmt.Errorf("failed to create a temporary file for %s: %w", newWriter.path, err)
		}
		writer, err := newWriter.newWriter(tmpFile)
		if err != nil {
			removeTempFile(tmpFile)
			sink.removeTempFiles()
			return nil, err
		}
		sink.files = append(sink.files, spooledParquetFile{writer: writer, tmpFile: tmpFile, path: newWriter.path})
	}
	return sink, nil
}

// close and delete a temporary file, logging instead of returning errors
// since the file is no longer needed
func removeTempFile(file *os.File) {
	_ = file.Close()
	if err := os.Remove(file.Name()); err != nil {
		log.Errorf("failed to remove temporary file %s: %v", file.Name(), err)
	}
}

func (p *parquetSink) removeTempFiles() {
	for _, file := range p.files {
		removeTempFile(file.tmpFile)
	}
}

// Add a feature to the parquet files; safe for concurrent use
func (p *parquetSink) Add(feature parquettable.Feature) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return fmt.Errorf("cannot add features to %s after they were finished", p.name)
	}
	for _, file := range p.files {
		if err := file.writer.Write(feature); err != nil {
			return fmt.Errorf("failed to write to temporary file %s for %s: %w", file.tmpFile.Name(), file.path, err)
		}
	}
	return nil
}

// Finish the parquet files with the crawl stats in their footers and upload them;
// returns the number of rows written. If no features were added, nothing
// is uploaded and errNoFeaturesHarvested is returned
func (p *parquetSink) Commit(stats pkg.SitemapCrawlStats) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return 0, fmt.Errorf("%s were already finished", p.name)
	}
	// every feature is added to every file so they all have the same number of rows
	rows := p.files[0].writer.Rows()
	if rows == 0 {
		p.abort(errNoFeaturesHarvested)
		return 0, errNoFeaturesHarvested
	}
	statsJson, err := json.Marshal(stats)
	if err != nil {
		p.abort(err)
		return 0, fmt.Errorf("failed to serialize the crawl stats for %s: %w", p.name, err)
	}
	for _, file := range p.files {
		if err := file.writer.SetKeyValueMetadata(CrawlStatsMetadataKey, string(statsJson)); err != nil {
			p.abort(err)
			return 0, err
		}
	}
	p.finished = true
	defer p.removeTempFiles()
	// every file is finished before any is uploaded so that a failure
	// to finish one does not leave only some of the files replaced
	for _, file := range p.files {
		log.Infof("Finishing the parquet file for %s with %d rows", file.path, rows)
		if err := file.writer.Close(); err != nil {
			return 0, fmt.Errorf("failed to finish temporary file %s for %s: %w", file.tmpFile.Name(), file.path, err)
		}
	}
	for _, file := range p.files {
		size, err := file.tmpFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}
		if _, err := file.tmpFile.Seek(0, io.SeekStart); err != nil {
			return 0, err
		}
		log.Infof("Uploading %d bytes to %s", size, file.path)
		if err := p.destination.StoreWithHash(file.path, file.tmpFile, int(size)); err != nil {
			return 0, fmt.Errorf("failed to upload %s: %w", file.path, err)
		}
	}
	return rows, nil
}

// Stop writing the parquet files without uploading them so the previous files in storage are kept
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
	p.removeTempFiles()
	log.Warnf("Did not upload %s and kept the previous files: %v", p.name, cause)
}
