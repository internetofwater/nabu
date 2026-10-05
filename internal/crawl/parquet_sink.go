// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/parquettable"
	log "github.com/sirupsen/logrus"
)

// The path in storage of the parquet file containing all
// JSON-LD documents harvested from a sitemap
func SummonedParquetPath(sitemapId string) storage.ObjectPath {
	return fmt.Sprintf("summoned/%s.parquet", sitemapId)
}

// parquetSink streams features from many goroutines into a single parquet file
// that is uploaded to storage as it is written. Nothing replaces the previous file
// in storage unless Commit succeeds
type parquetSink struct {
	mu         sync.Mutex
	writer     *parquettable.Writer
	pipeWriter *io.PipeWriter
	uploadDone chan error
	finished   bool
	path       storage.ObjectPath
}

// returned to writers if the upload stopped reading from the pipe without an error
var errUploadStopped = errors.New("upload stopped before all features were written")

// returned by Commit if no features were added; the upload is aborted so that
// a harvest where nothing succeeded does not replace the previous file with an empty one
var errNoFeaturesHarvested = errors.New("no documents were harvested")

func newParquetSink(destination storage.CrawlStorage, path storage.ObjectPath) (*parquetSink, error) {
	pipeReader, pipeWriter := io.Pipe()
	uploadDone := make(chan error, 1)
	// the upload must be reading from the pipe before
	// anything, including the parquet header, is written
	go func() {
		err := destination.StoreWithoutServersideHash(path, pipeReader)
		// unblock any writer if the upload stopped reading early
		if err != nil {
			_ = pipeReader.CloseWithError(err)
		} else {
			_ = pipeReader.CloseWithError(errUploadStopped)
		}
		uploadDone <- err
	}()

	writer, err := parquettable.NewWriter(pipeWriter)
	if err != nil {
		pipeWriter.CloseWithError(err)
		<-uploadDone
		return nil, err
	}
	return &parquetSink{writer: writer, pipeWriter: pipeWriter, uploadDone: uploadDone, path: path}, nil
}

// Add a feature to the parquet file; safe for concurrent use
func (p *parquetSink) Add(feature parquettable.Feature) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return fmt.Errorf("cannot add features to %s after it was finished", p.path)
	}
	return p.writer.Write(feature)
}

// Finish the parquet file and wait for the upload to complete; returns the number of rows written.
// If no features were added, the upload is aborted and errNoFeaturesHarvested is returned
func (p *parquetSink) Commit() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return 0, fmt.Errorf("%s was already finished", p.path)
	}
	if p.writer.Rows() == 0 {
		p.abort(errNoFeaturesHarvested)
		return 0, errNoFeaturesHarvested
	}
	p.finished = true
	if err := p.writer.Close(); err != nil {
		p.pipeWriter.CloseWithError(err)
		<-p.uploadDone
		return 0, err
	}
	_ = p.pipeWriter.Close()
	if uploadErr := <-p.uploadDone; uploadErr != nil {
		return 0, fmt.Errorf("failed to upload %s: %w", p.path, uploadErr)
	}
	return p.writer.Rows(), nil
}

// Stop writing the parquet file and cancel the upload so the previous file in storage is kept
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
	p.pipeWriter.CloseWithError(cause)
	<-p.uploadDone
	log.Warnf("Did not upload %s and kept the previous file: %v", p.path, cause)
}
