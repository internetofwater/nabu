// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/internal/parquettable"
	"github.com/minio/minio-go/v7"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// A parquet file of harvested JSON-LD documents to convert to N-Quads
type NquadsSource struct {
	// the id of the sitemap the documents were harvested from
	SitemapID string
	// opens the parquet file for reading
	open func(ctx context.Context) (parquet.ReaderAtSeeker, io.Closer, error)
	// a human readable description of where the parquet file is
	location string
}

// the parquet files written by a harvest are named after their sitemap
func sitemapIdFromParquetPath(parquetPath string) string {
	return strings.TrimSuffix(path.Base(parquetPath), ".parquet")
}

// Create a source from a parquet file on the local filesystem
func NewLocalNquadsSource(filePath string) NquadsSource {
	return NquadsSource{
		SitemapID: sitemapIdFromParquetPath(filePath),
		location:  filePath,
		open: func(context.Context) (parquet.ReaderAtSeeker, io.Closer, error) {
			file, err := os.Open(filePath)
			return file, file, err
		},
	}
}

// Create a source for every parquet file under the prefix in the s3 bucket
func (synchronizer *SynchronizerClient) NewS3NquadsSources(ctx context.Context, prefix string) ([]NquadsSource, error) {
	objects, err := synchronizer.S3Client.ObjectList(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects with prefix %s: %w", prefix, err)
	}
	sources := []NquadsSource{}
	for _, object := range objects {
		if !strings.HasSuffix(object.Key, ".parquet") {
			continue
		}
		key := object.Key
		sources = append(sources, NquadsSource{
			SitemapID: sitemapIdFromParquetPath(key),
			location:  fmt.Sprintf("s3://%s/%s", synchronizer.syncBucketName, key),
			open: func(ctx context.Context) (parquet.ReaderAtSeeker, io.Closer, error) {
				object, err := synchronizer.S3Client.Client.GetObject(ctx, synchronizer.syncBucketName, key, minio.GetObjectOptions{})
				return object, object, err
			},
		})
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("no parquet files found with prefix %s", prefix)
	}
	return sources, nil
}

// a document along with the source it came from
type nquadsJob struct {
	feature parquettable.Feature
	source  *NquadsSource
}

// WriteNquads converts every JSON-LD document in the sources to N-Quads and streams them to w.
// Each document is put in its own named graph and blank nodes are skolemized so that the
// output of many documents can be loaded into a graph database together.
func (synchronizer *SynchronizerClient) WriteNquads(ctx context.Context, w io.Writer, sources []NquadsSource) error {
	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, "write_nquads")
	defer span.End()

	group, ctx := errgroup.WithContext(ctx)

	jobs := make(chan nquadsJob, 1000)
	nquads := make(chan string, 1000)

	group.Go(func() error {
		defer close(jobs)
		for i := range sources {
			source := &sources[i]
			if err := synchronizer.readSource(ctx, source, jobs); err != nil {
				return err
			}
		}
		return nil
	})

	documentsConverted := atomic.Int64{}

	// converting jsonld to rdf is cpu bound so there is no benefit to more workers than cores
	workers := runtime.GOMAXPROCS(0)
	converters, convertersCtx := errgroup.WithContext(ctx)
	for range workers {
		converters.Go(func() error {
			for job := range jobs {
				nquad, err := synchronizer.featureToNquads(job.feature, job.source)
				if err != nil {
					return err
				}
				if nquad == "" {
					continue
				}
				if converted := documentsConverted.Add(1); converted%10000 == 0 {
					log.Infof("Converted %d JSON-LD documents to N-Quads", converted)
				}
				select {
				case nquads <- nquad:
				case <-convertersCtx.Done():
					return convertersCtx.Err()
				}
			}
			return nil
		})
	}
	group.Go(func() error {
		defer close(nquads)
		return converters.Wait()
	})

	group.Go(func() error {
		buffered := bufio.NewWriterSize(w, 1024*1024)
		for nquad := range nquads {
			if _, err := buffered.WriteString(nquad); err != nil {
				return err
			}
		}
		return buffered.Flush()
	})

	if err := group.Wait(); err != nil {
		return err
	}
	log.Infof("Converted %d JSON-LD documents from %d parquet files to N-Quads", documentsConverted.Load(), len(sources))
	return nil
}

// stream every document in the source to the jobs channel
func (synchronizer *SynchronizerClient) readSource(ctx context.Context, source *NquadsSource, jobs chan<- nquadsJob) error {
	log.Infof("Converting JSON-LD documents in %s to N-Quads", source.location)
	reader, closer, err := source.open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", source.location, err)
	}
	defer func() { _ = closer.Close() }()

	err = parquettable.Read(ctx, reader, func(feature parquettable.Feature) error {
		select {
		case jobs <- nquadsJob{feature: feature, source: source}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", source.location, err)
	}
	return nil
}

// The name of the graph for a document; this is the same name
// that the graph would have had in previous nq releases
func graphUrnForFeature(sitemapId string, feature parquettable.Feature) (string, error) {
	identifier := feature.ID
	if identifier == "" {
		identifier = feature.URL
	}
	if identifier == "" {
		sum := md5.Sum(feature.JSONLD)
		identifier = hex.EncodeToString(sum[:])
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(identifier))
	return common.MakeURN(fmt.Sprintf("summoned/%s/%s.jsonld", sitemapId, encoded))
}

// Convert a single document to N-Quads; returns an empty string if the document should be skipped
func (synchronizer *SynchronizerClient) featureToNquads(feature parquettable.Feature, source *NquadsSource) (string, error) {
	jsonld := feature.JSONLD
	triples, err := common.JsonldToTriples(string(jsonld), synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
	if err != nil {
		var syntaxErr *json.SyntaxError
		if errors.As(err, &syntaxErr) {
			log.Errorf("JSON syntax error when parsing; this is a sign that JSON-LD document %s from %s was invalid or corrupted at byte offset %d: %v", feature.ID, source.location, syntaxErr.Offset, syntaxErr)
			return "", nil
		}
		return "", fmt.Errorf("error when transforming JSON-LD document %s from %s to RDF: %w", feature.ID, source.location, err)
	}
	if len(triples) == 0 {
		return "", fmt.Errorf("jsonld to nq conversion returned empty string for %s from %s with data %s", feature.ID, source.location, string(jsonld))
	}

	// documents are written together so blank nodes must be made globally unique
	skolemizedTriples, err := common.Skolemization(triples)
	if err != nil {
		return "", fmt.Errorf("skolemization error for %s: %w", feature.ID, err)
	}

	graphURN, err := graphUrnForFeature(source.SitemapID, feature)
	if err != nil {
		return "", err
	}

	nquad, err := common.NtToNq(skolemizedTriples, graphURN)
	if err != nil {
		return "", fmt.Errorf("error converting %s with urn '%s' to nq: %w", feature.ID, graphURN, err)
	}
	return nquad, nil
}
