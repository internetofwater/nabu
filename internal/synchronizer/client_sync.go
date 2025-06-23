// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"
	"github.com/minio/minio-go/v7"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
)

// Struct holding the differences between the triplestore and s3
// and a mapping of URN to object name so we can load the graphs
// based on this diff info
type GraphDiff struct {
	TriplestoreGraphsNotInS3 []string
	S3GraphsNotInTriplestore []string
	// A map of URN to object name
	// This includes all URNs not just the ones in the diff
	s3UrnToAssociatedObjName map[string]string
}

// Return the difference in graphs between the triplestore and s3 based on the prefix
// i.e. summoned/counties0 will check urn:iow:summoned:counties0 when comparing between the two
//
// This function runs two goroutines to fetch the triplestore and s3 data in parallel
func (synchronizer *SynchronizerClient) getGraphDiff(ctx context.Context, prefix string) (GraphDiff, error) {
	ctx, span := opentelemetry.SubSpanFromCtx(ctx)
	defer span.End()

	var (
		objectNamesInS3     []minio.ObjectInfo
		graphsInTriplestore []string
		wg                  sync.WaitGroup
	)

	// Using channels to fetch data in parallel
	objChan := make(chan []minio.ObjectInfo, 1)
	graphChan := make(chan []string, 1)
	errChan := make(chan error, 2)

	wg.Add(2)

	// Fetch object names from S3 in parallel
	go func() {
		defer wg.Done()
		objs, err := synchronizer.S3Client.ObjectList(ctx, prefix)
		if err != nil {
			errChan <- err
			return
		}
		objChan <- objs
	}()

	// Fetch named graphs from triplestore in parallel
	go func() {
		defer wg.Done()
		graphs, err := synchronizer.GraphClient.NamedGraphsAssociatedWithS3Prefix(ctx, prefix)
		if err != nil {
			errChan <- err
			return
		}
		graphChan <- graphs
	}()

	// Wait for both goroutines to finish
	wg.Wait()
	close(objChan)
	close(graphChan)
	close(errChan)

	// Collect results
	for err := range errChan {
		if err != nil {
			log.Println(err)
			return GraphDiff{}, err
		}
	}

	if objs, ok := <-objChan; ok {
		objectNamesInS3 = objs
		if len(objectNamesInS3) == 1 && objectNamesInS3[0].Size == 0 {
			log.Warnf("No objects found with prefix %s in bucket %s", prefix, synchronizer.S3Client.DefaultBucket)
		}
	}
	if graphs, ok := <-graphChan; ok {
		graphsInTriplestore = graphs
	}

	// Convert object names to the URN pattern used in the graph
	s3UrnToAssociatedObjName := make(map[string]string)
	var s3ObjGraphNames []string
	for _, objectName := range objectNamesInS3 {
		s3ObjUrn, err := common.MakeURN(objectName.Key)
		if err != nil {
			return GraphDiff{}, err
		}
		s3UrnToAssociatedObjName[s3ObjUrn] = objectName.Key
		s3ObjGraphNames = append(s3ObjGraphNames, s3ObjUrn)
	}

	triplestoreGraphsNotInS3 := findMissing(graphsInTriplestore, s3ObjGraphNames)
	s3GraphsNotInTriplestore := findMissing(s3ObjGraphNames, graphsInTriplestore)

	span.SetAttributes(attribute.Int("s3_graphs_not_in_triplestore", len(s3GraphsNotInTriplestore)))
	span.SetAttributes(attribute.Int("triplestore_graphs_not_in_s3", len(triplestoreGraphsNotInS3)))

	return GraphDiff{
		TriplestoreGraphsNotInS3: triplestoreGraphsNotInS3,
		S3GraphsNotInTriplestore: s3GraphsNotInTriplestore,
		s3UrnToAssociatedObjName: s3UrnToAssociatedObjName,
	}, nil
}

// Batch upserts objects from s3 to triplestore using upsertBatchSize as defined in the synchronizer client
func (synchronizer *SynchronizerClient) batchedUpsert(ctx context.Context, s3GraphNames []s3.S3Prefix) error {
	batchSize := synchronizer.GraphClient.GetUpsertBatchSize()

	if batchSize == 0 {
		return fmt.Errorf("got invalid upsert batch size of 0")
	}

	_, span := opentelemetry.SubSpanFromCtx(ctx)
	defer span.End()

	var errorGroup errgroup.Group
	errorGroup.SetLimit(50)

	log.Infof("Upserting %d objects from S3 to triplestore", len(s3GraphNames))
	batches := createBatches(s3GraphNames, batchSize)

	log.Debugf("Upserting with batch size %d", batchSize)

	for i, batch := range batches {
		batch := batch // capture range variable
		errorGroup.Go(func() error {
			ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("batch_insertion_%d/%d", i, len(batches)))
			defer span.End()
			var namedGraphs []common.NamedGraph
			var graphUpdateMutex sync.Mutex

			var eg errgroup.Group
			eg.SetLimit(runtime.NumCPU())
			ctx, subspan := opentelemetry.SubSpanFromCtxWithName(ctx, "get_obj_and_convert_to_graph")
			for _, graphName := range batch {
				eg.Go(func() error {
					namedGraph, err := synchronizer.S3Client.GetObjectAndConvertToGraph(graphName, synchronizer.jsonldProcessor, synchronizer.jsonldOptions)
					if err != nil {
						return err
					}

					graphUpdateMutex.Lock()
					defer graphUpdateMutex.Unlock()
					namedGraphs = append(namedGraphs, namedGraph)
					return nil
				})
			}

			if err := eg.Wait(); err != nil {
				return err
			}
			subspan.End()
			return synchronizer.GraphClient.UpsertNamedGraphs(ctx, namedGraphs)
		})
	}

	return errorGroup.Wait()
}

// Get rid of graphs with specific prefix in the triplestore that are not in the object store
// Drops are determined by mapping a prefix to the associated URN
func (synchronizer *SynchronizerClient) SyncTriplestoreGraphs(ctx context.Context, prefix s3.S3Prefix, checkAndRemoveOrphans bool) error {
	if synchronizer.GraphClient.GetUpsertBatchSize() == 0 {
		return fmt.Errorf("got invalid upsert batch size of 0")
	}

	ctx, span := opentelemetry.SubSpanFromCtx(ctx)
	defer span.End()

	var s3GraphsNotInTriplestore []string
	// if we want to check for orphaned graphs
	// we need to get the diff between the graph and s3
	// then drop the orphaned graphs
	graphDiff, err := synchronizer.getGraphDiff(ctx, prefix)
	if err != nil {
		log.Error(err)
		return err
	}
	for _, graphName := range graphDiff.S3GraphsNotInTriplestore {
		asUrn := graphDiff.s3UrnToAssociatedObjName[graphName]
		s3GraphsNotInTriplestore = append(s3GraphsNotInTriplestore, asUrn)
	}

	orphanedGraphs := graphDiff.TriplestoreGraphsNotInS3
	// Don't send a drop request if there are no orphaned graphs in the triplestore to remove
	if len(orphanedGraphs) > 0 && checkAndRemoveOrphans {
		log.Infof("Dropping %d graphs from triplestore", len(orphanedGraphs))
		// All triplestore graphs not in s3 should be removed since s3 is the source of truth
		span.SetAttributes(attribute.Int("orphaned_graphs_to_drop", len(orphanedGraphs)))
		if err := synchronizer.GraphClient.DropGraphs(ctx, orphanedGraphs); err != nil {
			log.Errorf("Drop graph issue when syncing %v\n", err)
			return err
		}
	}
	return synchronizer.batchedUpsert(ctx, s3GraphsNotInTriplestore)
}
