// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/config"
	"github.com/internetofwater/nabu/internal/opentelemetry"

	interfaces "github.com/internetofwater/nabu/internal/crawl/storage"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/piprate/json-gold/ld"
	log "github.com/sirupsen/logrus"
)

// Wrapper to allow us to extend the minio client struct with new methods
type MinioClientWrapper struct {
	// Base client for accessing minio
	Client *minio.Client
	// Default bucket to use for operations.
	// Specified here to avoid having to pass it as a parameter to every operation
	// since we are only using one bucket
	DefaultBucket string
}

type S3Prefix = string

// MinioConnection Set up minio and initialize client
func NewMinioClientWrapper(mcfg config.MinioConfig) (*MinioClientWrapper, error) {

	var endpoint string

	if mcfg.Port == 0 {
		endpoint = mcfg.Address
	} else {
		endpoint = fmt.Sprintf("%s:%d", mcfg.Address, mcfg.Port)
	}
	accessKeyID := mcfg.Accesskey
	secretAccessKey := mcfg.Secretkey
	useSSL := mcfg.Ssl

	var minioClient *minio.Client
	var err error

	if mcfg.Region == "" {
		log.Info("Minio client created with no region set")
		minioClient, err = minio.New(endpoint,
			&minio.Options{Creds: credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
				Secure: useSSL,
			})

	} else {
		region := mcfg.Region
		minioClient, err = minio.New(endpoint,
			&minio.Options{Creds: credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
				Secure: useSSL,
				Region: region,
			})
	}

	return &MinioClientWrapper{Client: minioClient, DefaultBucket: mcfg.Bucket}, err
}

// Create the default bucket
func (m *MinioClientWrapper) MakeDefaultBucket() error {
	exists, err := m.Client.BucketExists(context.Background(), m.DefaultBucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return m.Client.MakeBucket(context.Background(), m.DefaultBucket, minio.MakeBucketOptions{})
}

// Remove an object from the store
func (m *MinioClientWrapper) Remove(object S3Prefix) error {
	opts := minio.RemoveObjectOptions{
		GovernanceBypass: true,
	}

	err := m.Client.RemoveObject(context.Background(), m.DefaultBucket, object, opts)
	if err != nil {
		log.Error(err)
		return err
	}

	return err
}

// Return a list of objects matching the specified prefix
// This uses goroutines and thus does not guarantee order
func (m *MinioClientWrapper) ObjectList(ctx context.Context, prefix S3Prefix) ([]minio.ObjectInfo, error) {

	ctx, span := opentelemetry.SubSpanFromCtx(ctx)
	defer span.End()

	var mu sync.Mutex
	wg := sync.WaitGroup{}
	objectInfo := []minio.ObjectInfo{}
	semaphoreChan := make(chan struct{}, 40) // Limit to concurrent goroutines so we don't overload

	objectCh := m.Client.ListObjects(ctx, m.DefaultBucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: true})

	for object := range objectCh {
		// Acquire a spot in the semaphore before starting a goroutine
		semaphoreChan <- struct{}{}
		wg.Add(1)
		go func(object minio.ObjectInfo) {
			defer func() {
				<-semaphoreChan // Release the spot in the semaphore when the goroutine is done
				wg.Done()
			}()
			mu.Lock()
			objectInfo = append(objectInfo, object)
			mu.Unlock()
		}(object)
	}

	wg.Wait()
	return objectInfo, nil
}

// Return the number of objects that match a given prefix within the
// specified bucket
func (m *MinioClientWrapper) NumberOfMatchingObjects(prefixes []S3Prefix) (int, error) {
	count := 0
	for _, prefix := range prefixes {
		objectCh := m.Client.ListObjects(context.Background(), m.DefaultBucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})

		for object := range objectCh {
			if object.Err != nil {
				log.Error(object.Err)
				return count, object.Err
			}
			count++
		}
	}
	return count, nil
}

func (m *MinioClientWrapper) GetObjectAsBytes(objectName S3Prefix) ([]byte, error) {
	fileObject, err := m.Client.GetObject(context.Background(), m.DefaultBucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		log.Info(err)
		return nil, err
	}
	defer fileObject.Close()

	stat, err := fileObject.Stat()
	if err != nil {
		log.Infof("Issue with reading an object. Seems to not exist in bucket: %s and name: %s", m.DefaultBucket, objectName)
		return nil, err
	}

	buf := make([]byte, stat.Size) // Preallocate buffer
	_, err = io.ReadFull(fileObject, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

/*
GetObjectAndConvertToGraph returns a NamedGraph from the object in the bucket
the graphname will be the urn representation of the object name

1. nq files are converted are converted to triples and the graph name is set to the urn of the object name
2. jsonld files are converted to nq with the graph name set to the urn of the object name
*/
func (m *MinioClientWrapper) GetObjectAndConvertToGraph(objectName S3Prefix, jsonldProcessor *ld.JsonLdProcessor, jsonldOptions *ld.JsonLdOptions) (common.NamedGraph, error) {
	objBytes, err := m.GetObjectAsBytes(objectName)
	if err != nil {
		return common.NamedGraph{}, err
	}

	if len(objBytes) == 0 {
		log.Warnf("Object %s is empty", objectName)
	}

	graphResourceIdentifier, err := common.MakeURN(objectName)
	if err != nil {
		return common.NamedGraph{}, err
	}

	if strings.HasSuffix(objectName, ".jsonld") {
		nTriples, err := common.JsonldToNQ(string(objBytes), jsonldProcessor, jsonldOptions)
		if err != nil {
			log.Errorf("JSONLD to NQ conversion error: %s", err)
			return common.NamedGraph{}, err
		}
		if nTriples == "" {
			return common.NamedGraph{}, fmt.Errorf("JSONLD to NQ conversion returned empty string for object %s with data %s", objectName, string(objBytes))
		}

		return common.NamedGraph{GraphURI: graphResourceIdentifier, Triples: nTriples}, nil
	} else if strings.HasSuffix(objectName, ".nq") {
		graph, err := common.QuadsToTripleWithCtx(string(objBytes))
		if err != nil {
			return common.NamedGraph{}, fmt.Errorf("nq to NTCtx error: %s when converting object %s with data %s", err, objectName, string(objBytes))
		}
		return common.NamedGraph{GraphURI: graphResourceIdentifier, Triples: graph.Triples}, nil
	} else {
		return common.NamedGraph{}, fmt.Errorf("object %s is not a jsonld or nq file and thus cannot be converted to a named graph", objectName)
	}
}

// Upload a local file to the bucket at the specified remote path
func (m *MinioClientWrapper) UploadFile(uploadPath S3Prefix, localFileName string) error {
	file, err := os.Open(localFileName)
	if err != nil {
		return err
	}
	defer file.Close()

	err = m.Store(uploadPath, file)
	return err
}

// Store bytes into the minio store
func (m MinioClientWrapper) Store(path S3Prefix, data io.Reader) error {
	_, err := m.Client.PutObject(context.Background(), m.DefaultBucket, path, data, -1, minio.PutObjectOptions{})
	return err
}

// Get bytes from the minio store
func (m MinioClientWrapper) Get(path S3Prefix) (io.ReadCloser, error) {
	return m.Client.GetObject(context.Background(), m.DefaultBucket, path, minio.GetObjectOptions{})
}

func (m MinioClientWrapper) Exists(path S3Prefix) (bool, error) {
	_, err := m.Client.StatObject(context.Background(), m.DefaultBucket, path, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	// This is a string from the s3 spec, not an arbitrary magic val
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return false, nil
	}
	return false, err
}

func (m MinioClientWrapper) BatchStore(batch chan interfaces.BatchFileObject) error {
	snowBallChan := make(chan minio.SnowballObject)

	go func() {
		for obj := range batch {
			snowBallChan <- minio.SnowballObject{
				Key:     obj.Path,
				Content: obj.Reader,
			}
		}
		close(snowBallChan)
	}()
	return m.Client.PutObjectsSnowball(context.Background(), m.DefaultBucket, minio.SnowballOptions{}, snowBallChan)
}

var _ interfaces.BatchCrawlStorage = MinioClientWrapper{}
