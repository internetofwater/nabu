// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/config"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"golang.org/x/sync/errgroup"

	"github.com/internetofwater/nabu/internal/crawl/storage"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/piprate/json-gold/ld"
	log "github.com/sirupsen/logrus"
)

var _ storage.CrawlStorage = &MinioClientWrapper{}

// Wrapper to allow us to extend the minio client struct with new methods
type MinioClientWrapper struct {
	// Base client for accessing minio
	Client *minio.Client
	// Default bucket to use for operations.
	// Specified here to avoid having to pass it as a parameter to every operation
	// since we are only using one bucket
	DefaultBucket string

	// A separate bucket for storing public metadata
	// This is used so the metadata can be made
	// public without exposing crawled data
	MetadataBucket string
}

type S3Prefix = string

// MinioConnection Set up minio and initialize client
func NewMinioClientWrapper(mcfg config.MinioConfig) (*MinioClientWrapper, error) {
	if mcfg.MetadataBucket == "" {
		return nil, errors.New("no metadata bucket specified")
	}
	if mcfg.Bucket == "" {
		return nil, errors.New("no bucket specified")
	}

	var endpoint string

	if mcfg.Port == 0 {
		endpoint = mcfg.Address
	} else {
		endpoint = fmt.Sprintf("%s:%d", mcfg.Address, mcfg.Port)
	}
	accessKeyID := mcfg.Accesskey
	secretAccessKey := mcfg.Secretkey
	useSSL := mcfg.SSL

	var minioClient *minio.Client
	var err error

	if mcfg.Region == "" {
		log.Debug("Minio client created with no region set")
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

	return &MinioClientWrapper{Client: minioClient, DefaultBucket: mcfg.Bucket, MetadataBucket: mcfg.MetadataBucket}, err
}

// Create the default bucket
func (m *MinioClientWrapper) SetupBuckets() error {
	exists, err := m.Client.BucketExists(context.Background(), m.DefaultBucket)
	if err != nil {
		return err
	}
	if !exists {
		if err := m.Client.MakeBucket(context.Background(), m.DefaultBucket, minio.MakeBucketOptions{}); err != nil {
			return err
		}
	}

	exists, err = m.Client.BucketExists(context.Background(), m.MetadataBucket)
	if err != nil {
		return err
	}
	if !exists {
		if err := m.Client.MakeBucket(context.Background(), m.MetadataBucket, minio.MakeBucketOptions{}); err != nil {
			return err
		}
	}
	return nil
}

// Remove an object from the store
func (m MinioClientWrapper) Remove(object S3Prefix) error {
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

	var eg errgroup.Group
	var mu sync.Mutex
	eg.SetLimit(40)
	objectInfo := []minio.ObjectInfo{}

	objectCh := m.Client.ListObjects(ctx, m.DefaultBucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: true})

	for object := range objectCh {
		if object.Err != nil {
			log.Error(object.Err)
			return nil, object.Err
		}
		eg.Go(func() error {
			mu.Lock()
			objectInfo = append(objectInfo, object)
			mu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return objectInfo, nil
}

// Get the hash of the file using ETag header metadata
func (m MinioClientWrapper) GetHash(objectName S3Prefix) (storage.Md5Hash, error) {
	result, err := m.Client.StatObject(context.Background(), m.DefaultBucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return "", err
	}
	if strings.Contains(result.ETag, "-") {
		return "", fmt.Errorf("object %s contains - signifying that the data came from a multipart upload and thus the hash is not of the raw content", objectName)
	}
	return result.ETag, nil
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
	defer func() { _ = fileObject.Close() }()

	stat, err := fileObject.Stat()
	if err != nil {
		log.Errorf("Issue with reading an object. Seems to not exist in bucket: %s and name: %s", m.DefaultBucket, objectName)
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
	defer func() { _ = file.Close() }()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	err = m.StoreWithHash(uploadPath, file, int(stat.Size()))
	return err
}

// StoreWithServersideHash bytes into the minio store
func (m MinioClientWrapper) StoreWithHash(path S3Prefix, data io.Reader, sizeInBytes int) error {
	_, err := m.Client.PutObject(context.Background(), m.DefaultBucket, path, data, int64(sizeInBytes), minio.PutObjectOptions{})
	return err
}

func (m MinioClientWrapper) StoreWithoutServersideHash(path S3Prefix, data io.Reader) error {
	_, err := m.Client.PutObject(context.Background(), m.DefaultBucket, path, data, -1, minio.PutObjectOptions{})
	return err
}

func (m MinioClientWrapper) StoreMetadata(path S3Prefix, data io.Reader) error {
	_, err := m.Client.PutObject(context.Background(), m.MetadataBucket, path, data, -1, minio.PutObjectOptions{})
	return err
}

func (m MinioClientWrapper) ListDir(path S3Prefix) (storage.Set, error) {
	objs, err := m.ObjectList(context.Background(), path)
	if err != nil {
		return nil, err
	}
	set := make(storage.Set)
	for _, obj := range objs {
		set.Add(obj.Key)
	}
	return set, nil
}

// Get bytes from the minio store
func (m MinioClientWrapper) Get(path S3Prefix) (io.ReadCloser, error) {
	return m.Client.GetObject(context.Background(), m.DefaultBucket, path, minio.GetObjectOptions{})
}

// Return true if the file with the specified name in the bucket has the same bytesum as the local file of the same name
func (m MinioClientWrapper) MatchesWithLocalBytesum(remotePrefix S3Prefix, localDir string, name string) (bool, error) {

	if !strings.HasSuffix(remotePrefix, "/") {
		return false, fmt.Errorf("prefix %s is arbitrary and must end with /", remotePrefix)
	}

	prefixForHash := remotePrefix + name + ".bytesum"
	log.Debugf("Checking remote file hash at %s", prefixForHash)
	remoteHashFile, err := m.Client.GetObject(context.Background(), m.DefaultBucket, prefixForHash, minio.StatObjectOptions{})
	if err != nil {
		return false, nil
	}
	remoteHash, err := io.ReadAll(remoteHashFile)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			log.Debugf("Remote file bytesum %s does not exist", prefixForHash)
			return false, nil
		}
		return false, err
	}

	localHashFile := localDir + "/" + name + ".bytesum"
	log.Debugf("Checking local file bytesum at %s", localHashFile)
	localHashValue, err := os.ReadFile(localHashFile)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return string(remoteHash) == string(localHashValue), nil

}

// pull all bytesums from the bucket to disk
func (m MinioClientWrapper) pullAllByteSums(ctx context.Context, prefix S3Prefix, outputDir string) error {
	byteSumChan := m.Client.ListObjects(ctx, m.DefaultBucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})

	for byteSum := range byteSumChan {
		if byteSum.Err != nil {
			return byteSum.Err
		}

		if !strings.HasSuffix(byteSum.Key, ".bytesum") {
			continue
		}

		fileName := path.Base(byteSum.Key)

		file, err := os.OpenFile(filepath.Join(outputDir, fileName), os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer func() {
			if err := file.Close(); err != nil {
				log.Error(err)
			}
		}()

		ob, err := m.Client.GetObject(ctx, m.DefaultBucket, byteSum.Key, minio.GetObjectOptions{})
		if err != nil {
			return err
		}
		defer func() {
			if err := ob.Close(); err != nil {
				log.Error(err)
			}
		}()

		_, err = io.Copy(file, ob)
		if err != nil {
			return err
		}
	}
	return nil
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

// Return true if the bucket is empty
// This is essentially a more optimized version of listing the directory
func (m MinioClientWrapper) IsEmptyDir(path S3Prefix) (bool, error) {
	objs := m.Client.ListObjects(context.Background(), m.DefaultBucket, minio.ListObjectsOptions{Prefix: path, Recursive: true})
	for obj := range objs {
		if obj.Err != nil {
			return false, obj.Err
		}
		if obj.Key != path {
			return false, nil
		}
	}
	return true, nil
}

// PullSeparateFilesToDir downloads all the objects with the given prefix
// and stores them in the specified directory without combining them
func (m MinioClientWrapper) PullSeparateFilesToDir(ctx context.Context, prefix S3Prefix, outputDir string, nameFilter string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}

	log.Debugf("Downloading all objects in parallel with prefix %s to %s", prefix, outputDir)

	objChan := m.Client.ListObjects(ctx, m.DefaultBucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	var eg errgroup.Group
	eg.SetLimit(1)

	cumulativeDownloadedFiles := atomic.Int32{}
	var mu sync.Mutex
	cumulativeDownloadedMegabytes := float64(0)

	for obj := range objChan {

		if nameFilter != "" && !strings.Contains(obj.Key, nameFilter) {
			log.Debugf("Skipping %s because it does not contain %s", obj.Key, nameFilter)
			continue
		}

		if obj.Err != nil {
			return fmt.Errorf("error when pulling files, %s", obj.Err)
		}

		if strings.HasSuffix(obj.Key, "prov.nq") || strings.HasSuffix(obj.Key, ".sha256") || strings.HasSuffix(obj.Key, ".bytesum") {
			// skip adding metadata like prov graphs or sha hashes into the concatenated file
			continue
		}
		eg.Go(func() error {
			megabytes := float64(obj.Size) / (1024 * 1024)
			log.Debugf("Downloading %s of size %0.5fMB", obj.Key, megabytes)

			// get the last item in the s3 object prefix
			// this is since the s3 prefix may be nested and we don't
			// want to have to make nested dirs to store the files
			fileName := path.Base(obj.Key)

			isPresent, err := m.MatchesWithLocalBytesum(prefix, outputDir, fileName)
			if err != nil {
				log.Errorf("Error checking if file %s exists locally: %v", fileName, err)
				return err
			}
			if isPresent {
				log.Warnf("File %s already exists locally, skipping download", fileName)
				return nil
			}

			ob, err := m.Client.GetObject(ctx, m.DefaultBucket, obj.Key, minio.GetObjectOptions{})
			if err != nil {
				return err
			}
			defer func() {
				if err := ob.Close(); err != nil {
					log.Error(err)
				}
			}()

			fullLocalPath := path.Join(outputDir, fileName)

			file, err := os.OpenFile(fullLocalPath, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return err
			}
			defer func() {
				if err := file.Close(); err != nil {
					log.Error(err)
				}
			}()

			_, err = io.Copy(file, ob)
			if err != nil {
				return err
			}

			// print the pulled files to stdout; this way external programs like oras can consume the output
			// and know which files were downloaded
			fmt.Println(fullLocalPath)

			cumulativeDownloadedFiles.Add(1)
			mu.Lock()
			cumulativeDownloadedMegabytes += megabytes
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	// pull all bytesums after all files have been downloaded; otherwise if we were
	// to do it in parallel with the file download it would have a race condition
	if err := m.pullAllByteSums(ctx, prefix, outputDir); err != nil {
		return err
	}

	log.Infof("Downloaded %d files to %s with total size: %0.5fMB", cumulativeDownloadedFiles.Load(), outputDir, cumulativeDownloadedMegabytes)

	return nil
}

func (m MinioClientWrapper) PullAndConcat(ctx context.Context, prefix S3Prefix, outputFile string) error {
	const READ_WRITE_OWNER_READ_OTHERS = 0664
	file, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, READ_WRITE_OWNER_READ_OTHERS)
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Error(err)
		}
	}()

	log.Debugf("Concatenating all objects with prefix %s to %s", prefix, outputFile)

	bufferedChannel := make(chan chunk, 100)

	writerProcess := errgroup.Group{}
	writerProcess.SetLimit(1)

	// consume the buffer and write to disk
	writerProcess.Go(func() error {
		bufferedWriter := bufio.NewWriterSize(file, FourMB)
		for chunk := range bufferedChannel {
			if chunk.err != nil {
				return chunk.err
			}
			if _, err := bufferedWriter.Write(chunk.data); err != nil {
				return err
			}
		}
		flushErr := bufferedWriter.Flush()
		log.Debug("Concat to disk complete")
		return flushErr
	})

	objChan := m.Client.ListObjects(ctx, m.DefaultBucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	var downloadProcess errgroup.Group
	ioBoundGoroutineCount := runtime.NumCPU() * 2
	downloadProcess.SetLimit(ioBoundGoroutineCount)

	cumulativeObjSize := atomic.Int64{}

	for obj := range objChan {
		if obj.Err != nil {
			close(bufferedChannel)
			return obj.Err
		}

		if strings.HasSuffix(obj.Key, "prov.nq") {
			// skip adding prov graphs into the concatenated file
			continue
		}

		downloadProcess.Go(func() error {
			size, err := getObjAndWriteToChannel(ctx, &m, &obj, bufferedChannel)
			if err != nil {
				return err
			}
			cumulativeObjSize.Add(size)
			return nil
		})
	}

	// Wait until all downloads are complete
	if downloadErr := downloadProcess.Wait(); downloadErr != nil {
		return downloadErr
	}
	// Close the channel for downloads once all are done
	close(bufferedChannel)
	// Wait for all writes to finish
	if writerErr := writerProcess.Wait(); writerErr != nil {
		return writerErr
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() != cumulativeObjSize.Load() {
		return fmt.Errorf("file size mismatch: created a file of size %d, but downloaded %d", cumulativeObjSize.Load(), stat.Size())
	}

	return nil
}

// Pull will either pull files to a single file or a directory
// 1. Concurrently read from S3
// 2. Pass the data to a channel
// 3. write to the file using buffered writer
func (m MinioClientWrapper) Pull(ctx context.Context, prefix S3Prefix, outputFileOrDir string, nameFilter string) error {
	if prefix == "" {
		return errors.New("prefix cannot be empty when concatenating; you should not implicitly download the entire bucket")
	}
	if outputFileOrDir == "" {
		return errors.New("local file name cannot be empty")
	}

	isDir := strings.HasSuffix(outputFileOrDir, "/")

	if isDir {
		log.Debugf("%s was specified as the local download directory due to the ending /", outputFileOrDir)
		return m.PullSeparateFilesToDir(ctx, prefix, outputFileOrDir, nameFilter)
	} else {
		log.Debugf("%s was specified as the local file destination", outputFileOrDir)
		if nameFilter != "" {
			return fmt.Errorf("substr is not implemented for pull and concat")
		}
		return m.PullAndConcat(ctx, prefix, outputFileOrDir)
	}
}
