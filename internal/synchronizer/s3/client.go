package s3

import (
	"context"
	"fmt"
	"io"
	"nabu/pkg/config"
	"os"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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

// Remove an object from the store
func (m *MinioClientWrapper) Remove(object string) error {
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
func (m *MinioClientWrapper) ObjectList(prefix string) ([]minio.ObjectInfo, error) {
	var mu sync.Mutex
	wg := sync.WaitGroup{}
	objectInfo := []minio.ObjectInfo{}
	semaphoreChan := make(chan struct{}, 20) // Limit to 20 concurrent goroutines so we don't overload

	objectCh := m.Client.ListObjects(context.Background(), m.DefaultBucket,
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

// Copy s3. Can be to either the same bucket or a different bucket
func (m *MinioClientWrapper) Copy(srcbucket, srcobject, dstbucket, dstobject string) error {

	// Source object
	srcOpts := minio.CopySrcOptions{
		Bucket: srcbucket,
		Object: srcobject,
	}

	// Destination object
	dstOpts := minio.CopyDestOptions{
		Bucket: dstbucket,
		Object: dstobject,
	}

	// Copy object call
	_, err := m.Client.CopyObject(context.Background(), dstOpts, srcOpts)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// Return the number of objects that match a given prefix within the
// specified bucket
func (m *MinioClientWrapper) NumberOfMatchingObjects(prefixes []string) (int, error) {
	count := 0
	for _, prefix := range prefixes {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		objectCh := m.Client.ListObjects(ctx, m.DefaultBucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})

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

func (m *MinioClientWrapper) GetObjectAsBytes(objectName string) ([]byte, error) {
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

func (m *MinioClientWrapper) UploadFile(uploadPath string, localFileName string) error {
	file, err := os.Open(localFileName)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = m.Client.PutObject(context.Background(), m.DefaultBucket, uploadPath, file, -1, minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}
