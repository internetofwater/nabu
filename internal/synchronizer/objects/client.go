package objects

import (
	"bytes"
	"context"
	"fmt"
	"nabu/pkg/config"

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
		log.Info("Minio created with no region set")
		minioClient, err = minio.New(endpoint,
			&minio.Options{Creds: credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
				Secure: useSSL,
			})

	} else {
		log.Warn("region set for GCS or AWS, may cause issues with minio")
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

// Copy objects. Can be to either the same bucket or a different bucket
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
		log.Println(err)
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
				log.Println(object.Err)
				return count, object.Err
			}
			count++
		}
	}
	return count, nil
}

// Return a list of object names in a bucket
func (m *MinioClientWrapper) GetObjects(prefixes []string) ([]minio.ObjectInfo, error) {
	objectArray := []minio.ObjectInfo{}

	for _, prefix := range prefixes {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		objectCh := m.Client.ListObjects(ctx, m.DefaultBucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})

		for object := range objectCh {
			if object.Err != nil {
				log.Println(object.Err)
				return objectArray, object.Err
			}
			objectArray = append(objectArray, object)
		}
	}

	return objectArray, nil
}

// Get the byes of an object from the store
func (m *MinioClientWrapper) GetObjectAsBytes(objectName string) ([]byte, error) {
	fileObject, err := m.Client.GetObject(context.Background(), m.DefaultBucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		log.Info(err)
		return nil, err
	}
	defer fileObject.Close()

	_, err = fileObject.Stat()
	if err != nil {
		log.Infof("Issue with reading an object. Seems to not exist when looking in bucket: %s and name: %s", m.DefaultBucket, objectName)
		return nil, err
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(fileObject)
	if err != nil {
		return nil, err
	}

	bufferBytes := buf.Bytes() // Does a complete copy of the bytes in the buffer.

	return bufferBytes, err
}

// BulkLoad
