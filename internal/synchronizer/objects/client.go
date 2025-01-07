package objects

import (
	"bytes"
	"context"

	"github.com/minio/minio-go/v7"
	log "github.com/sirupsen/logrus"
)

// Wrapper to allow us to extend the minio client struct with new methods
type MinioClientWrapper struct {
	// Base client for accessing minio
	Client *minio.Client
	// Default bucket to use for operations. Used to avoid having to pass it as a parameter
	DefaultBucket string
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
func (m *MinioClientWrapper) GetObjectAsBytes(object string) ([]byte, error) {
	fileObject, err := m.Client.GetObject(context.Background(), m.DefaultBucket, object, minio.GetObjectOptions{})
	if err != nil {
		log.Info(err)
		return nil, err
	}
	defer fileObject.Close()

	_, err = fileObject.Stat()
	if err != nil {
		log.Infof("Issue with reading an object:  %s%s", m.DefaultBucket, object)
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
