package objects

import (
	"bytes"
	"context"

	"github.com/minio/minio-go/v7"
	log "github.com/sirupsen/logrus"
)

// Wrapper to allow us to extend the minio client struct with new methods
type MinioClientWrapper struct {
	Client *minio.Client
}

// Remove is the generic object collection function
func (m *MinioClientWrapper) Remove(bucket, object string) error {
	opts := minio.RemoveObjectOptions{
		GovernanceBypass: true,
	}

	err := m.Client.RemoveObject(context.Background(), bucket, object, opts)
	if err != nil {
		log.Error(err)
		return err
	}

	return err
}

// Copy is the generic object collection function
func (m *MinioClientWrapper) Copy(srcbucket, srcobject, dstbucket, dstobject string) error {

	// Use-case 1: Simple copy object with no conditions.
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
	uploadInfo, err := m.Client.CopyObject(context.Background(), dstOpts, srcOpts)
	if err != nil {
		log.Println(err)
		return err
	}

	log.Println("Successfully copied object:", uploadInfo)

	return nil
}
func (m *MinioClientWrapper) GetObjects(bucketName string, prefixes []string) ([]string, error) {
	oa := []string{}

	for _, prefix := range prefixes {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		objectCh := m.Client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})

		for object := range objectCh {
			if object.Err != nil {
				log.Println(object.Err)
				return oa, object.Err
			}
			oa = append(oa, object.Key)
		}
		log.Printf("%s:%s object count: %d\n", bucketName, prefix, len(oa))
	}

	return oa, nil
}

// GetS3Bytes simply pulls the byes of an object from the store
func (m *MinioClientWrapper) GetS3Bytes(bucket, object string) ([]byte, string, error) {
	fo, err := m.Client.GetObject(context.Background(), bucket, object, minio.GetObjectOptions{})
	if err != nil {
		log.Info(err)
		return nil, "", err
	}
	defer fo.Close()

	oi, err := fo.Stat()
	if err != nil {
		log.Infof("Issue with reading an object:  %s%s", bucket, object)
	}

	dgraph := ""
	if len(oi.Metadata["X-Amz-Meta-Dgraph"]) > 0 {
		dgraph = oi.Metadata["X-Amz-Meta-Dgraph"][0]
	}

	// fmt.Printf("%s %s %s \n", urlval, sha1val, resuri)

	// TODO  set an upper byte size  limit here and return error if the size is too big
	// TODO  why was this done, return size too and let the calling function worry about it...????
	//sz := oi.Size        // what type is this...
	//if sz > 1073741824 { // if bigger than 1 GB (which is very small) move on
	//	return nil, "", errors.New("gets3bytes says file above processing size threshold")
	//}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(fo)
	if err != nil {
		return nil, "", err
	}

	bb := buf.Bytes() // Does a complete copy of the bytes in the buffer.

	return bb, dgraph, err
}

// BulkLoad
