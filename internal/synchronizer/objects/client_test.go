package objects

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type S3ClientSuite struct {
	suite.Suite
	minioContainer MinioContainer
}

// Setup common dependencies before starting the test suite
func (suite *S3ClientSuite) SetupSuite() {
	minioContainer, err := NewMinioContainer("minioadmin", "minioadmin", "gleanerbucket")
	require.NoError(suite.T(), err)
	suite.minioContainer = minioContainer

	// create the bucket
	err = suite.minioContainer.ClientWrapper.Client.MakeBucket(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, minio.MakeBucketOptions{})
	require.NoError(suite.T(), err)

}

// Make sure the number of matched objects is correct
// both with and without prefixes
func (suite *S3ClientSuite) TestNumberOfMatchedObjects() {
	t := suite.T()

	// Insert test data into MinIO
	insertTestData := func(prefix string, count int) {
		objectData := []byte("test data")
		for i := 0; i < count; i++ {
			objectName := prefix + "test-object-" + fmt.Sprint(i)
			info, err := suite.minioContainer.ClientWrapper.Client.PutObject(
				context.Background(),
				suite.minioContainer.ClientWrapper.DefaultBucket,
				objectName,
				bytes.NewReader(objectData),
				int64(len(objectData)),
				minio.PutObjectOptions{},
			)
			require.NoError(t, err)
			require.Equal(t, info.Key, objectName)
		}
	}

	const rootObjects = 10
	const testPrefixedObjects = 7
	const otherPrefixedObjects = 19
	const testPrefix = "test-prefix/"
	const otherPrefix = "other-prefix/"

	// Insert root objects
	insertTestData("", rootObjects)
	// Insert test-prefixed objects
	insertTestData(testPrefix, testPrefixedObjects)
	// Insert other-prefixed objects
	insertTestData(otherPrefix, otherPrefixedObjects)

	// Validate the number of matched objects
	matchedObjects, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(t, err)
	require.Equal(t, rootObjects+testPrefixedObjects+otherPrefixedObjects, matchedObjects)

	// Validate the number of matched objects with a prefix
	matchedObjects, err = suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{testPrefix})
	require.NoError(t, err)
	require.Equal(t, testPrefixedObjects, matchedObjects)

	// Validate the number of matched objects with multiple prefixes
	matchedObjects, err = suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{testPrefix, otherPrefix})
	require.NoError(t, err)
	require.Equal(t, testPrefixedObjects+otherPrefixedObjects, matchedObjects)
}

func (t *S3ClientSuite) TestRemove() {

	// reset the bucket before testing remove so we
	//  dont have any artifacts from previous runs
	err := t.minioContainer.ClientWrapper.Client.RemoveBucket(context.Background(), t.minioContainer.ClientWrapper.DefaultBucket)
	require.NoError(t.T(), err)

	err = t.minioContainer.ClientWrapper.Client.MakeBucket(context.Background(), t.minioContainer.ClientWrapper.DefaultBucket, minio.MakeBucketOptions{})
	require.NoError(t.T(), err)

	// Insert test data into MinIO
	insertTestData := func(count int) {
		objectData := []byte("test data")
		for i := 0; i < count; i++ {
			objectName := "test-object-" + fmt.Sprint(i)
			_, err := t.minioContainer.ClientWrapper.Client.PutObject(
				context.Background(),
				t.minioContainer.ClientWrapper.DefaultBucket,
				objectName,
				bytes.NewReader(objectData),
				int64(len(objectData)),
				minio.PutObjectOptions{},
			)
			require.NoError(t.T(), err)
		}
	}

	const objects = 10
	// Insert objects
	insertTestData(objects)

	// Validate the number of matched objects
	matchedObjects, err := t.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(t.T(), err)
	require.Equal(t.T(), objects, matchedObjects)

	// Remove an object
	err = t.minioContainer.ClientWrapper.Remove("test-object-0")
	require.NoError(t.T(), err)

	// Validate the number of matched objects
	matchedObjects, err = t.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(t.T(), err)
	require.Equal(t.T(), objects-1, matchedObjects)
}

// Run the entire test suite
func TestS3ClientSuite(t *testing.T) {
	suite.Run(t, new(S3ClientSuite))
}
