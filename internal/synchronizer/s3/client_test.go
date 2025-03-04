package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"nabu/internal/common"
	"nabu/internal/common/projectpath"
	"path/filepath"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// Wrapper struct to store a handle to the container for all
type S3ClientSuite struct {
	suite.Suite
	minioContainer MinioContainer
}

// Setup common dependencies before starting the test suite
func (suite *S3ClientSuite) SetupSuite() {
	config := MinioContainerConfig{
		Username:      "minioadmin",
		Password:      "minioadmin",
		DefaultBucket: "gleanerbucket",
		ContainerName: "objects_test_minio",
	}
	minioContainer, err := NewMinioContainer(config)
	require.NoError(suite.T(), err)
	suite.minioContainer = minioContainer

	// create the bucket
	err = suite.minioContainer.ClientWrapper.Client.MakeBucket(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, minio.MakeBucketOptions{})
	require.NoError(suite.T(), err)

}

func (s *S3ClientSuite) TearDownSuite() {
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	require.NoError(s.T(), err)
}

// Make sure the number of matched objects is correct
// both with and without prefixes
func (suite *S3ClientSuite) TestNumberOfMatchedObjects() {
	t := suite.T()

	// remove all objects from the bucket before testing
	// that way we know we are starting from 0 items
	for object := range suite.minioContainer.ClientWrapper.Client.ListObjects(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, minio.ListObjectsOptions{}) {
		err := suite.minioContainer.ClientWrapper.Client.RemoveObject(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, object.Key, minio.RemoveObjectOptions{})
		require.NoError(t, err)
	}

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

// make sure that we can remove objects from the minio bucket
func (suite *S3ClientSuite) TestRemove() {

	// Validate the number of matched objects
	// before inserting so we dont need to wipe the bucket
	beforeInsert, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(suite.T(), err)

	// Insert test data into MinIO
	insertTestData := func(count int) {
		objectData := []byte("test data")
		for i := 0; i < count; i++ {
			objectName := "removable-object-" + fmt.Sprint(i)
			_, err := suite.minioContainer.ClientWrapper.Client.PutObject(
				context.Background(),
				suite.minioContainer.ClientWrapper.DefaultBucket,
				objectName,
				bytes.NewReader(objectData),
				int64(len(objectData)),
				minio.PutObjectOptions{},
			)
			require.NoError(suite.T(), err)
		}
	}

	const newObjects = 10
	// Insert objects
	insertTestData(newObjects)

	// Validate the number of matched objects
	matchedObjectsAfterInsert, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), newObjects+beforeInsert, matchedObjectsAfterInsert)

	// Remove an object
	err = suite.minioContainer.ClientWrapper.Remove("removable-object-0")
	require.NoError(suite.T(), err)

	// Validate the number of matched objects
	matchedObjectsAfterInsert, err = suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), beforeInsert+newObjects-1, matchedObjectsAfterInsert)
}

// Make sure that we can retrieve object info from a given bucket
func (suite *S3ClientSuite) TestGetObjects() {

	// Validate the number of matched objects
	// before inserting so we dont need to wipe the bucket
	objsBeforeInsert, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(suite.T(), err)

	// Insert test data into MinIO
	insertTestData := func(count int) {
		for i := range count {
			objectData := []byte(fmt.Sprintf("test data %d", i))

			objectName := "get-object-" + fmt.Sprint(i)
			_, err := suite.minioContainer.ClientWrapper.Client.PutObject(
				context.Background(),
				suite.minioContainer.ClientWrapper.DefaultBucket,
				objectName,
				bytes.NewReader(objectData),
				int64(len(objectData)),
				minio.PutObjectOptions{},
			)
			require.NoError(suite.T(), err)
		}
	}

	const newObjects = 10
	// Insert objects
	insertTestData(newObjects)

	// get the objects
	objects, err := suite.minioContainer.ClientWrapper.ObjectList("")
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), newObjects+objsBeforeInsert, len(objects))

	// get the first key and use that to get the data from within that object
	firstKey := objects[0].Key
	object, err := suite.minioContainer.ClientWrapper.Client.GetObject(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, firstKey, minio.GetObjectOptions{})
	require.NoError(suite.T(), err)
	// check the data
	data, err := io.ReadAll(object)
	require.NoError(suite.T(), err)

	keyNumber := strings.Split(firstKey, "-")[2]
	matchingData := fmt.Sprintf("test data %s", keyNumber)
	require.Equal(suite.T(), matchingData, string(data))
}

// Make sure that we can copy data between the same bucket
func (suite *S3ClientSuite) TestCopyBetweenBuckets() {

	// check the number of items in the default bucket
	_, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(suite.T(), err)

	testObj := "test-object-for-copy-test"

	// Insert one item into minio as a test
	_, err = suite.minioContainer.ClientWrapper.Client.PutObject(
		context.Background(),
		suite.minioContainer.ClientWrapper.DefaultBucket,
		testObj,
		bytes.NewReader([]byte(testObj)),
		int64(len(testObj)),
		minio.PutObjectOptions{},
	)
	require.NoError(suite.T(), err)

	newBucket := "new-bucket"
	// make a new bucket
	err = suite.minioContainer.ClientWrapper.Client.MakeBucket(context.Background(), newBucket, minio.MakeBucketOptions{})
	require.NoError(suite.T(), err)

	// copy the object to the new bucket
	err = suite.minioContainer.ClientWrapper.Copy(
		suite.minioContainer.ClientWrapper.DefaultBucket,
		testObj,
		suite.minioContainer.ClientWrapper.DefaultBucket,
		testObj+"2",
	)
	require.NoError(suite.T(), err)

	// get the data in testObj2 and make sure it is the same as testObj
	object, err := suite.minioContainer.ClientWrapper.Client.GetObject(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, testObj+"2", minio.GetObjectOptions{})
	require.NoError(suite.T(), err)
	// check the data
	data, err := io.ReadAll(object)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), testObj, string(data))
}

func (suite *S3ClientSuite) TestGetObjectAsBytes() {

	const dummyData = "dummy data"
	// Insert one item into minio as a test
	_, err := suite.minioContainer.ClientWrapper.Client.PutObject(
		context.Background(),
		suite.minioContainer.ClientWrapper.DefaultBucket,
		"test-object-for-get-test",
		bytes.NewReader([]byte(dummyData)),
		int64(len(dummyData)),
		minio.PutObjectOptions{},
	)
	require.NoError(suite.T(), err)

	data, err := suite.minioContainer.ClientWrapper.GetObjectAsBytes("test-object-for-get-test")
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), dummyData, string(data))

}

func (suite *S3ClientSuite) TestUploadFile() {
	testfile := filepath.Join(projectpath.Root, "main.go")
	err := suite.minioContainer.ClientWrapper.UploadFile("testFiles/main.go", testfile)
	require.NoError(suite.T(), err)

	// get the data in testObj2 and make sure it is the same as testObj
	object, err := suite.minioContainer.ClientWrapper.Client.GetObject(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, "testFiles/main.go", minio.GetObjectOptions{})
	require.NoError(suite.T(), err)
	// check the data
	data, err := io.ReadAll(object)
	require.NoError(suite.T(), err)
	require.Contains(suite.T(), string(data), "package main")

}

func (suite *S3ClientSuite) TestGetObjectAsNamedGraph() {
	// put a jsonld file into the bucket
	t := suite.T()
	testfile := filepath.Join("testdata", "hu02.jsonld")
	err := suite.minioContainer.ClientWrapper.UploadFile("testFiles/hu02.jsonld", testfile)
	require.NoError(t, err)
	defer func() {
		err = suite.minioContainer.ClientWrapper.Remove("testFiles/hu02.jsonld")
		require.NoError(t, err)
	}()
	// get the data in testObj2 and make sure it is the same as testObj
	proc, opt, err := common.NewJsonldProcessor(false, nil)
	require.NoError(t, err)
	object, err := suite.minioContainer.ClientWrapper.GetObjectAndConvertToGraph("testFiles/hu02.jsonld", proc, opt)
	require.NoError(t, err)
	require.Contains(t, object.Triples, "<http://schema.org/DataDownload>")
	require.Contains(t, object.Triples, "http://schema.org/address")
}

// Run the entire test suite
func TestS3ClientSuite(t *testing.T) {
	suite.Run(t, new(S3ClientSuite))
}
