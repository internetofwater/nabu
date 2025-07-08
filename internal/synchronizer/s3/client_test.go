// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/common/projectpath"

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
	minioContainer, err := NewMinioContainerFromConfig(config)
	suite.Require().NoError(err)
	suite.minioContainer = minioContainer

	// create the bucket
	err = suite.minioContainer.ClientWrapper.MakeDefaultBucket()
	suite.Require().NoError(err)

}

func (s *S3ClientSuite) TearDownSuite() {
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	s.Require().NoError(err)
}

// Make sure the number of matched objects is correct
// both with and without prefixes
func (suite *S3ClientSuite) TestNumberOfMatchedObjects() {
	t := suite.T()

	const rootObjectsToAdd = 10

	const psuedoRoot = "test_num_matching_root/"
	const testPrefix = psuedoRoot + "test-prefix/"
	const otherPrefix = psuedoRoot + "other-prefix/"

	const testPrefixedObjectsToAdd = 7
	const otherPrefixedObjectsToAdd = 19

	insertTestData := func(prefix string, count int) {
		objectData := []byte("test data")
		for i := range count {
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

	// Insert root objects
	insertTestData(psuedoRoot, rootObjectsToAdd)
	// Insert test-prefixed objects
	insertTestData(testPrefix, testPrefixedObjectsToAdd)
	// Insert other-prefixed objects
	insertTestData(otherPrefix, otherPrefixedObjectsToAdd)

	// Validate the number of matched objects
	matchedObjects, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{psuedoRoot})
	require.NoError(t, err)
	require.Equal(t, rootObjectsToAdd+testPrefixedObjectsToAdd+otherPrefixedObjectsToAdd, matchedObjects)

	// Validate the number of matched objects with a prefix
	matchedObjects, err = suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{testPrefix})
	require.NoError(t, err)
	require.Equal(t, testPrefixedObjectsToAdd, matchedObjects)

	// Validate the number of matched objects with multiple prefixes
	matchedObjects, err = suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{testPrefix, otherPrefix})
	require.NoError(t, err)
	require.Equal(t, testPrefixedObjectsToAdd+otherPrefixedObjectsToAdd, matchedObjects)

	// make sure that we can get the number of root objects
	rootObjs, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(t, err)
	require.Greater(t, rootObjs, 0)
}

// make sure that we can remove objects from the minio bucket
func (suite *S3ClientSuite) TestRemove() {

	// Validate the number of matched objects
	// before inserting so we dont need to wipe the bucket
	beforeInsert, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{""})
	suite.Require().NoError(err)

	// Insert test data into MinIO
	insertTestData := func(count int) {
		objectData := []byte("test data")
		for i := range count {
			objectName := "removable-object-" + fmt.Sprint(i)
			_, err := suite.minioContainer.ClientWrapper.Client.PutObject(
				context.Background(),
				suite.minioContainer.ClientWrapper.DefaultBucket,
				objectName,
				bytes.NewReader(objectData),
				int64(len(objectData)),
				minio.PutObjectOptions{},
			)
			suite.Require().NoError(err)
		}
	}

	const newObjects = 10
	// Insert objects
	insertTestData(newObjects)

	// Validate the number of matched objects
	matchedObjectsAfterInsert, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{""})
	suite.Require().NoError(err)
	suite.Require().Equal(newObjects+beforeInsert, matchedObjectsAfterInsert)

	// Remove an object
	err = suite.minioContainer.ClientWrapper.Remove("removable-object-0")
	suite.Require().NoError(err)

	// Validate the number of matched objects
	matchedObjectsAfterInsert, err = suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{""})
	suite.Require().NoError(err)
	suite.Require().Equal(beforeInsert+newObjects-1, matchedObjectsAfterInsert)
}

// Make sure that we can retrieve object info from a given bucket
func (suite *S3ClientSuite) TestGetObjects() {

	const testPrefix = "get_obj_test/"

	// Validate the number of matched objects
	// before inserting so we dont need to wipe the bucket
	objsBeforeInsert, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{testPrefix})
	suite.Require().NoError(err)

	// Insert test data into MinIO
	insertTestData := func(count int) {
		for i := range count {
			objectData := []byte(fmt.Sprintf("test data %d", i))

			objectName := testPrefix + "get-object-" + fmt.Sprint(i)
			_, err := suite.minioContainer.ClientWrapper.Client.PutObject(
				context.Background(),
				suite.minioContainer.ClientWrapper.DefaultBucket,
				objectName,
				bytes.NewReader(objectData),
				int64(len(objectData)),
				minio.PutObjectOptions{},
			)
			suite.Require().NoError(err)
		}
	}

	const newObjects = 10
	// Insert objects
	insertTestData(newObjects)

	// get the objects
	objects, err := suite.minioContainer.ClientWrapper.ObjectList(context.Background(), testPrefix)
	suite.Require().NoError(err)
	require.Len(suite.T(), objects, newObjects+objsBeforeInsert)

	// get the first key and use that to get the data from within that object
	firstKey := objects[0].Key
	object, err := suite.minioContainer.ClientWrapper.Client.GetObject(context.Background(),
		suite.minioContainer.ClientWrapper.DefaultBucket,
		firstKey,
		minio.GetObjectOptions{},
	)
	suite.Require().NoError(err)
	// check the data
	data, err := io.ReadAll(object)
	suite.Require().NoError(err)

	keyNumber := strings.Split(firstKey, "-")[2]
	matchingData := fmt.Sprintf("test data %s", keyNumber)
	suite.Require().Equal(matchingData, string(data))
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
	suite.Require().NoError(err)

	data, err := suite.minioContainer.ClientWrapper.GetObjectAsBytes("test-object-for-get-test")
	suite.Require().NoError(err)
	suite.Require().Equal(dummyData, string(data))

}

func (suite *S3ClientSuite) TestUploadFile() {
	testfile := filepath.Join(projectpath.Root, "LICENSE")
	err := suite.minioContainer.ClientWrapper.UploadFile("testFiles/LICENSE", testfile)
	suite.Require().NoError(err)

	// get the data in testObj2 and make sure it is the same as testObj
	object, err := suite.minioContainer.ClientWrapper.Client.GetObject(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, "testFiles/LICENSE", minio.GetObjectOptions{})
	suite.Require().NoError(err)
	// check the data
	data, err := io.ReadAll(object)
	suite.Require().NoError(err)
	suite.Require().Contains(string(data), "Copyright")

}

func (suite *S3ClientSuite) TestHashMatch() {

	tmpFile, err := os.CreateTemp("", "test")
	suite.Require().NoError(err)
	dir := path.Dir(tmpFile.Name())
	base := path.Base(tmpFile.Name())
	const hash_test_prefix = "hash_test_prefix"
	exists, err := suite.minioContainer.ClientWrapper.MatchesWithLocalHash(hash_test_prefix, dir, base)
	suite.Require().NoError(err)
	suite.Require().False(exists)

	hash := sha256.New()
	_, err = io.Copy(hash, tmpFile)
	suite.Require().NoError(err)

	file, err := os.Create(tmpFile.Name() + ".sha256")
	suite.Require().NoError(err)
	defer func() {
		_ = os.Remove(file.Name())
	}()
	// populate the file hash locally
	_, err = io.Copy(file, bytes.NewReader(hash.Sum(nil)))
	suite.Require().NoError(err)

	// upload dummy file
	_, err = suite.minioContainer.ClientWrapper.Client.PutObject(context.Background(),
		suite.minioContainer.ClientWrapper.DefaultBucket,
		hash_test_prefix+"/"+base,
		bytes.NewReader([]byte("dummy data")),
		-1,
		minio.PutObjectOptions{},
	)
	suite.Require().NoError(err)

	// upload hash
	_, err = suite.minioContainer.ClientWrapper.Client.PutObject(context.Background(),
		suite.minioContainer.ClientWrapper.DefaultBucket,
		hash_test_prefix+"/"+base+".sha256",
		bytes.NewReader(hash.Sum(nil)),
		-1,
		minio.PutObjectOptions{},
	)

	suite.Require().NoError(err)
	exists, err = suite.minioContainer.ClientWrapper.MatchesWithLocalHash(hash_test_prefix, dir, base)
	suite.Require().NoError(err)
	suite.Require().True(exists)
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

// Test that the minio client conforms to the crud interface so gleaner can use it
func (suite *S3ClientSuite) TestCRUD() {
	testBytes := bytes.NewReader([]byte("test data"))
	err := suite.minioContainer.ClientWrapper.Store("test/testCRUD", testBytes)
	suite.Require().NoError(err)

	exists, err := suite.minioContainer.ClientWrapper.Exists("test/testCRUD")
	suite.Require().NoError(err)
	suite.Require().True(exists)

	data, err := suite.minioContainer.ClientWrapper.Get("test/testCRUD")
	suite.Require().NoError(err)
	defer func() { _ = data.Close() }()

	bytes, err := io.ReadAll(data)
	suite.Require().NoError(err)
	suite.Require().Equal("test data", string(bytes))

	err = suite.minioContainer.ClientWrapper.Remove("test/testCRUD")
	suite.Require().NoError(err)

	exists, err = suite.minioContainer.ClientWrapper.Exists("test/testCRUD")
	suite.Require().NoError(err)
	suite.Require().False(exists)
}

func (suite *S3ClientSuite) TestPull() {

	var data []string
	const prefix = "concat_test"

	// insert 100 data points into minio
	for i := range 100 {
		dataPoint := fmt.Sprintf("test data %d", i)
		data = append(data, dataPoint)
		err := suite.minioContainer.ClientWrapper.Store(fmt.Sprintf("%s/%d", prefix, i), bytes.NewReader([]byte(dataPoint)))
		suite.Require().NoError(err)
	}

	suite.T().Run("concat to a single file", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "concat")
		suite.Require().NoError(err)
		defer func() {
			err = os.Remove(tmpFile.Name())
			suite.Require().NoError(err)
		}()
		err = suite.minioContainer.ClientWrapper.Pull(context.Background(), prefix, tmpFile.Name(), false)
		suite.Require().NoError(err)

		concatData, err := os.ReadFile(tmpFile.Name())
		suite.Require().NoError(err)

		concatAsString := string(concatData)

		for _, dataPoint := range data {
			suite.Require().Contains(concatAsString, dataPoint)
		}
	})

	suite.T().Run("pull to a dir", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "concat-*")
		tmpDir = tmpDir + "/"
		suite.Require().NoError(err)
		err = suite.minioContainer.ClientWrapper.Pull(context.Background(), prefix, tmpDir, false)
		suite.Require().NoError(err)

		files, err := os.ReadDir(tmpDir)
		suite.Require().NoError(err)
		for _, file := range files {
			fileData, err := os.ReadFile(filepath.Join(tmpDir, file.Name()))
			suite.Require().NoError(err)
			suite.Require().Contains(string(fileData), file.Name())
		}
	})

	suite.T().Run("pull to a dir with files named according to their hash", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "pull-hash-*")
		tmpDir = tmpDir + "/"
		suite.Require().NoError(err)
		err = suite.minioContainer.ClientWrapper.Pull(context.Background(), prefix, tmpDir, true)
		suite.Require().NoError(err)

		files, err := os.ReadDir(tmpDir)
		suite.Require().NoError(err)
		for _, file := range files {
			if file.Name() == "hash_to_filename.json" {
				continue
			}
			fileData, err := os.ReadFile(filepath.Join(tmpDir, file.Name()))
			suite.Require().NoError(err)
			sha256OfFile := sha256.Sum256(fileData)
			hashHex := hex.EncodeToString(sha256OfFile[:])
			suite.Require().Equal(hashHex, file.Name())
		}
	})

	err := suite.minioContainer.ClientWrapper.Remove(prefix)
	suite.Require().NoError(err)
}

// Run the entire test suite
func TestS3ClientSuite(t *testing.T) {
	suite.Run(t, new(S3ClientSuite))
}
