// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
		Username:       "minioadmin",
		Password:       "minioadmin",
		DefaultBucket:  "nabutestbucket",
		MetadataBucket: "metadatabucket",
		ContainerName:  "objects_test_minio",
	}
	minioContainer, err := NewMinioContainerFromConfig(config)
	suite.Require().NoError(err)
	suite.minioContainer = minioContainer

	// create the bucket
	err = suite.minioContainer.ClientWrapper.SetupBuckets()
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

// Test that the minio client conforms to the crud interface so gleaner can use it
func (suite *S3ClientSuite) TestCRUD() {
	testBytes := bytes.NewReader([]byte("test data"))
	err := suite.minioContainer.ClientWrapper.StoreWithoutServersideHash("test/testCRUD", testBytes)
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
	const prefix = "pull_test/"

	// insert 100 data points into minio
	for i := range 100 {
		dataPoint := fmt.Sprintf("test data %d", i)
		data = append(data, dataPoint)
		err := suite.minioContainer.ClientWrapper.StoreWithoutServersideHash(fmt.Sprintf("%s%d", prefix, i), bytes.NewReader([]byte(dataPoint)))
		suite.Require().NoError(err)
	}

	suite.T().Run("concat to a single file", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "pull")
		suite.Require().NoError(err)
		defer func() {
			err = os.Remove(tmpFile.Name())
			suite.Require().NoError(err)
		}()
		err = suite.minioContainer.ClientWrapper.Pull(context.Background(), prefix, tmpFile.Name(), "")
		suite.Require().NoError(err)

		concatData, err := os.ReadFile(tmpFile.Name())
		suite.Require().NoError(err)

		concatAsString := string(concatData)

		for _, dataPoint := range data {
			suite.Require().Contains(concatAsString, dataPoint)
		}
	})

	suite.T().Run("concat to a single file fails with gzipped data since gzip cannot be naively concatenated", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "pull-gzip")

		const gzipped_prefix = "pull_test_with_gzip/"

		for i := range 10 {
			dataPoint := fmt.Sprintf("test data %d", i)
			data = append(data, dataPoint)
			err := suite.minioContainer.ClientWrapper.StoreWithoutServersideHash(fmt.Sprintf("%s%d.gz", gzipped_prefix, i), bytes.NewReader([]byte(dataPoint)))
			suite.Require().NoError(err)
		}

		suite.Require().NoError(err)
		defer func() {
			err = os.Remove(tmpFile.Name())
			suite.Require().NoError(err)
		}()
		err = suite.minioContainer.ClientWrapper.Pull(context.Background(), gzipped_prefix, tmpFile.Name(), "")
		suite.Require().Error(err)
	})

	suite.T().Run("pull separate files to a dir", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "pull-dir-*")
		tmpDir = tmpDir + "/"
		suite.Require().NoError(err)
		err = suite.minioContainer.ClientWrapper.Pull(context.Background(), prefix, tmpDir, "")
		suite.Require().NoError(err)

		files, err := os.ReadDir(tmpDir)
		suite.Require().NoError(err)
		for _, file := range files {
			fileData, err := os.ReadFile(filepath.Join(tmpDir, file.Name()))
			suite.Require().NoError(err)
			suite.Require().Contains(string(fileData), file.Name())
		}
	})

	suite.T().Run("substr filter", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "pull-dir-with-filter-*")
		tmpDir = tmpDir + "/"
		suite.Require().NoError(err)
		err = suite.minioContainer.ClientWrapper.Pull(context.Background(), prefix, tmpDir, "NOTHING_SHOULD_MATCH")
		suite.Require().NoError(err)

		files, err := os.ReadDir(tmpDir)
		suite.Require().NoError(err)
		suite.Require().Len(files, 0)

		err = suite.minioContainer.ClientWrapper.Pull(context.Background(), prefix, tmpDir, "99")
		suite.Require().NoError(err)

		files, err = os.ReadDir(tmpDir)
		suite.Require().NoError(err)
		suite.Require().Len(files, 1, "There should only be one file in the dir since there is only file with 99 in the name")
	})

	err := suite.minioContainer.ClientWrapper.Remove(prefix)
	suite.Require().NoError(err)
}

func (suite *S3ClientSuite) TestPullSkipsUnchangedFiles() {
	const prefix = "pull_unchanged_test/"
	for i := range 10 {
		err := suite.minioContainer.ClientWrapper.StoreWithoutServersideHash(fmt.Sprintf("%s%d.parquet", prefix, i), strings.NewReader(fmt.Sprintf("test data %d", i)))
		suite.Require().NoError(err)
	}

	tmpDir := suite.T().TempDir() + "/"
	err := suite.minioContainer.ClientWrapper.Pull(context.Background(), prefix, tmpDir, "")
	suite.Require().NoError(err)

	suite.T().Run("pulled files have the modified time of their object", func(t *testing.T) {
		files, err := os.ReadDir(tmpDir)
		require.NoError(t, err)
		require.Len(t, files, 10, "no extra files should be created")
		for i := range 10 {
			stat, err := suite.minioContainer.ClientWrapper.Client.StatObject(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, fmt.Sprintf("%s%d.parquet", prefix, i), minio.StatObjectOptions{})
			require.NoError(t, err)
			local, err := os.Stat(fmt.Sprintf("%s%d.parquet", tmpDir, i))
			require.NoError(t, err)
			require.True(t, local.ModTime().Truncate(time.Second).Equal(stat.LastModified.Truncate(time.Second)))
		}
	})

	// replace the contents of a pulled file with a marker of the same size while keeping
	// its modified time; if the file is downloaded again the marker is overwritten
	markAsPulled := func(t *testing.T, name string) string {
		info, err := os.Stat(tmpDir + name)
		require.NoError(t, err)
		marker := strings.Repeat("x", int(info.Size()))
		require.NoError(t, os.WriteFile(tmpDir+name, []byte(marker), 0644))
		require.NoError(t, os.Chtimes(tmpDir+name, time.Time{}, info.ModTime()))
		return marker
	}

	suite.T().Run("unchanged files are not pulled again", func(t *testing.T) {
		markers := map[string]string{}
		for i := range 10 {
			name := fmt.Sprintf("%d.parquet", i)
			markers[name] = markAsPulled(t, name)
		}
		err := suite.minioContainer.ClientWrapper.Pull(context.Background(), prefix, tmpDir, "")
		require.NoError(t, err)
		for name, marker := range markers {
			data, err := os.ReadFile(tmpDir + name)
			require.NoError(t, err)
			require.Equal(t, marker, string(data), "%s should not have been downloaded again", name)
		}
	})

	suite.T().Run("changed files are pulled again", func(t *testing.T) {
		// wait so the new upload has a different last modified time
		time.Sleep(time.Second)
		err := suite.minioContainer.ClientWrapper.StoreWithoutServersideHash(prefix+"0.parquet", strings.NewReader("changed"))
		require.NoError(t, err)
		unchangedMarker := markAsPulled(t, "1.parquet")
		err = suite.minioContainer.ClientWrapper.Pull(context.Background(), prefix, tmpDir, "")
		require.NoError(t, err)
		data, err := os.ReadFile(tmpDir + "0.parquet")
		require.NoError(t, err)
		require.Equal(t, "changed", string(data), "the old contents should be fully replaced")
		data, err = os.ReadFile(tmpDir + "1.parquet")
		require.NoError(t, err)
		require.Equal(t, unchangedMarker, string(data), "unchanged files should not be downloaded again")
	})

	suite.T().Run("locally modified files are pulled again", func(t *testing.T) {
		require.NoError(t, os.Chtimes(tmpDir+"1.parquet", time.Time{}, time.Now().Add(time.Hour)))
		err := suite.minioContainer.ClientWrapper.Pull(context.Background(), prefix, tmpDir, "")
		require.NoError(t, err)
		data, err := os.ReadFile(tmpDir + "1.parquet")
		require.NoError(t, err)
		require.Equal(t, "test data 1", string(data))
		stat, err := suite.minioContainer.ClientWrapper.Client.StatObject(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, prefix+"1.parquet", minio.StatObjectOptions{})
		require.NoError(t, err)
		local, err := os.Stat(tmpDir + "1.parquet")
		require.NoError(t, err)
		require.True(t, local.ModTime().Truncate(time.Second).Equal(stat.LastModified.Truncate(time.Second)))
	})

	err = suite.minioContainer.ClientWrapper.Remove(prefix)
	suite.Require().NoError(err)
}

func (suite *S3ClientSuite) TestIsEmpty() {
	// populate the minio bucket with 10 data points and their byte sums
	const prefix = "is_empty_test/"
	err := suite.minioContainer.ClientWrapper.StoreWithoutServersideHash(prefix+"test", bytes.NewReader([]byte("test data")))
	suite.Require().NoError(err)
	empty, err := suite.minioContainer.ClientWrapper.IsEmptyDir(prefix)
	suite.Require().NoError(err)
	suite.Require().False(empty)
}

func (suite *S3ClientSuite) TestGetMD5HashServerside() {
	const prefix = "hash_test/"
	data := []byte("test data")
	md5String := fmt.Sprintf("%x", md5.Sum(data))
	err := suite.minioContainer.ClientWrapper.StoreWithHash(prefix+"test", bytes.NewReader(data), len(data))
	suite.Require().NoError(err)
	hash, exists, err := suite.minioContainer.ClientWrapper.GetHash(prefix + "test")
	suite.Require().NoError(err)
	suite.Require().Equal(md5String, hash)
	suite.Require().True(exists)

	suite.T().Run("catches bad hash from multipart upload", func(t *testing.T) {
		const undefinedSize = -1
		// undefined size prevents minio from generating a hash
		err := suite.minioContainer.ClientWrapper.StoreWithHash(prefix+"testNoHash", bytes.NewReader(data), undefinedSize)
		suite.Require().NoError(err)

		_, file_exists, err := suite.minioContainer.ClientWrapper.GetHash(prefix + "testNoHash")
		suite.Require().Error(err)
		suite.Require().True(file_exists)
	})

}

func (suite *S3ClientSuite) TestFailedStreamingStoreKeepsPreviousObject() {
	const path = "streaming/file.parquet"
	store := suite.minioContainer.ClientWrapper
	suite.Require().NoError(store.StoreWithoutServersideHash(path, bytes.NewReader([]byte("original"))))

	pipeReader, pipeWriter := io.Pipe()
	go func() {
		_, _ = pipeWriter.Write([]byte("partial"))
		pipeWriter.CloseWithError(fmt.Errorf("upstream failure"))
	}()
	suite.Require().Error(store.StoreWithoutServersideHash(path, pipeReader))

	data, err := store.GetObjectAsBytes(path)
	suite.Require().NoError(err)
	suite.Require().Equal("original", string(data))
}

// Run the entire test suite
func TestS3ClientSuite(t *testing.T) {
	suite.Run(t, new(S3ClientSuite))
}

func TestIsUnchangedSincePull(t *testing.T) {
	localFile := filepath.Join(t.TempDir(), "summoned.parquet")
	lastModified := time.Date(2026, 10, 5, 12, 0, 0, 123_000_000, time.UTC)
	obj := minio.ObjectInfo{Key: "summoned/summoned.parquet", Size: 4, LastModified: lastModified}

	unchanged, err := isUnchangedSincePull(localFile, obj)
	require.NoError(t, err)
	require.False(t, unchanged, "a file that was never pulled must be pulled")

	require.NoError(t, os.WriteFile(localFile, []byte("data"), 0644))
	unchanged, err = isUnchangedSincePull(localFile, obj)
	require.NoError(t, err)
	require.False(t, unchanged, "a file with a different modified time must be pulled")

	// some filesystems only store the time to the second
	require.NoError(t, os.Chtimes(localFile, time.Time{}, lastModified.Truncate(time.Second)))
	unchanged, err = isUnchangedSincePull(localFile, obj)
	require.NoError(t, err)
	require.True(t, unchanged)

	withNewUpload := obj
	withNewUpload.LastModified = lastModified.Add(time.Second)
	unchanged, err = isUnchangedSincePull(localFile, withNewUpload)
	require.NoError(t, err)
	require.False(t, unchanged, "a newer upload must be pulled")

	withDifferentSize := obj
	withDifferentSize.Size = 5
	unchanged, err = isUnchangedSincePull(localFile, withDifferentSize)
	require.NoError(t, err)
	require.False(t, unchanged, "a file of a different size must be pulled")

	withoutTime := obj
	withoutTime.LastModified = time.Time{}
	unchanged, err = isUnchangedSincePull(localFile, withoutTime)
	require.NoError(t, err)
	require.False(t, unchanged, "an object without a modified time must always be pulled")
}
