package synchronizer

import (
	"context"
	"nabu/internal/synchronizer/objects"
	"nabu/internal/synchronizer/triplestore"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SynchronizerClientSuite struct {
	suite.Suite
	minioContainer   objects.MinioContainer
	graphdbContainer triplestore.GraphDBContainer
}

func (suite *SynchronizerClientSuite) SetupSuite() {
	minioContainer, err := objects.NewMinioContainer("minioadmin", "minioadmin", "gleanerbucket")
	suite.Require().NoError(err)
	suite.minioContainer = minioContainer

	err = suite.minioContainer.ClientWrapper.Client.MakeBucket(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, minio.MakeBucketOptions{})
	require.NoError(suite.T(), err)

	graphdbContainer, err := triplestore.NewGraphDBContainer("gleaner")
	suite.Require().NoError(err)
	suite.graphdbContainer = graphdbContainer

}

func TestSyncheronizerClientSuite(t *testing.T) {
	suite.Run(t, new(SynchronizerClientSuite))
}
