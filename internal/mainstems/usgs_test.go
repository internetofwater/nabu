// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"testing"

	"github.com/internetofwater/nabu/internal/common"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestLocationWithoutMainstem(t *testing.T) {
	log.SetLevel(log.TraceLevel)

	mockResponses := map[string]common.MockResponse{
		"https://labs-beta.waterdata.usgs.gov/api/fabric/pygeoapi/collections/catchmentsp/items?f=json&skipGeometry=true&bbox=-105.534308,40.344080,-105.534308,40.344080": {
			File:       "testdata/catchmentForRise1.json",
			StatusCode: 200,
		},
		"https://api.water.usgs.gov/nldi/linked-data/comid/13718/navigation/UM/flowlines?f=json&distance=3000": {
			File:       "testdata/flowlinesForRise1.json",
			StatusCode: 200,
		},
		"https://reference.geoconnex.us/collections/mainstems/items?f=json&skipGeometry=true&head_nhdpv2_comid=https://geoconnex.us/nhdplusv2/comid/13718": {
			File:       "testdata/geoconnexURIForRise1.json",
			StatusCode: 200,
		},
	}

	mockClient := common.NewMockedClient(true,
		mockResponses,
	)

	client := NewUSGSMainstemService(mockClient)
	resp, err := client.GetMainstemForPoint(-105.5343083, 40.3440796)
	require.NoError(t, err)
	require.False(t, resp.foundAssociatedMainstem)
}

func TestLocationWithMainstem(t *testing.T) {

	mockResponses := map[string]common.MockResponse{
		"https://labs-beta.waterdata.usgs.gov/api/fabric/pygeoapi/collections/catchmentsp/items?f=json&skipGeometry=true&bbox=-106.736600,36.594800,-106.736600,36.594800": {
			File:       "testdata/catchmentForRise324.json",
			StatusCode: 200,
		},
		"https://api.water.usgs.gov/nldi/linked-data/comid/17844620/navigation/UM/flowlines?f=json&distance=3000": {
			File:       "testdata/flowlinesForRise324.json",
			StatusCode: 200,
		},
		"https://reference.geoconnex.us/collections/mainstems/items?f=json&skipGeometry=true&head_nhdpv2_comid=https://geoconnex.us/nhdplusv2/comid/24706958": {
			File:       "testdata/geoconnexURIForRise324.json",
			StatusCode: 200,
		},
	}

	mockClient := common.NewMockedClient(true,
		mockResponses,
	)

	client := NewUSGSMainstemService(mockClient)
	mainstem, err := client.GetMainstemForPoint(-106.7366, 36.5948)
	require.NoError(t, err)
	require.True(t, mainstem.foundAssociatedMainstem)
	require.Equal(t, "https://geoconnex.us/ref/mainstems/1608053", mainstem.mainstemURI)
}
