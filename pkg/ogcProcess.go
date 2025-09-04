// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package pkg

import (
	"io"
	"net/http"
)

type OgcAPIProcessClient struct {
	// base url to the api
	BaseUrl string
}

func NewOgcAPIProcessClient(baseUrl string) *OgcAPIProcessClient {
	return &OgcAPIProcessClient{
		BaseUrl: baseUrl,
	}
}

func (c *OgcAPIProcessClient) RunProcess(processName string) (string, error) {
	url := c.BaseUrl + "/processes/" + processName + "/execution?f=json"

	resp, err := http.Get(url)

	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return "", err
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(bodyBytes), nil
}
