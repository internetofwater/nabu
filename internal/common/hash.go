// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

// write the data to the destination and return the sha256
// by using a tee; this makes it so we only read from the object once
func WriteAndReturnSHA256(destination io.Writer, source io.Reader) (string, error) {
	hashDestination := sha256.New()
	tee := io.TeeReader(source, hashDestination)
	_, err := io.Copy(destination, tee)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hashDestination.Sum(nil)), nil
}
