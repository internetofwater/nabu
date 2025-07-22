// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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

// A writer that keeps track of the sum of all bytes
// It is essentially a hash that doesn't depend on order;
// it is a good fit for n-quads where we care about the data
// but not the order of the quads inside
type SumWriter struct {
	Sum uint64
}

// Write implements the io.Writer interface
func (sw *SumWriter) Write(p []byte) (n int, err error) {
	for _, b := range p {
		sw.Sum += uint64(b)
	}
	return len(p), nil
}

func (sw *SumWriter) ToString() string {
	return fmt.Sprintf("%d", sw.Sum)
}

func ByteSum(b []byte) uint64 {
	var sum uint64
	for _, v := range b {
		sum += uint64(v)
	}
	return sum
}
