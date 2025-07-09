// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"

	"github.com/minio/minio-go/v7"
	log "github.com/sirupsen/logrus"
)

// a chunk of bytes that can be passed between channels
type chunk struct {
	data []byte
	err  error
}

const FourMB = 1024 * 1024 * 4

// Download a single object and write it to a channel; return the size of the object
func getObjAndWriteToChannel(ctx context.Context, m *MinioClientWrapper, obj *minio.ObjectInfo, ch chan<- chunk) (int64, error) {
	log.Debugf("Downloading %s of size %0.2fMB", obj.Key, float64(obj.Size)/(1024*1024))
	ob, err := m.Client.GetObject(ctx, m.DefaultBucket, obj.Key, minio.GetObjectOptions{})
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := ob.Close(); err != nil {
			log.Error(err)
		}
	}()
	buf := make([]byte, FourMB)
	for {
		// read up to the size of the buffer
		n, err := ob.Read(buf)
		if n > 0 {
			// Copy the data to a new slice to avoid data race
			// this is since bytes are a reference type and
			// thus to pass this data to another goroutine
			// could cause a data race
			dataCopy := make([]byte, n)
			copy(dataCopy, buf[:n])
			ch <- chunk{data: dataCopy, err: nil}
		}
		// at the end of the file return
		if err == io.EOF {
			break
		}
		if err != nil {
			ch <- chunk{err: err}
			break
		}
	}
	return obj.Size, nil
}

// write the data to the destination and return the sha256
// by using a tee; this makes it so we only read from the object once
func writeAndReturnSHA256(destination io.Writer, source io.Reader) (string, error) {
	hashDestination := sha256.New()
	tee := io.TeeReader(source, hashDestination)
	_, err := io.Copy(destination, tee)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hashDestination.Sum(nil)), nil
}
