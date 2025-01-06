package synchronizer

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"nabu/internal/common"
	"strings"

	"github.com/minio/minio-go/v7"
	log "github.com/sirupsen/logrus"
)

func getObjectsAndWriteToPipe(synchronizer *SynchronizerClient, prefix string, pipeWriter *io.PipeWriter) error {
	defer func(pw *io.PipeWriter) {
		err := pw.Close()
		if err != nil {
			log.Error(err)
		}
	}(pipeWriter)

	matches, err := synchronizer.s3Client.NumberOfMatchingObjects([]string{prefix})
	if err != nil {
		return err
	}
	singleFileMode := false
	if matches == 1 {
		singleFileMode = true
		log.Printf("Single file mode set: %t", singleFileMode)
	}
	log.Printf("\nChannel/object length: %d\n", matches)

	objects, err := synchronizer.s3Client.GetObjects([]string{prefix})
	if err != nil {
		return err
	}

	for _, object := range objects {

		retrievedObject, err := synchronizer.s3Client.Client.GetObject(context.Background(), synchronizer.s3Client.DefaultBucket, object.Key, minio.GetObjectOptions{})
		if err != nil {
			return err
		}
		_, err = io.Copy(pipeWriter, retrievedObject)

		if err != nil {
			fmt.Println(err)
		}

		var buffer bytes.Buffer
		bufferWriter := bufio.NewWriter(&buffer)

		_, err = io.Copy(bufferWriter, retrievedObject)
		if err != nil {
			log.Println(err)
		}

		jsonldString := buffer.String()

		nq := ""
		//log.Println("Calling JSONLDtoNQ")
		if strings.HasSuffix(object.Key, ".nq") {
			nq = jsonldString
		} else {
			nq, err = common.JsonldToNQ(jsonldString)
			if err != nil {
				return err
			}
		}

		var singleFileNquad string

		if singleFileMode {
			singleFileNquad = nq //  just pass through the RDF without trying to Skolemize since we ar a single fil
		} else {
			singleFileNquad, err = common.Skolemization(nq, object.Key)
			if err != nil {
				return err
			}
		}

		// 1) get graph URI
		ctx, err := common.MakeURN(object.Key)
		if err != nil {
			return err
		}
		// 2) convert NT to NQ
		csnq, err := common.NtToNq(singleFileNquad, ctx)
		if err != nil {
			return err
		}

		_, err = pipeWriter.Write([]byte(csnq))
		if err != nil {
			return err
		}
	}
	return nil
}
