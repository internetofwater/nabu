package cli

import (
	"context"
	"fmt"
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

func test() error {
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}

	exists, err := client.S3Client.Client.BucketExists(context.Background(), cfgStruct.Minio.Bucket)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("default bucket %s does not exist", cfgStruct.Minio.Bucket)
	}

	// url := client.GraphClient.BaseUrl

	// if url == "" {
	// 	return fmt.Errorf("graph url not set")
	// }

	// // try to ping the graph database using http
	// resp, err := http.Get(url)

	log.Info("tests passed")

	return err
}

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "test to connect to s3 and triplestore",
	Long:  `Test to see if nabu can connect to s3 and triplestore but don't do anything`,
	Run: func(cmd *cobra.Command, args []string) {
		err := test()
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(testCmd)
}
