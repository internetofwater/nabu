package nabu

import (
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

func release() error {
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}

	for _, prefix := range cfgStruct.Prefixes {
		err = client.GenerateNqRelease(prefix)
	}

	return err
}

var releaseCmd = &cobra.Command{
	Use:   "release",
	Short: "nabu release command",
	Long:  `Generate static file nq releases from jsonld files`,
	Run: func(cmd *cobra.Command, args []string) {
		err := release()
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(releaseCmd)
}
