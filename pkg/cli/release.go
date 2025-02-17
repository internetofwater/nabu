package cli

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

	err = client.GenerateNqRelease(cfgStruct.Prefixes)

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
