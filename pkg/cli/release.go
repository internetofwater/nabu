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

	err = client.GenerateNqReleaseAndArchiveOld(cfgStruct.Prefixes)

	if err != nil {
		log.Error(err)
	}
	return err
}

var releaseCmd = &cobra.Command{
	Use:   "release",
	Short: "nabu release command",
	Long:  `Generate static file nq releases for the indexes sources and also a master release`,
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
