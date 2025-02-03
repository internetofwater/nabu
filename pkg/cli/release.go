package cli

import (
	"nabu/internal/synchronizer"
	"nabu/pkg/config"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func release(v1 *viper.Viper) error {
	client, err := synchronizer.NewSynchronizerClientFromViper(v1)
	if err != nil {
		return err
	}
	objConfig, err := config.GetConfigForS3Objects(v1)
	if err != nil {
		return err
	}

	err = client.GenerateNqReleaseAndArchiveOld(objConfig.Prefixes)

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
		err := release(viperVal)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(releaseCmd)
}
