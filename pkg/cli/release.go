package cli

import (
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func release(v1 *viper.Viper) error {
	client, err := synchronizer.NewSynchronizerClient(v1)
	if err != nil {
		return err
	}
	prefixes := []string{}
	err = client.GenerateNqReleaseAndArchiveOld(prefixes)

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
