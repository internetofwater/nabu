package cli

import (
	"nabu/internal/synchronizer"
	"nabu/pkg/config"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func prefix(v1 *viper.Viper) error {
	log.Info("Nabu started with mode: prefix")
	client, err := synchronizer.NewSynchronizerClientFromViper(v1)
	if err != nil {
		log.Fatal(err)
	}
	objConfig, err := config.GetConfigForS3Objects(v1)
	if err != nil {
		return err
	}
	err = client.CopyAllPrefixedObjToTriplestore(objConfig.Prefixes)

	if err != nil {
		return err
	}
	return err
}

var PrefixCmd = &cobra.Command{
	Use:   "prefix ",
	Short: "nabu prefix command",
	Long:  `Load graphs in s3 with a specific prefix into the triplestore`,
	Run: func(cmd *cobra.Command, args []string) {
		err := prefix(viperVal)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(PrefixCmd)
}
