package cli

import (
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

func sync() error {
	log.Info("sync graphs in triplestore not in objectVal store")
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}
	err = client.SyncTriplestoreGraphs(cfgStruct.Prefixes)
	if err != nil {
		log.Error(err)
	}
	return err
}

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "nabu sync command",
	Long:  `Remove graphs in triplestore not in the s3 store and add graphs in the s3 store not in the triplestore`,
	Run: func(cmd *cobra.Command, args []string) {
		err := sync()
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(syncCmd)
}
