package cli

import (
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

func sync() error {
	log.Info("dropping graphs in triplestore not in s3 and adding graphs to triplestore that are missing from it but present in s3")
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}
	for _, prefix := range cfgStruct.Prefixes {
		err = client.SyncTriplestoreGraphs(prefix, true)
		if err != nil {
			log.Error(err)
		}
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
