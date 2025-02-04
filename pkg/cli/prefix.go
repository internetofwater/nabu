package cli

import (
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

func prefix() error {
	log.Info("Nabu started with mode: prefix")
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		log.Fatal(err)
	}
	err = client.CopyAllPrefixedObjToTriplestore(cfgStruct.Prefixes)

	if err != nil {
		return err
	}
	return err
}

var PrefixCmd = &cobra.Command{
	Use:   "prefix",
	Short: "nabu prefix command",
	Long:  `Load graphs in s3 with a specific prefix into the triplestore`,
	Run: func(cmd *cobra.Command, args []string) {
		err := prefix()
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(PrefixCmd)
}
