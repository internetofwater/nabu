package cli

import (
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

func clear() error {
	log.Info("Nabu started with mode: clear")

	synchronizerClient, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}
	err = synchronizerClient.GraphClient.ClearAllGraphs()
	if err != nil {
		return err
	}

	return nil
}

// checkCmd represents the check command
var ClearCmd = &cobra.Command{
	Use:   "clear",
	Short: "nabu clear command",
	Long:  `Removes all graphs from a SPARQL endpoint `,
	Run: func(cmd *cobra.Command, args []string) {
		err := clear()
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(ClearCmd)
}
