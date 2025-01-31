package cli

import (
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func clear(v1 *viper.Viper) error {
	log.Info("Nabu started with mode: clear")

	dangerous := v1.GetBool("flags.dangerous")

	if dangerous {
		log.Println("dangerous mode is enabled")
		synchronizerClient, err := synchronizer.NewSynchronizerClientFromViper(v1)
		if err != nil {
			return err
		}
		err = synchronizerClient.GraphClient.ClearAllGraphs()
		if err != nil {
			return err
		}
	} else {
		log.Fatal("dangerous mode must be set to true to run this")
	}

	return nil
}

// checkCmd represents the check command
var ClearCmd = &cobra.Command{
	Use:   "clear ",
	Short: "nabu clear command",
	Long:  `Removes all graphs from a SPARQL endpoint `,
	Run: func(cmd *cobra.Command, args []string) {
		err := clear(viperVal)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(ClearCmd)
}
