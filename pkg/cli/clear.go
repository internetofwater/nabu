package cli

import (
	"nabu/internal/synchronizer/graph"

	"os"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func clear(v1 *viper.Viper) error {
	log.Info("Nabu started with mode: clear")

	d := v1.GetBool("flags.dangerous")

	if d {
		log.Println("dangerous mode is enabled")
		graph := graph.GraphDbClient{}
		err := graph.ClearAllGraphs()
		if err != nil {
			log.Error(err)
			return err
		}
	} else {
		log.Println("dangerous mode must be set to true to run this")
		return nil
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
			os.Exit(1)
		}
		os.Exit(0)
	},
}

func init() {
	rootCmd.AddCommand(ClearCmd)
}
