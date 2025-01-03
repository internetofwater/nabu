package cli

import (
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func prefix(v1 *viper.Viper) error {
	log.Info("Nabu started with mode: prefix")
	client, err := synchronizer.NewSynchronizerClient(v1)
	if err != nil {
		log.Fatal(err)
	}
	err = client.ObjectAssembly()

	if err != nil {
		log.Error(err)
	}
	return err
}

// checkCmd represents the check command
var PrefixCmd = &cobra.Command{
	Use:   "prefix ",
	Short: "nabu prefix command",
	Long:  `Load graphs from prefix to triplestore`,
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
