package cli

import (
	"nabu/internal/synchronizer"
	"nabu/pkg/config"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func prune(v1 *viper.Viper) error {
	log.Info("Prune graphs in triplestore not in objectVal store")
	client, err := synchronizer.NewSynchronizerClientFromViper(v1)
	if err != nil {
		return err
	}
	objConfig, err := config.GetConfigForS3Objects(v1)
	if err != nil {
		return err
	}
	err = client.RemoveGraphsNotInS3(objConfig.Prefixes)
	if err != nil {
		log.Error(err)
	}
	return err
}

// checkCmd represents the check command
var pruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "nabu prune command",
	// NOTE: this is marked as not implemented in the upstream, but appears to be implemented here
	Long: `Remove graphs in triplestore not in the s3 store`,
	Run: func(cmd *cobra.Command, args []string) {
		err := prune(viperVal)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(pruneCmd)
}
