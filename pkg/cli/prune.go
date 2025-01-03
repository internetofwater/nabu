package cli

import (
	"github.com/minio/minio-go/v7"
	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func prune(v1 *viper.Viper, mc *minio.Client) error {
	log.Info("Prune graphs in triplestore not in objectVal store")
	err := prune.Snip(v1, mc)
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
	Long: `This will read the configs/{cfgPath}/gleaner file, and try to connect to the minio server`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Prune call started")
		err := prune(viperVal, mc)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(pruneCmd)
}
