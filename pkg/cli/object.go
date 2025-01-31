package cli

import (
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func object(v1 *viper.Viper, objectName string) error {
	client, err := synchronizer.NewSynchronizerClientFromViper(v1)
	if err != nil {
		return err
	}
	err = client.UploadNqFileToTriplestore(objectName)
	if err != nil {
		return err
	}

	return nil
}

// checkCmd represents the check command
var objectCmd = &cobra.Command{
	Use:   "object",
	Short: "nabu object command",
	Long:  `Load a single n-quads graph object from s3 into the triplestore`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 {
			objectVal := args[0]
			err := object(viperVal, objectVal)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("must have exactly one argument which is the object to load")
		}

	},
}

func init() {
	rootCmd.AddCommand(objectCmd)

	// Here you will define your flags and configuration settings.
	// bucketVal is available at top level
	//objectCmd.Flags().StringVar(&objectVal, "object", "", "object to load")
	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// checkCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// checkCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
