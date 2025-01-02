package cli

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"os"
	"path"
	"path/filepath"

	"nabu/internal/common"
	"nabu/internal/objects"
	"nabu/pkg/config"

	"github.com/minio/minio-go/v7"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/spf13/viper"
)

var cfgFile, cfgName, cfgPath, nabuConfName string
var minioVal, portVal, accessVal, secretVal, bucketVal string
var sslVal, dangerousVal bool
var viperVal *viper.Viper
var mc *minio.Client
var prefixVal, endpointVal string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "nabu",
	Short: "nabu ",
	Long: `nabu
`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	common.InitLogging()

	err := mime.AddExtensionType(".jsonld", "application/ld+json")
	if err != nil {
		log.Fatal(err)
	}

	akey := os.Getenv("MINIO_ACCESS_KEY")
	skey := os.Getenv("MINIO_SECRET_KEY")
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&prefixVal, "prefix", "", "prefix to run. use source in future.")

	rootCmd.PersistentFlags().StringVar(&endpointVal, "endpoint", "", "end point server set for the SPARQL endpoints")

	rootCmd.PersistentFlags().StringVar(&cfgPath, "cfgPath", "configs", "base location for config files (default is configs/)")
	rootCmd.PersistentFlags().StringVar(&cfgName, "cfgName", "local", "config file (default is local so configs/local)")
	rootCmd.PersistentFlags().StringVar(&nabuConfName, "nabuConfName", "nabu", "config file (default is local so configs/local)")
	rootCmd.PersistentFlags().StringVar(&cfgFile, "cfg", "", "compatibility/overload: full path to config file (default location gleaner in configs/local)")

	// minio env variables
	rootCmd.PersistentFlags().StringVar(&minioVal, "address", "localhost", "FQDN for server")
	rootCmd.PersistentFlags().StringVar(&portVal, "port", "9000", "Port for minio server, default 9000")
	rootCmd.PersistentFlags().StringVar(&accessVal, "access", akey, "Access Key ID")
	rootCmd.PersistentFlags().StringVar(&secretVal, "secret", skey, "Secret access key")
	rootCmd.PersistentFlags().StringVar(&bucketVal, "bucket", "gleaner", "The configuration bucket")

	rootCmd.PersistentFlags().BoolVar(&sslVal, "ssl", false, "Use SSL boolean")
	rootCmd.PersistentFlags().BoolVar(&dangerousVal, "dangerous", false, "Use dangerous mode boolean")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	var err error
	//viperVal := viper.New()
	if cfgFile != "" {
		// Use config file from the flag.
		//viperVal.SetConfigFile(cfgFile)
		viperVal, err = config.ReadNabuConfig(filepath.Base(cfgFile), filepath.Dir(cfgFile))
		if err != nil {
			log.Fatalf("cannot read config %s", err)
		}
	} else {
		// Find home directory.
		//home, err := os.UserHomeDir()
		//cobra.CheckErr(err)
		//
		//// Search config in home directory with name "nabu" (without extension).
		//viperVal.AddConfigPath(home)
		//viperVal.AddConfigPath(path.Join(cfgPath, cfgName))
		//viperVal.SetConfigType("yaml")
		//viperVal.SetConfigName("nabu")
		viperVal, err = config.ReadNabuConfig(nabuConfName, path.Join(cfgPath, cfgName))
		if err != nil {
			log.Fatalf("cannot read config %s", err)
		}
	}

	//viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.

	mc, err = objects.MinioConnection(viperVal)
	if err != nil {
		log.Fatalf("cannot connect to minio: %s", err)
	}

	_, err = mc.ListBuckets(context.Background())
	if err != nil {
		err = errors.New(err.Error() + fmt.Sprintf(" Ignore that. It's not the bucket. check config/minio: address, port, ssl. connection info: endpoint: %v ", mc.EndpointURL()))
		log.Fatal("cannot connect to minio: ", err)
	}

	bucketVal, err = config.GetBucketName(viperVal)
	if err != nil {
		log.Fatalf("cannot read bucketname from : %s ", err)
	}
	// Override prefix in config if flag set
	//if isFlagPassed("prefix") {
	//	out := viperVal.GetStringMapString("objects")
	//	b := out["bucket"]
	//	p := prefixVal
	//	// r := out["region"]
	//	// v1.Set("objects", map[string]string{"bucket": b, "prefix": NEWPREFIX, "region": r})
	//	viperVal.Set("objects", map[string]string{"bucket": b, "prefix": p})
	//}

	if dangerousVal {
		viperVal.Set("flags.dangerous", true)
	}

	if endpointVal != "" {
		viperVal.Set("flags.endpoint", endpointVal)
	}

	if prefixVal != "" {
		//out := viperVal.GetStringMapString("objects")
		//d := out["domain"]

		var p []string
		p = append(p, prefixVal)

		viperVal.Set("objects.prefix", p)

		//p := prefixVal
		// r := out["region"]
		// v1.Set("objects", map[string]string{"bucket": b, "prefix": NEWPREFIX, "region": r})
		//viperVal.Set("objects", map[string]string{"domain": d, "prefix": p})
	}

}
