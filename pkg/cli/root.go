package cli

import (
	"mime"
	"os"
	"path/filepath"

	"nabu/internal/common"
	"nabu/pkg/config"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var cfgFile, nabuConfName, minioVal, accessVal, secretVal, bucketVal, endpointVal, prefixVal string
var portVal int
var sslVal, dangerousVal bool

var cfgStruct config.NabuConfig

var rootCmd = &cobra.Command{
	Use:   "nabu",
	Short: "nabu",
	Long:  "nabu",
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	common.InitLogging()

	err := mime.AddExtensionType(".jsonld", "application/ld+json")
	if err != nil {
		log.Fatal(err)
	}

	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&prefixVal, "prefix", "", "prefix to operate upon")
	rootCmd.PersistentFlags().StringVar(&endpointVal, "endpoint", "", "endpoint for server for the SPARQL endpoints")
	rootCmd.PersistentFlags().StringVar(&nabuConfName, "", "", "config file to use for nabu")
	rootCmd.PersistentFlags().StringVar(&cfgFile, "cfg", "", "full path to yaml config file for nabu")
	rootCmd.PersistentFlags().StringVar(&minioVal, "address", "", "hostname for s3 server")
	rootCmd.PersistentFlags().IntVar(&portVal, "port", -1, "Port for s3 server")
	rootCmd.PersistentFlags().StringVar(&accessVal, "access", os.Getenv("MINIO_ACCESS_KEY"), "Access Key (i.e. username)")
	rootCmd.PersistentFlags().StringVar(&secretVal, "secret", os.Getenv("MINIO_SECRET_KEY"), "Secret access key")
	rootCmd.PersistentFlags().StringVar(&bucketVal, "bucket", "", "The configuration bucket")

	rootCmd.PersistentFlags().BoolVar(&sslVal, "ssl", false, "Use SSL boolean")
	rootCmd.PersistentFlags().BoolVar(&dangerousVal, "dangerous", false, "Use dangerous mode boolean")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	var err error
	if cfgFile != "" {
		cfgStruct, err = config.ReadNabuConfig(nabuConfName, filepath.Dir(cfgFile))
		if err != nil {
			log.Fatalf("cannot read config %s", err)
		}
	} else {
		log.Fatal("FATAL: no config file provided with --cfg")
	}

	if endpointVal != "" {
		cfgStruct.Sparql.Endpoint = endpointVal
	}
	if minioVal != "" {
		cfgStruct.Minio.Address = minioVal
	}
	if portVal != -1 {
		cfgStruct.Minio.Port = portVal
	}
	if accessVal != "" {
		cfgStruct.Minio.Accesskey = accessVal
	}
	if secretVal != "" {
		cfgStruct.Minio.Secretkey = secretVal
	}
	if bucketVal != "" {
		cfgStruct.Minio.Bucket = bucketVal
	}
	if !sslVal {
		cfgStruct.Minio.Ssl = sslVal
	}
	if prefixVal != "" {
		cfgStruct.Prefixes = []string{prefixVal}
	}

}
