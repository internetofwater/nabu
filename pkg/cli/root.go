package cli

import (
	"os"
	"path/filepath"

	"nabu/internal/common"
	"nabu/pkg/config"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var cfgFile, nabuConfName, minioVal, accessVal, secretVal, bucketVal, endpointVal, prefixVal, repositoryVal string
var portVal int
var sslVal, dangerousVal bool

var cfgStruct config.NabuConfig

var rootCmd = &cobra.Command{
	Use:   "nabu",
	Short: "nabu",
	Long:  "nabu",
}

func Execute() {
	err := rootCmd.Execute()
	cobra.CheckErr(err)
}

func init() {
	common.InitLogging()

	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&prefixVal, "prefix", "", "prefix to operate upon")
	rootCmd.PersistentFlags().StringVar(&endpointVal, "endpoint", "", "endpoint for server for the SPARQL endpoints")
	rootCmd.PersistentFlags().StringVar(&nabuConfName, "", "", "config file to use for nabu")
	rootCmd.PersistentFlags().StringVar(&cfgFile, "cfg", "", "full path to yaml config file for nabu")
	rootCmd.PersistentFlags().StringVar(&minioVal, "address", "", "")
	rootCmd.PersistentFlags().IntVar(&portVal, "port", -1, "Port for s3 server")
	rootCmd.PersistentFlags().StringVar(&accessVal, "access", os.Getenv("S3_ACCESS_KEY"), "Access Key (i.e. username)")
	rootCmd.PersistentFlags().StringVar(&secretVal, "secret", os.Getenv("S3_SECRET_KEY"), "Secret access key")
	rootCmd.PersistentFlags().StringVar(&bucketVal, "bucket", "", "The configuration bucket")
	rootCmd.PersistentFlags().StringVar(&repositoryVal, "repository", "", "the default repository to use for graphdb")

	rootCmd.PersistentFlags().BoolVar(&sslVal, "ssl", false, "Use SSL boolean")
	rootCmd.PersistentFlags().BoolVar(&dangerousVal, "dangerous", false, "Use dangerous mode boolean")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	var err error
	if cfgFile != "" {
		var configPath string
		fileName := filepath.Base(cfgFile)

		// If the path is absolute, use it directly
		if filepath.IsAbs(cfgFile) {
			configPath = filepath.Dir(cfgFile)
		} else {
			// If it's a relative path, resolve it against the current working directory
			configPath, err = os.Getwd()
			if err != nil {
				log.Fatalf("cannot get current directory: %s", err)
			}
			configPath = filepath.Join(configPath, filepath.Dir(cfgFile))
		}

		// Make sure the file exists in the resolved path
		if _, err = os.Stat(filepath.Join(configPath, fileName)); os.IsNotExist(err) {
			log.Fatalf("config file does not exist at path: %s", filepath.Join(configPath, fileName))
		}

		cfgStruct, err = config.ReadNabuConfig(configPath, fileName)
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
	if sslVal {
		cfgStruct.Minio.Ssl = sslVal
	}
	if prefixVal != "" {
		cfgStruct.Prefixes = []string{prefixVal}
	}
	if repositoryVal != "" {
		cfgStruct.Sparql.Repository = repositoryVal
	}

}
