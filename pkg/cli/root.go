package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"nabu/internal/common"
	"nabu/internal/common/projectpath"
	"nabu/internal/synchronizer/s3"
	"nabu/pkg/config"

	"runtime/trace"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var cfgFile, minioVal, accessVal, secretVal, bucketVal, endpointVal, prefixVal, repositoryVal, logVal string
var portVal, batchVal int
var sslVal, dangerousVal, doTrace bool

var cfgStruct config.NabuConfig

var rootCmd = &cobra.Command{
	Use:   "nabu",
	Short: "nabu",
	Long:  "nabu",
}

func Execute() {
	err := rootCmd.Execute()
	if trace.IsEnabled() {
		trace.Stop()
	}
	cobra.CheckErr(err)

	if common.PROFILING_ENABLED() {
		mc, minioErr := s3.NewMinioClientWrapper(cfgStruct.Minio)
		cobra.CheckErr(minioErr)
		traceFile := filepath.Join(projectpath.Root, "trace.out")
		joinedArgs := strings.Join(rootCmd.Flags().Args(), "_")

		traceName := fmt.Sprintf("traces/trace_%s.out", joinedArgs)
		uploadErr := mc.UploadFile(traceName, traceFile)
		cobra.CheckErr(uploadErr)

		uploadErr = mc.UploadFile(fmt.Sprintf("traces/http_trace_%s.csv", joinedArgs), filepath.Join(projectpath.Root, "http_trace.csv"))
		cobra.CheckErr(uploadErr)
	}

}

func init() {

	cobra.OnInitialize(initLogging)
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&prefixVal, "prefix", "", "prefix to operate upon")
	rootCmd.PersistentFlags().StringVar(&endpointVal, "endpoint", "", "endpoint for server for the SPARQL endpoints")
	rootCmd.PersistentFlags().StringVar(&cfgFile, "cfg", "", "full path to yaml config file for nabu")
	rootCmd.PersistentFlags().StringVar(&minioVal, "address", "", "The address of the minio server")
	rootCmd.PersistentFlags().StringVar(&accessVal, "access", os.Getenv("S3_ACCESS_KEY"), "Access Key (i.e. username)")
	rootCmd.PersistentFlags().StringVar(&secretVal, "secret", os.Getenv("S3_SECRET_KEY"), "Secret access key")
	rootCmd.PersistentFlags().StringVar(&bucketVal, "bucket", "", "The configuration bucket")
	rootCmd.PersistentFlags().StringVar(&repositoryVal, "repository", "", "the default repository to use for graphdb")
	rootCmd.PersistentFlags().StringVar(&logVal, "log-level", "INFO", "the log level to use for the nabu logger")

	rootCmd.PersistentFlags().BoolVar(&sslVal, "ssl", false, "Use SSL boolean")
	rootCmd.PersistentFlags().BoolVar(&dangerousVal, "dangerous", false, "Use dangerous mode boolean")
	rootCmd.PersistentFlags().BoolVar(&doTrace, "trace", false, "Enable tracing")

	rootCmd.PersistentFlags().IntVar(&portVal, "port", -1, "Port for s3 server")
	rootCmd.PersistentFlags().IntVar(&batchVal, "upsert-batch-size", 1, "The batch size to use when syncing data from s3 to triplestore")
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
	// go structs default to 0 so we need to set it to 1 by defaults
	cfgStruct.Sparql.Batch = 1
	if batchVal != 1 {
		cfgStruct.Sparql.Batch = batchVal
	}

	if common.PROFILING_ENABLED() || doTrace {
		filePath := filepath.Join(projectpath.Root, "trace.out")
		log.Infof("Trace enabled; Outputting to %s", filePath)
		cfgStruct.Trace = true
		os.Setenv("NABU_PROFILING", "True")

		f, err := os.Create(filePath)
		if err != nil {
			log.Fatal(err)
		}
		if err := trace.Start(f); err != nil {
			log.Fatal(err)
		}
	}
}

func initLogging() {
	switch logVal {
	case "DEBUG":
		log.SetLevel(log.DebugLevel)
	case "INFO":
		log.SetLevel(log.InfoLevel)
	case "WARN":
		log.SetLevel(log.WarnLevel)
	case "ERROR":
		log.SetLevel(log.ErrorLevel)
	case "FATAL":
		log.SetLevel(log.FatalLevel)
	default:
		log.Fatalf("Invalid log level: %s", logVal)
	}
	log.SetFormatter(&log.JSONFormatter{})
}
