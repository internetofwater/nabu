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
	"github.com/spf13/viper"
)

// global viper instance that reads in cli/viperConfig data
// should not be used in internal code
var viperConfig *viper.Viper

// global config struct that is the marshalled version of the viper config
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

	if common.PROFILING_ENABLED() || cfgStruct.Trace {
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

	viperConfig = viper.New()

	rootCmd.PersistentFlags().String("prefix", "", "prefix to operate upon")
	rootCmd.PersistentFlags().String("endpoint", "", "endpoint for server for the SPARQL endpoints")
	rootCmd.PersistentFlags().String("cfg", "nabuconfig.yaml", "full path to yaml config file for nabu")
	rootCmd.PersistentFlags().String("address", "", "The address of the minio server")
	rootCmd.PersistentFlags().String("access", os.Getenv("S3_ACCESS_KEY"), "Access Key (i.e. username)")
	rootCmd.PersistentFlags().String("secret", os.Getenv("S3_SECRET_KEY"), "Secret access key")
	rootCmd.PersistentFlags().String("bucket", "", "The configuration bucket")
	rootCmd.PersistentFlags().String("repository", "", "the default repository to use for graphdb")
	rootCmd.PersistentFlags().String("log-level", "INFO", "the log level to use for the nabu logger")

	rootCmd.PersistentFlags().Bool("ssl", false, "Use SSL boolean")
	rootCmd.PersistentFlags().Bool("dangerous", false, "Use dangerous mode boolean")
	rootCmd.PersistentFlags().Bool("trace", false, "Enable tracing")

	rootCmd.PersistentFlags().Int("port", -1, "Port for s3 server")
	rootCmd.PersistentFlags().Int("upsert-batch-size", 1, "The batch size to use when syncing data from s3 to triplestore")
	if err := viperConfig.BindPFlags(rootCmd.PersistentFlags()); err != nil {
		log.Fatalf("Error binding flags: %v", err)
	}

	cobra.OnInitialize(initConfig)
	cobra.OnInitialize(func() {
		initLogging(viperConfig.GetString("log-level"))
	})
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	customConfPath, err := rootCmd.PersistentFlags().GetString("config")
	if err != nil {
		log.Fatal(fmt.Errorf("failed to get config path: %w", err))
	}
	viperConfig.SetConfigFile(filepath.Base(customConfPath))
	viperConfig.SetConfigName("nabuconfig")
	viperConfig.SetConfigType("yaml")
	viperConfig.AddConfigPath(".")

	if err := viperConfig.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Info("Config file not found, using CLI flags and defaults only")
		} else {
			log.Fatalf("Error reading config file: %v", err)
		}
	}

	cfgStruct, err = config.NewNabuConfigFromViper(viperConfig)
	if err != nil {
		log.Fatal(err)
	}

	if common.PROFILING_ENABLED() || cfgStruct.Trace {
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

func initLogging(logVal string) {
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
	log.SetReportCaller(true)
	log.SetFormatter(&log.JSONFormatter{})
}
