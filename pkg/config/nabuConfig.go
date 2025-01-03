package config

import (
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

var nabuTemplate = map[string]interface{}{
	"minio":   MinioTemplate,
	"sparql":  sparqlTemplate,
	"objects": ObjectTemplate,
}

func fileNameWithoutExtTrimSuffix(fileName string) string {
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}

func ReadNabuConfig(filename string, cfgPath string) (*viper.Viper, error) {
	v := viper.New()
	for key, value := range nabuTemplate {
		v.SetDefault(key, value)
	}

	v.SetConfigName(fileNameWithoutExtTrimSuffix(filename))
	v.AddConfigPath(cfgPath)
	v.SetConfigType("yaml")
	//v.BindEnv("headless", "GLEANER_HEADLESS_ENDPOINT")
	_ = v.BindEnv("minio.address", "MINIO_ADDRESS")
	_ = v.BindEnv("minio.port", "MINIO_PORT")
	_ = v.BindEnv("minio.ssl", "MINIO_USE_SSL")
	_ = v.BindEnv("minio.accesskey", "MINIO_ACCESS_KEY")
	_ = v.BindEnv("minio.secretkey", "MINIO_SECRET_KEY")
	_ = v.BindEnv("minio.bucket", "MINIO_BUCKET")
	v.AutomaticEnv()
	err := v.ReadInConfig()
	return v, err
}
