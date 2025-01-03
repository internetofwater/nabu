package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Objects struct {
	Bucket string //`mapstructure:"MINIO_BUCKET"`
	Domain string //`mapstructure:"MINIO_DOMAIN"`
	Prefix []string
}

var ObjectTemplate = map[string]interface{}{
	"objects": map[string]interface{}{
		"bucket": "gleaner",
		"domain": "us-east-1",
		"prefix": map[string][]string{},
	},
}

func GetConfigForS3Objects(viperConfig *viper.Viper) (Objects, error) {
	sub := viperConfig.Sub("objects")
	var objects Objects
	for key, value := range sparqlTemplate {
		sub.SetDefault(key, value)
	}
	_ = sub.BindEnv("bucket", "MINIO_BUCKET")
	_ = sub.BindEnv("domain", "S3_DOMAIN")
	sub.AutomaticEnv()
	// config already read. substree passed
	err := sub.Unmarshal(&objects)
	if err != nil {
		panic(fmt.Errorf("error when parsing servers s3  config: %v", err))
	}
	return objects, err
}
