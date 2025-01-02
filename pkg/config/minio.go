package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// frig frig... do not use lowercase... those are private variables
type Minio struct {
	Address   string // `mapstructure:"MINIO_ADDRESS"`
	Port      int    //`mapstructure:"MINIO_PORT"`
	Ssl       bool   //`mapstructure:"MINIO_USE_SSL"`
	Accesskey string //`mapstructure:"MINIO_ACCESS_KEY"`
	Secretkey string // `mapstructure:"MINIO_SECRET_KEY"`
	Bucket    string
	Region    string
}

var MinioTemplate = map[string]interface{}{
	"minio": map[string]string{
		"address":   "localhost",
		"port":      "9000",
		"accesskey": "",
		"secretkey": "",
		"bucket":    "",
		"ssl":       "false",
		"region":    "",
	},
}

func GetMinioConfig(viperConfig *viper.Viper) (Minio, error) {
	sub := viperConfig.Sub("minio")
	return ReadMinioConfig(sub)
}

// use config.Sub("minio)
func ReadMinioConfig(minioSubtress *viper.Viper) (Minio, error) {
	var minioCfg Minio
	for key, value := range MinioTemplate {
		minioSubtress.SetDefault(key, value)
	}
	_ = minioSubtress.BindEnv("address", "MINIO_ADDRESS")
	_ = minioSubtress.BindEnv("port", "MINIO_PORT")
	_ = minioSubtress.BindEnv("ssl", "MINIO_USE_SSL")
	_ = minioSubtress.BindEnv("accesskey", "MINIO_ACCESS_KEY")
	_ = minioSubtress.BindEnv("secretkey", "MINIO_SECRET_KEY")
	_ = minioSubtress.BindEnv("secretkey", "MINIO_SECRET_KEY")
	_ = minioSubtress.BindEnv("bucket", "MINIO_BUCKET")
	_ = minioSubtress.BindEnv("region", "MINIO_REGION")
	minioSubtress.AutomaticEnv()
	// config already read. substree passed
	err := minioSubtress.Unmarshal(&minioCfg)
	if err != nil {
		panic(fmt.Errorf("error when parsing minio config: %v", err))
	}
	return minioCfg, err
}

func GetBucketName(v1 *viper.Viper) (string, error) {
	minSubtree := v1.Sub("minio")
	miniocfg, err := ReadMinioConfig(minSubtree)
	if err != nil {
		panic(err)
	}
	bucketName := miniocfg.Bucket //miniocfg["bucket"] //   get the top level bucket for all of gleaner operations from config file
	return bucketName, err
}
