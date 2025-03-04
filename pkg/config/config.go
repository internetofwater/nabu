package config

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type NabuConfig struct {
	Minio       MinioConfig
	Sparql      SparqlConfig
	Context     ContextConfig
	ContextMaps []ContextMap
	Prefixes    []string
	Trace       bool   `optional:"true"`
	LogLevel    string `optional:"true"`
}

type SparqlConfig struct {
	Endpoint     string
	Authenticate bool
	Username     string
	Password     string
	// the name of the repository in graphdb
	Repository string
	// the number of statements to send in one batch
	// when upserting triples
	Batch int `optional:"true"`
}

type MinioConfig struct {
	Address   string
	Port      int
	Ssl       bool
	Accesskey string
	Secretkey string
	Bucket    string
	Region    string
}

type ContextConfig struct {
	// whether or not to cache the context when
	// decoding json-ld
	Cache  bool
	Strict bool
}

type ContextMap struct {
	Prefix string
	File   string
}

func fileNameWithoutExtTrimSuffix(fileName string) string {
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}

// ensures all struct fields are present in the YAML config and errors if any are missing
func checkMissingFields(v *viper.Viper, structType reflect.Type, parentKey string) error {
	var missingFields []string

	for i := range structType.NumField() {
		field := structType.Field(i)
		fieldName := field.Tag.Get("mapstructure")
		if fieldName == "" {
			fieldName = strings.ToLower(field.Name) // Default to lowercase field name
		}

		optional := field.Tag.Get("optional") // Skip checking optional fields
		if optional == "true" {
			continue
		}

		fullKey := fieldName
		if parentKey != "" {
			fullKey = parentKey + "." + fieldName
		}

		if field.Type.Kind() == reflect.Struct {
			// Recursively check nested structs
			if err := checkMissingFields(v, field.Type, fullKey); err != nil {
				missingFields = append(missingFields, err.Error())
			}
		} else if !v.IsSet(fullKey) {
			missingFields = append(missingFields, fullKey)
		}
	}

	if len(missingFields) > 0 {
		return fmt.Errorf("missing required fields: %v", strings.Join(missingFields, ", "))
	}

	return nil
}

// Return a nabu config struct from a viper instance
// Validates the config
func NewNabuConfigFromViper(v *viper.Viper) (NabuConfig, error) {
	// Check for missing required fields before unmarshaling
	if err := checkMissingFields(v, reflect.TypeOf(NabuConfig{}), ""); err != nil {
		return NabuConfig{}, err
	}

	var config NabuConfig
	if err := v.UnmarshalExact(&config); err != nil {
		var tmpConfig NabuConfig
		if debugErr := v.Unmarshal(&tmpConfig); debugErr != nil {
			log.Error("failed to unmarshal config for debugging purposes", debugErr)
		} else {
			log.Infof("Got the following config when searching for missing fields: %#v", tmpConfig)
		}

		return NabuConfig{}, fmt.Errorf("failed to unmarshal config exactly with the following error: %w", err)
	}

	return config, nil
}

// Return a nabu config struct from a config file
func NewNabuConfig(cfgPath, filename string) (NabuConfig, error) {
	v := viper.New()

	v.SetConfigName(fileNameWithoutExtTrimSuffix(filename))
	v.AddConfigPath(cfgPath)
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		return NabuConfig{}, err
	}
	return NewNabuConfigFromViper(v)
}
