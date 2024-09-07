package config

import (
	"fmt"
	"log"

	"github.com/spf13/viper"
)

type EndPoint struct {
	Service      string
	Baseurl      string
	Type         string
	Authenticate bool
	Username     string
	Password     string
	Modes        []Mode
}

type Mode struct {
	Action string
	Suffix string
	Accept string
	Method string
}

type ServiceMode struct {
	Service      string
	URL          string // combined Baseurl + Suffix
	Type         string
	Authenticate bool
	Username     string
	Password     string
	Accept       string
	Method       string
}

func GetEndPointsConfig(v1 *viper.Viper) ([]EndPoint, error) {
	var subtreeKey = "endpoints"
	var endpointsCfg []EndPoint

	if v1 == nil {
		return nil, fmt.Errorf("GetEndPointsConfig: viperConfig is nil")
	}

	err := v1.UnmarshalKey(subtreeKey, &endpointsCfg)
	if err != nil {
		log.Fatal("error when parsing ", subtreeKey, " config: ", err)
	}

	// Log the content of the parsed configuration
	log.Printf("Config: %+v", endpointsCfg)

	return endpointsCfg, err
}

func GetEndpoint(v1 *viper.Viper, set, servertype string) (ServiceMode, error) {
	sm := ServiceMode{}
	var err error

	epcfg, err := GetEndPointsConfig(v1)
	if err != nil {
		log.Fatalf("error getting endpoint node in config: %v", err)
	}

	// If `set` is empty and there are multiple endpoints, log this and act accordingly
	if set == "" && len(epcfg) != 1 {
		log.Printf("Ambiguous service request. Multiple endpoints found, but no service specified.")
	}

	// If `set` is empty and only one endpoint exists, use that one
	if set == "" && len(epcfg) == 1 {
		set = epcfg[0].Service
		log.Printf("Defaulting to the only available service: %s", set)
	}

	log.Printf("Looking for: %s", set)

	// Loop through the endpoints and check if `set` matches `Service`
	for _, item := range epcfg {
		log.Printf("Checking service: %s", item.Service)
		if item.Service == set {
			// Loop through the modes and check if `servertype` matches `Action`
			for _, m := range item.Modes {
				log.Printf("Checking action: %s", m.Action)
				if m.Action == servertype {
					// Log Baseurl and Suffix values before constructing the URL
					log.Printf("Found matching service and action. Baseurl: %s, Suffix: %s", item.Baseurl, m.Suffix)

					// Construct URL and populate the ServiceMode struct
					sm.Service = item.Service
					sm.URL = item.Baseurl + m.Suffix
					sm.Type = item.Type
					sm.Authenticate = item.Authenticate
					sm.Username = item.Username
					sm.Password = item.Password
					sm.Accept = m.Accept
					sm.Method = m.Method

					// Log the full URL being set
					log.Printf("Constructed URL: %s", sm.URL)

					return sm, nil // return the item if found
				}
			}
		}
	}

	// If `sm.URL` is still empty, log an error
	if sm.URL == "" {
		log.Fatalf("FATAL: error getting SPARQL endpoint node from config, sm.URL is empty")
	}

	return sm, err
}
