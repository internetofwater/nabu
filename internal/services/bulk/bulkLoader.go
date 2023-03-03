package bulk

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/gleanerio/nabu/internal/objects"
	"github.com/gleanerio/nabu/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/minio/minio-go/v7"
)

// BulkAssembly collects the objects from a bucket to load
func BulkAssembly(v1 *viper.Viper, mc *minio.Client) error {
	fmt.Println("Bulk function BulkAssembly")
	bucketName, _ := config.GetBucketName(v1)
	objCfg, _ := config.GetObjectsConfig(v1)
	pa := objCfg.Prefix

	var err error

	// Set "single file flag" to bypass skolemization, if is a single file the JSON-LD to RDF will correctly map blank nodes.
	sf := true
	if len(pa) > 1 {
		sf = false
	}

	for p := range pa {
		name := fmt.Sprintf("%s_bulk.rdf", baseName(path.Base(pa[p])))
		err = objects.PipeCopy(v1, mc, name, bucketName, pa[p], "scratch", sf) // have this function return the object name and path, easy to load and remove then
		//err = objects.MillerNG(name, bucketName, pa[p], mc) // have this function return the object name and path, easy to load and remove then
		if err != nil {
			return err
		}
	}

	for p := range pa {
		name := fmt.Sprintf("%s_bulk.rdf", baseName(path.Base(pa[p])))
		_, err := docfunc(v1, mc, bucketName, fmt.Sprintf("/scratch/%s", name))
		if err != nil {
			log.Println(err)
		}
	}

	// TODO  remove the temporary object?
	for p := range pa {
		//name := fmt.Sprintf("%s_bulk.rdf", pa[p])
		name := fmt.Sprintf("%s_bulk.rdf", baseName(path.Base(pa[p])))
		opts := minio.RemoveObjectOptions{}
		err = mc.RemoveObject(context.Background(), bucketName, fmt.Sprintf("%s/%s", pa[p], name), opts)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	return err
}

func baseName(s string) string {
	n := strings.LastIndexByte(s, '.')
	if n == -1 {
		return s
	}
	return s[:n]
}
