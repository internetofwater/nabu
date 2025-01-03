package graph

import (
	"context"

	"nabu/pkg/config"

	"github.com/schollz/progressbar/v3"
	log "github.com/sirupsen/logrus"

	"github.com/minio/minio-go/v7"
	"github.com/spf13/viper"
)

func (g *GraphDbClient) ObjectAssembly(v1 *viper.Viper, mc *minio.Client) error {
	objs, err := config.GetConfigForS3Objects(v1)
	if err != nil {
		return err
	}

	var pa = objs.Prefix

	//if strings.Contains(strings.Join(pa, ","), s) {
	//	fmt.Println(s, "is in the array")
	//}

	for p := range pa {
		oa := []string{}

		// NEW
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		bucketName, _ := config.GetBucketName(v1)
		objectCh := mc.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: pa[p], Recursive: true})

		for object := range objectCh {
			if object.Err != nil {
				log.Error(object.Err)
				return object.Err
			}
			// fmt.Println(object)
			oa = append(oa, object.Key)
		}

		log.Infof("%s:%s object count: %d\n", bucketName, pa[p], len(oa))
		bar := progressbar.Default(int64(len(oa)))
		for item := range oa {

			panic("not implemented, make sure pipeload takes proper bytes")
			_, err := g.PipeLoad([]byte{}, bucketName, oa[item])
			if err != nil {
				log.Error(err)
			}
			err = bar.Add(1)
			if err != nil {
				log.Error(err)
			}
			// log.Println(string(s)) // get "s" on pipeload and send to a log file
		}
	}

	return err
}
