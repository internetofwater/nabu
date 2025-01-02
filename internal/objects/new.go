package objects

import (
	"errors"
	"fmt"

	"nabu/pkg/config"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// MinioConnection Set up minio and initialize client
func NewMinioClientWrapper(v1 *viper.Viper) (*MinioClientWrapper, error) {
	//mcfg := v1.GetStringMapString("minio")

	mcfg, err := config.GetMinioConfig(v1)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	//endpoint := fmt.Sprintf("%s:%s", mcfg["address"], mcfg["port"])
	//accessKeyID := mcfg["accesskey"]
	//secretAccessKey := mcfg["secretkey"]
	//useSSL, err := strconv.ParseBool(fmt.Sprintf("%s", mcfg["useSSL"]))
	//endpoint := fmt.Sprintf("%s:%d", mcfg.Address, mcfg.Port)
	//accessKeyID := mcfg.Accesskey
	//secretAccessKey := mcfg.Secretkey
	//useSSL := mcfg.Ssl

	// minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, true)

	//minioClient, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKeyID, secretAccessKey, ""), Secure: useSSL})
	//if err != nil {
	//	err = errors.New(err.Error() + fmt.Sprintf("connection info: endpoint: %v SSL: %v ", endpoint, useSSL))
	//
	//	log.Fatalln(err)
	//	return nil, err
	//}

	// minioClient.SetCustomTransport(&http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}})
	// if err != nil {
	// 	log.Fatalln(err)
	// }

	var endpoint, accessKeyID, secretAccessKey string
	var useSSL bool

	if mcfg.Port == 0 {
		endpoint = mcfg.Address
		accessKeyID = mcfg.Accesskey
		secretAccessKey = mcfg.Secretkey
		useSSL = mcfg.Ssl
	} else {
		endpoint = fmt.Sprintf("%s:%d", mcfg.Address, mcfg.Port)
		accessKeyID = mcfg.Accesskey
		secretAccessKey = mcfg.Secretkey
		useSSL = mcfg.Ssl
	}

	var minioClient *minio.Client

	if mcfg.Region == "" {
		log.Println("info: no region set")
		minioClient, err = minio.New(endpoint,
			&minio.Options{Creds: credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
				Secure: useSSL,
			})
	} else {
		log.Println("info: region set for GCS or AWS, may cause issues with minio")
		region := mcfg.Region
		minioClient, err = minio.New(endpoint,
			&minio.Options{Creds: credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
				Secure: useSSL,
				Region: region,
			})
	}

	minioClient.IsOnline()
	if err != nil {
		err = errors.New(err.Error() + fmt.Sprintf("connection info: endpoint: %v SSL: %v ", endpoint, useSSL))

		log.Fatalln(err)
		return nil, err
	}
	return &MinioClientWrapper{Client: minioClient}, err
}
