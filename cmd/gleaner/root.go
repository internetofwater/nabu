package gleaner

import (
	"nabu/internal/config"
	"nabu/internal/synchronizer/s3"
	"os"
	"strings"

	arg "github.com/alexflint/go-arg"
)

type GleanerArgs struct {
	Address      string `arg:"--address" default:"127.0.0.1"`
	Port         int    `arg:"--port" default:"7200"`
	Bucket       string `arg:"--bucket" default:"iow"` // minio bucket to put data
	SitemapIndex string `arg:"--sitemap-index"`
	Source       string `arg:"--source"` // source to crawl from the config
	Config       string `arg:"--cfg"`    // full path to config
	Mode         string `arg:"--mode"`
	SecretKey    string `arg:"--secret-key" default:"minioadmin"` // secret key for minio
	AccessKey    string `arg:"--access-key" default:"minioadmin"` // access key for minio
	SSL          bool   `arg:"--ssl"`                             // use SSL for HTTP requests
	SetupBuckets bool   `arg:"--setup-buckets"`                   // setup buckets before crawling
	Rude         bool   `arg:"--rude"`                            // ignore robots.txt

	ConcurrentSitemaps int `arg:"--concurrent-sitemaps" default:"10"`
	SitemapWorkers     int `arg:"--sitemap-workers" default:"10"`
}

type GleanerRunner struct {
	args GleanerArgs
}

func NewGleanerRunner(input string) GleanerRunner {
	args := GleanerArgs{}
	os.Args = append([]string{"gleaner"}, strings.Split(input, " ")...)
	arg.MustParse(&args)
	return GleanerRunner{
		args: args,
	}
}

func (g GleanerRunner) Run() error {

	minioS3, err := s3.NewMinioClientWrapper(config.MinioConfig{
		Address:   g.args.Address,
		Port:      g.args.Port,
		Ssl:       g.args.SSL,
		Accesskey: g.args.AccessKey,
		Secretkey: g.args.SecretKey,
		Bucket:    g.args.Bucket,
	})
	if err != nil {
		return err
	}

	if g.args.SitemapIndex != "" {
		index, err := NewSitemapIndexHarvester(g.args.SitemapIndex)
		if err != nil {
			return err
		}

		configuredSitemap := index.WithStorageDestination(minioS3).WithConcurrencyConfig(g.args.ConcurrentSitemaps, g.args.SitemapWorkers)

		if g.args.Source != "" {
			return configuredSitemap.HarvestSitemap(g.args.Source)
		} else {
			return configuredSitemap.HarvestAll()
		}
	} else if g.args.Source != "" {
		panic("not implemented")
	}

	return nil
}
