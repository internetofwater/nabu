package main

import (
	"fmt"
	"nabu/internal/config"
	"nabu/internal/interfaces"
	"nabu/internal/synchronizer/s3"
	"os"

	crawl "nabu/internal/crawl"

	arg "github.com/alexflint/go-arg"
	log "github.com/sirupsen/logrus"
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
	ToDisk       bool   `arg:"--to-disk" default:"false"`         // save to disk instead of minio
	LogLevel     string `arg:"--log-level" default:"INFO"`

	ConcurrentSitemaps int `arg:"--concurrent-sitemaps" default:"10"`
	SitemapWorkers     int `arg:"--sitemap-workers" default:"10"`
}

type GleanerRunner struct {
	args GleanerArgs
}

func NewGleanerRunner(cliArgs []string) GleanerRunner {
	args := GleanerArgs{}
	allArgsButBinary := cliArgs[1:]
	const dummyBinaryName = "gleaner" // we need to add some arbitrary binary name
	os.Args = append([]string{dummyBinaryName}, allArgsButBinary...)
	arg.MustParse(&args)
	return GleanerRunner{
		args: args,
	}
}

func (g GleanerRunner) Run() error {
	level, err := log.ParseLevel(g.args.LogLevel)
	if err != nil {
		return fmt.Errorf("invalid log level %s: %w", g.args.LogLevel, err)
	}
	log.SetLevel(level)
	log.SetOutput(os.Stdout)
	log.Info("Starting Gleaner")

	if g.args.SitemapIndex != "" {
		index, err := crawl.NewSitemapIndexHarvester(g.args.SitemapIndex)
		if err != nil {
			return err
		}
		var configuredSitemap crawl.Index
		if g.args.ToDisk {
			log.Info("Saving to fetched files to disk")
			tmpFSStorage, err := interfaces.NewLocalTempFSCrawlStorage()
			if err != nil {
				return err
			}
			configuredSitemap = index.WithStorageDestination(tmpFSStorage)
		} else {
			log.Infof("Saving to fetched files to s3 bucket at %s:%d", g.args.Address, g.args.Port)
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
			configuredSitemap = index.WithStorageDestination(minioS3)
		}

		configuredSitemap = configuredSitemap.WithConcurrencyConfig(g.args.ConcurrentSitemaps, g.args.SitemapWorkers)

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

func main() {
	if err := NewGleanerRunner(os.Args).Run(); err != nil {
		log.Fatalf("Failed to run: %v", err)
	}
}
