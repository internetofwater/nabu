// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/trace"
	"strings"

	"github.com/internetofwater/nabu/internal/common/projectpath"
	"github.com/internetofwater/nabu/internal/config"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"
	"github.com/internetofwater/nabu/pkg"

	"github.com/alexflint/go-arg"
	log "github.com/sirupsen/logrus"
	otelTrace "go.opentelemetry.io/otel/trace"
)

type MinioOptions struct {
	Address  string `arg:"--address" help:"The address of the s3 server" default:"127.0.0.1"` // The address of the minio server
	Port     int    `arg:"--port" default:"9000"`
	Username string `arg:"--s3-access-key,env:S3_ACCESS_KEY" help:"Access Key (i.e. username)" default:"minioadmin"` // Access Key (i.e. username)
	Password string `arg:"--s3-secret-key,env:S3_SECRET_KEY" help:"Secret Key (i.e. password)" default:"minioadmin"` // Secret Key (i.e. password)
	Bucket   string `arg:"--bucket" help:"The s3 bucket to use for sync operations" default:"iow"`                   // The configuration bucket
	Region   string `arg:"--region" help:"region for the s3 server"`                                                 // region for the minio server
	SSL      bool   `arg:"--ssl" help:"Use SSL when connecting to s3"`
}

type SparqlOptions struct {
	Endpoint   string `arg:"--endpoint" help:"endpoint for server for the SPARQL endpoints" default:"http://127.0.0.1:7200"`
	Repository string `arg:"--repository" help:"the default repository to use for graphdb" default:"iow"` // the default repository to use for graphdb
}

type NabuArgs struct {
	// Subcommands that can be run
	Clear   *ClearCmd   `arg:"subcommand:clear" help:"clear all graphs from the triplestore"`              // clear all graphs from the triplestore
	Object  *ObjectCmd  `arg:"subcommand:object" help:"upload a single object to the triplestore"`         // upload a single object to the triplestore
	Release *ReleaseCmd `arg:"subcommand:release" help:"upload a release to the triplestore"`              // upload a release to the triplestore
	Prefix  *PrefixCmd  `arg:"subcommand:prefix" help:"upload a prefix to the triplestore"`                // upload a prefix to the triplestore
	Sync    *SyncCmd    `arg:"subcommand:sync" help:"sync the triplestore with the s3 bucket"`             // sync the triplestore with the s3 bucket
	Test    *TestCmd    `arg:"subcommand:test" help:"test the connection to the s3 bucket"`                // test the connection to the s3 bucket
	Harvest *HarvestCmd `arg:"subcommand:harvest" help:"harvest sitemaps and store them in the s3 bucket"` // harvest sitemaps and store them in the s3 bucket

	MinioOptions
	SparqlOptions

	Cfg string `arg:"--cfg" help:"full path to yaml config file for nabu"` // full path to yaml config file for nabu

	LogLevel        string            `arg:"--log-level" default:"INFO"`         // the log level to use for the nabu logger
	Trace           bool              `arg:"--trace"`                            // Enable tracing
	Dangerous       bool              `arg:"--dangerous"`                        // Use dangerous mode boolean
	UpsertBatchSize int               `arg:"--upsert-batch-size" default:"1"`    // Port for s3 server
	Prefixes        []string          `arg:"--prefix" help:"prefixes to upload"` // prefixes to upload
	PrefixesToFile  map[string]string `arg:"--prefixes-to-file" help:"prefixes to file mapping"`
	Cache           bool              `arg:"--cache" help:"use cache for context"`
	Strict          bool              `arg:"--strict" help:"use strict mode for context"`
	UseOtel         bool              `arg:"--use-otel"`
	OtelEndpoint    string            `arg:"--otel-endpoint" help:"OpenTelemetry endpoint"`
}

// ToStructuredConfig converts the args to a structured config
// that can be used for more config isolation
func (n NabuArgs) ToStructuredConfig() config.NabuConfig {
	return config.NabuConfig{
		Minio:       n.GetMinioConfig(),
		Sparql:      n.GetSparqlConfig(),
		Context:     n.GetContextConfig(),
		ContextMaps: n.GetContextMaps(),
		Prefixes:    n.Prefixes,
	}
}

func (n NabuArgs) GetMinioConfig() config.MinioConfig {
	return config.MinioConfig{
		Address:   n.Address,
		Port:      n.Port,
		Ssl:       n.SSL,
		Accesskey: n.Username,
		Secretkey: n.Password,
		Bucket:    n.Bucket,
		Region:    n.Region,
	}
}

func (n NabuArgs) GetSparqlConfig() config.SparqlConfig {
	return config.SparqlConfig{
		Endpoint:     n.Endpoint,
		Authenticate: n.Password != "",
		Username:     n.Username,
		Password:     n.Password,
		Repository:   n.Repository,
		Batch:        n.UpsertBatchSize,
	}
}

func (n NabuArgs) GetContextConfig() config.ContextConfig {
	return config.ContextConfig{
		Cache:  n.Cache,
		Strict: n.Strict,
	}
}

func (n NabuArgs) GetContextMaps() []config.ContextMap {
	ctxMap := []config.ContextMap{}
	for prefix, file := range n.PrefixesToFile {
		ctxMap = append(ctxMap, config.ContextMap{
			Prefix: prefix,
			File:   file,
		})
	}
	return ctxMap
}

type NabuRunner struct {
	args NabuArgs
}

func NewNabuRunner(cliArgs []string) NabuRunner {
	args := NabuArgs{}
	const dummyBinaryName = "nabu" // we need to add some arbitrary binary name before the args; it doesn't matter
	os.Args = append([]string{dummyBinaryName}, cliArgs...)

	parser := arg.MustParse(&args)
	subCmd := parser.Subcommand()
	if subCmd == nil || subCmd == "" {
		log.Error("no subcommand provided")
		parser.WriteHelp(os.Stderr)
		os.Exit(1)
	}
	return NabuRunner{
		args: args,
	}
}

func uploadTracefile(minioConfig config.MinioConfig) error {
	mc, err := s3.NewMinioClientWrapper(minioConfig)
	if err != nil {
		return err
	}
	traceFile := filepath.Join(projectpath.Root, "trace.out")
	joinedArgs := strings.Join(os.Args[1:], "_")
	// replace all special characters with underscore
	joinedArgs = strings.NewReplacer("/", "_", ".", "_", "-", "_", ":", "_").Replace(joinedArgs)
	traceName := fmt.Sprintf("traces/trace_%s.out", joinedArgs)
	log.Debugf("Uploading trace file %s", traceName)
	return mc.UploadFile(traceName, traceFile)
}

func (n NabuRunner) Run(ctx context.Context) (harvestReport pkg.SitemapIndexCrawlStats, err error) {
	defer trace.Stop()

	level, err := log.ParseLevel(n.args.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %s: %w", n.args.LogLevel, err)
	}
	log.SetLevel(level)

	if n.args.UseOtel || n.args.OtelEndpoint != "" {
		if n.args.OtelEndpoint == "" {
			n.args.OtelEndpoint = opentelemetry.DefaultTracingEndpoint
		}
		log.Infof("Starting opentelemetry traces and exporting to: %s", n.args.OtelEndpoint)
		opentelemetry.InitTracer("nabu", n.args.OtelEndpoint)
		var span otelTrace.Span
		argsAsStr := strings.Join(os.Args, "_")
		ctx, span = opentelemetry.SubSpanFromCtxWithName(ctx, argsAsStr)
		defer opentelemetry.Shutdown()
		defer span.End()
	}

	if n.args.Trace {
		filePath := filepath.Join(projectpath.Root, "trace.out")
		log.Infof("Trace enabled; Outputting to %s", filePath)
		f, err := os.Create(filePath)
		if err != nil {
			log.Fatal(err)
		}
		if err := trace.Start(f); err != nil {
			log.Fatal(err)
		}
		defer func() {
			err := uploadTracefile(n.args.GetMinioConfig())
			if err != nil {
				log.Errorf("error uploading trace file: %v", err)
			}
		}()
	}

	cfgStruct := n.args.ToStructuredConfig()

	switch {
	case n.args.Clear != nil:
		return nil, clear(cfgStruct)
	case n.args.Object != nil:
		return nil, object(n.args.Object.Object, cfgStruct)
	case n.args.Release != nil:
		return nil, release(cfgStruct)
	case n.args.Prefix != nil:
		return nil, prefix(cfgStruct)
	case n.args.Sync != nil:
		return nil, Sync(ctx, cfgStruct)
	case n.args.Test != nil:
		return nil, Test(cfgStruct)
	case n.args.Harvest != nil:
		return Harvest(ctx, cfgStruct.Minio, *n.args.Harvest)
	default:
		return nil, fmt.Errorf("unknown nabu subcommand")
	}
}

func main() {
	if crawlStats, err := NewNabuRunner(os.Args[1:]).Run(context.Background()); err != nil {
		log.Fatal(err)
	} else {
		// if there were no explicit errors, check for any crawl failures
		// if there were, exit with a non-zero exit code
		for _, sitemap := range crawlStats {
			if len(sitemap.CrawlFailures) > 0 {
				log.Warn("At least one sitemap contained errors when harvesting; check the log for details")
				// we use exit status 3 since it is not a fatal error that would exit 1
				// nor a user error that would exit 2
				const nonFatalError = 3
				log.Exit(nonFatalError)
			}
		}
	}
}
