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
	"github.com/internetofwater/nabu/internal/synchronizer"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"
	"github.com/internetofwater/nabu/pkg"

	"github.com/alexflint/go-arg"
	log "github.com/sirupsen/logrus"
	otelTrace "go.opentelemetry.io/otel/trace"
)

type ObjectCmd struct {
	Object string `arg:"positional"`
}
type UploadCmd struct{}
type SyncCmd struct{}
type TestCmd struct{}
type ReleaseCmd struct{}
type ClearCmd struct{}
type ConcatCmd struct {
	OutputFile string `arg:"positional"`
}

type NabuArgs struct {
	// Subcommands that can be run
	Clear   *ClearCmd   `arg:"subcommand:clear" help:"clear all graphs from the triplestore"`
	Object  *ObjectCmd  `arg:"subcommand:object" help:"upload a single object to the triplestore"`
	Release *ReleaseCmd `arg:"subcommand:release" help:"generate an nq release graph for all objects under a specific prefix"`
	Upload  *UploadCmd  `arg:"subcommand:upload" help:"upload all objects under a specific prefix to the triplestore"`
	Sync    *SyncCmd    `arg:"subcommand:sync" help:"sync the triplestore with the s3 bucket"`
	Test    *TestCmd    `arg:"subcommand:test" help:"test the connection to the s3 bucket"`
	Harvest *HarvestCmd `arg:"subcommand:harvest" help:"harvest sitemaps and store them in the s3 bucket"`
	Concat  *ConcatCmd  `arg:"subcommand:concat" help:"merge all graphs under a prefix into a single graph"`

	// Flags that can be set for config particular services / operations
	config.MinioConfig
	config.SparqlConfig
	config.ContextConfig

	// Flags that can be set which affect all operations
	LogLevel          string            `arg:"--log-level" default:"INFO"`
	Trace             bool              `arg:"--trace" help:"enable runtime profiling and tracing for performance analysis"`
	Prefix            string            `arg:"--prefix" help:"prefix in S3 to sync or upload against"`
	PrefixToFileCache map[string]string `arg:"--prefixes-to-file" help:"prefix name to file mapping; used for caching"`
	UseOtel           bool              `arg:"--use-otel"`
	OtelEndpoint      string            `arg:"--otel-endpoint" help:"OpenTelemetry endpoint"`
}

// ToStructuredConfig converts the args to a structured config
// that can be used for more config isolation
func (n NabuArgs) ToStructuredConfig() config.NabuConfig {
	return config.NabuConfig{
		Minio:             n.MinioConfig,
		Sparql:            n.SparqlConfig,
		Context:           n.ContextConfig,
		PrefixToFileCache: n.PrefixToFileCache,
		Prefix:            n.Prefix,
	}
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
			err := uploadTracefile(n.args.MinioConfig)
			if err != nil {
				log.Errorf("error uploading trace file: %v", err)
			}
		}()
	}

	cfgStruct := n.args.ToStructuredConfig()
	synchronizerClient, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return nil, err
	}

	switch {
	case n.args.Clear != nil:
		return nil, synchronizerClient.GraphClient.ClearAllGraphs()
	case n.args.Object != nil:
		return nil, synchronizerClient.UploadNqFileToTriplestore(n.args.Object.Object)
	case n.args.Release != nil:
		return nil, synchronizerClient.GenerateNqRelease(cfgStruct.Prefix)
	case n.args.Upload != nil:
		return nil, synchronizerClient.SyncTriplestoreGraphs(ctx, cfgStruct.Prefix, false)
	case n.args.Sync != nil:
		return nil, synchronizerClient.SyncTriplestoreGraphs(ctx, cfgStruct.Prefix, true)
	case n.args.Test != nil:
		return nil, Test(ctx, synchronizerClient)
	case n.args.Harvest != nil:
		return Harvest(ctx, cfgStruct.Minio, *n.args.Harvest)
	case n.args.Concat != nil:
		return nil, synchronizerClient.S3Client.ConcatToDisk(ctx, cfgStruct.Prefix, n.args.Concat.OutputFile)
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
