// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/moby/moby/pkg/stdcopy"

	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/pkg"
	"golang.org/x/sync/errgroup"
)

func (s *Sitemap) HarvestBulkSitemap(ctx context.Context, config *SitemapHarvestConfig) (pkg.SitemapCrawlStats, []string, error) {

	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_bulk_harvest_%s", s.sitemapId))
	defer span.End()

	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(config.workers)

	dockerClient, err := client.NewClientWithOpts()
	if err != nil {
		return pkg.SitemapCrawlStats{}, []string{}, err
	}
	defer func() { _ = dockerClient.Close() }()

	for _, url := range s.URL {
		group.Go(func() error {

			// reader, err := dockerClient.ImagePull(ctx, url.Loc, image.PullOptions{})
			// if err != nil {
			// 	return err
			// }
			// defer func() { _ = reader.Close() }()

			// read the output to completion to ensure the image is pulled
			// _, err = io.ReadAll(reader)
			if err != nil {
				return err
			}
			resp, err := dockerClient.ContainerCreate(
				ctx,
				&container.Config{
					Image: url.Loc,
				},
				nil,
				nil,
				nil,
				url.Loc, // optional container name
			)
			if err != nil {
				return err
			}

			if err = dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
				return err
			}

			waitResponseChan, errChan := dockerClient.ContainerWait(ctx, url.Loc, container.WaitConditionNotRunning)

			logReader, err := dockerClient.ContainerLogs(ctx, url.Loc, container.LogsOptions{
				ShowStdout: true,
				ShowStderr: false,
				Follow:     true,
			})
			if err != nil {
				return err
			}

			// create buffers for stdout/stderr
			var stdoutBuf, stderrBuf bytes.Buffer

			// demultiplex the stream
			_, err = stdcopy.StdCopy(&stdoutBuf, &stderrBuf, logReader)
			if err != nil {
				return fmt.Errorf("demuxing container logs: %w", err)
			}

			scanner := bufio.NewScanner(logReader)

			for scanner.Scan() {
				line := scanner.Bytes()
				if len(bytes.TrimSpace(line)) == 0 {
					continue
				}

				// serialize the line as json
				var jsonObj map[string]any
				if err := json.Unmarshal(line, &jsonObj); err != nil {
					return fmt.Errorf("error unmarshaling line from container logs: %w", err)
				}

				id := jsonObj["@id"]
				if id == nil {
					return fmt.Errorf("missing @id field in JSON-LD document: %s", string(line))
				}

				idStr, ok := id.(string)
				if !ok {
					return fmt.Errorf("invalid @id field type in JSON-LD document: %T", id)
				}

				encodedId := base64.StdEncoding.EncodeToString([]byte(idStr))

				// each line is one JSON-LD document
				if err := config.storageDestination.StoreWithHash(
					"summoned/"+encodedId+".jsonld",
					bytes.NewReader(line),
					len(line),
				); err != nil {
					return err
				}
			}

			if err := scanner.Err(); err != nil {
				return err
			}

			select {
			case err := <-errChan:
				if err != nil {
					return err
				}
			case resp := <-waitResponseChan:
				if resp.StatusCode != 0 {
					return fmt.Errorf("container %s exited with status code %d", url.Loc, resp.StatusCode)
				} else if resp.Error != nil {
					return fmt.Errorf("container %s exited with error: %s", url.Loc, resp.Error.Message)
				}
			}

			return nil
		})
	}

	return pkg.SitemapCrawlStats{}, []string{}, group.Wait()
}
