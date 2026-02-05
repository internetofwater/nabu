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
	"io"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/moby/moby/pkg/stdcopy"
	log "github.com/sirupsen/logrus"

	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/pkg"
	"golang.org/x/sync/errgroup"
)

func (s *Sitemap) HarvestBulkSitemap(ctx context.Context, config *SitemapHarvestConfig) (pkg.SitemapCrawlStats, []string, error) {

	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_bulk_harvest_%s", s.sitemapId))
	defer span.End()

	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(config.workers)

	dockerClient, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	if err != nil {
		return pkg.SitemapCrawlStats{}, []string{}, err
	}
	defer func() { _ = dockerClient.Close() }()

	for _, url := range s.URL {
		group.Go(func() error {

			docker_image_name := url.Loc

			if strings.Contains(docker_image_name, "/") {

				reader, err := dockerClient.ImagePull(ctx, docker_image_name, image.PullOptions{})
				if err != nil {
					return err
				}
				defer func() { _ = reader.Close() }()

				// read the output to completion to ensure the image is pulled
				_, err = io.ReadAll(reader)
				if err != nil {
					return err
				}
			}
			resp, err := dockerClient.ContainerCreate(
				ctx,
				&container.Config{Image: docker_image_name},
				nil, nil, nil,
				"", // don't reuse image name as container name
			)
			if err != nil {
				return err
			}

			if err = dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
				return err
			}

			waitResponseChan, errChan := dockerClient.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)

			logReader, err := dockerClient.ContainerLogs(ctx, resp.ID, container.LogsOptions{
				ShowStdout: true,
				ShowStderr: false,
				Follow:     true,
			})
			if err != nil {
				return err
			}
			defer func() { _ = logReader.Close() }()

			piperReader, piperWriter := io.Pipe()

			// demux Docker stream → pipe (runs concurrently)
			go func() {
				_, err = stdcopy.StdCopy(piperWriter, io.Discard, logReader)
				if err != nil {
					log.Errorf("error demuxing docker logs: %v", err)
				}
				_ = piperWriter.Close()
			}()

			scanner := bufio.NewScanner(piperReader)

			scanner.Buffer(make([]byte, 1024*64), 1024*1024*10) // 10 MB lines

			for scanner.Scan() {
				line := scanner.Bytes()
				if len(bytes.TrimSpace(line)) == 0 {
					continue
				}

				var jsonObj map[string]any
				if err := json.Unmarshal(line, &jsonObj); err != nil {
					return fmt.Errorf("error unmarshaling line from container logs: %w", err)
				}

				idStr, ok := jsonObj["@id"].(string)
				if !ok {
					return fmt.Errorf("missing or invalid @id in JSON-LD: %s", string(line))
				}

				encodedId := base64.StdEncoding.EncodeToString([]byte(idStr))

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
					return fmt.Errorf("container exited with status %d", resp.StatusCode)
				}
			}

			return nil
		})
	}

	return pkg.SitemapCrawlStats{}, []string{}, group.Wait()
}
