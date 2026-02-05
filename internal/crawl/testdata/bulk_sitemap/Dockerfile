# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

FROM busybox

COPY ./data.txt /data.txt

#####
# This container represents the simple possible output of a bulk sitemap docker container
# it just outputs contents of the text file to stdout and then exits. each line in stdout
# represents a separate JSON-LD document
CMD ["cat", "/data.txt"]
