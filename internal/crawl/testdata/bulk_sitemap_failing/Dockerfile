# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

FROM busybox

#####
# This container represents a bulk sitemap docker container that crashes;
# it writes the reason it failed to stderr and exits with a non zero status
CMD ["sh", "-c", "echo 'Traceback: failed to download the source data' >&2; exit 1"]
