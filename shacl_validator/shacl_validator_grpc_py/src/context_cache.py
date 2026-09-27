# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

"""Cache remote JSON-LD contexts for the lifetime of the process.

rdflib creates a new Context for every JSON-LD parse and each Context only
caches the remote contexts it fetched itself, so a document with a remote
"@context" such as "https://schema.org/" is fetched over the network on every
parse. This replaces the function rdflib uses to fetch remote contexts with
one that caches by URL across all parses in the process.
"""

import logging
import threading

from rdflib.plugins.shared.jsonld import context as jsonld_context

LOGGER = logging.getLogger(__name__)

_original_source_to_json = jsonld_context.source_to_json
_cache: dict = {}
_lock = threading.Lock()


def _cached_source_to_json(source, *args, **kwargs):
    # only remote context urls are cached; anything else is passed through unchanged
    if not isinstance(source, str) or args or kwargs:
        return _original_source_to_json(source, *args, **kwargs)

    with _lock:
        cached = _cache.get(source)
    if cached is not None:
        return cached

    # fetched outside the lock so a slow fetch doesn't block other urls;
    # concurrent first requests for the same url may each fetch it once
    result = _original_source_to_json(source)
    with _lock:
        _cache.setdefault(source, result)
    LOGGER.info(f"Cached remote JSON-LD context {source}")
    return result


def install():
    """Make rdflib use the process-wide context cache; safe to call more than once."""
    jsonld_context.source_to_json = _cached_source_to_json


def clear():
    """Remove all cached contexts."""
    with _lock:
        _cache.clear()
