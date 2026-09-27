# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from pathlib import Path

from rdflib import Graph
from lib import validate_jsonld

test_data_dir = Path(__file__).parent.parent.parent / "testdata"

shacl_graph = Graph().parse( test_data_dir.parent / "shapes" / "geoconnex.ttl")

def test_all_valid_cases():

    valid_dir = test_data_dir / "valid"
    for file in valid_dir.iterdir():
        jsonld = file.read_text()
        conforms, _, text = validate_jsonld(jsonld, shacl_graph)
        assert conforms, f"SHACL Validation failed for {file.name}: \n{text}"
   

def test_all_invalid_cases():

    invalid_dir = test_data_dir / "invalid"
    for file in invalid_dir.iterdir():
        jsonld = file.read_text()
        conforms, _, text = validate_jsonld(jsonld, shacl_graph)
        assert not conforms, f"SHACL Validation unexpectedly passed for {file.name}: \n{text}"
    


def test_remote_contexts_are_fetched_once(monkeypatch):
    import context_cache

    fetched = []
    remote_context = {"@context": {"@vocab": "https://schema.org/"}}

    def fake_source_to_json(source, *args, **kwargs):
        fetched.append(source)
        return remote_context, None

    context_cache.clear()
    monkeypatch.setattr(context_cache, "_original_source_to_json", fake_source_to_json)
    context_cache.install()

    jsonld = '{"@context": "https://example.com/context.jsonld", "@id": "https://example.com/place", "@type": "Place", "name": "a place"}'
    first = Graph().parse(data=jsonld, format="json-ld")
    second = Graph().parse(data=jsonld, format="json-ld")

    assert fetched == ["https://example.com/context.jsonld"]
    assert set(first) == set(second)
    assert len(first) == 2
    assert remote_context == {"@context": {"@vocab": "https://schema.org/"}}, "parsing should not mutate the cached context"
    context_cache.clear()
