# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from pathlib import Path
from lib import validate_jsonld

test_data_dir = Path(__file__).parent / "testdata"

def test_valid_huc10():

    jsonldData = test_data_dir/ "huc10_items_0101000201.jsonld"

    conforms, _, text = validate_jsonld(jsonldData.read_text(), "location_oriented")

    assert conforms, f"\nSHACL Validation failed:\n{text}"

def test_valid_full_example_from_docs():
    jsonldData = test_data_dir/ "full_docs.jsonld"

    conforms, _, text = validate_jsonld(jsonldData.read_text(), "location_oriented")

    assert conforms, f"\nSHACL Validation failed:\n{text}"

def test_valid_mainstem():

    jsonldData = test_data_dir / "mainstems_1.jsonld"

    conforms, _, text = validate_jsonld(jsonldData.read_text(), "location_oriented")

    assert conforms, f"\nSHACL Validation failed:\n{text}"

def test_invalid_mainstem():

    jsonldData = test_data_dir / "invalid"/ "mainstems_1.jsonld"

    conforms, _, _ = validate_jsonld(jsonldData.read_text(), "location_oriented")

    assert not conforms, "SHACL Validation should have failed since the name was missing"

def test_containing_catchment():

    jsonldData = test_data_dir / "containing_catchment.jsonld"

    conforms, _, text = validate_jsonld(jsonldData.read_text(), "location_oriented")

    assert conforms, f"\nSHACL Validation failed:\n{text}"


def test_invalid_containing_catchment():
    jsonldData = test_data_dir / "invalid" / "containing_catchment.jsonld"

    conforms, _, text = validate_jsonld(jsonldData.read_text(), "location_oriented")

    assert not conforms, f"\nSHACL Validation failed:\n{text}"

def test_invalid_same_as():
    jsonldData = test_data_dir / "invalid" / "same_as.jsonld"

    conforms, _, text = validate_jsonld(jsonldData.read_text(), "location_oriented")

    assert not conforms, f"\nSHACL Validation failed:\n{text}"

def test_invalid_hydrolocation_type():
    jsonldData = test_data_dir / "invalid" / "bad_hydrolocation_type.jsonld"

    conforms, _, _ = validate_jsonld(jsonldData.read_text(), "location_oriented")

    assert not conforms


def test_no_place_type_specified():
    jsonldData = test_data_dir / "invalid" / "without_type_place.jsonld"

    conforms, _, text = validate_jsonld(jsonldData.read_text(), "location_oriented")

    assert not conforms
    assert "@type" in text