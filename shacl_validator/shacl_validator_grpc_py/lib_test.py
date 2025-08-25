# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from pathlib import Path
from lib import validate_jsonld

test_data_dir = Path(__file__).parent.parent / "testdata"

def test_all_valid_cases():

    valid_dir = test_data_dir / "valid"
    for file in valid_dir.iterdir():
        jsonld = file.read_text()
        conforms, _, text = validate_jsonld(jsonld, format="location_oriented")
        assert conforms, f"SHACL Validation failed for {file.name}: \n{text}"
   

def test_all_invalid_cases():

    invalid_dir = test_data_dir / "invalid"
    for file in invalid_dir.iterdir():
        jsonld = file.read_text()
        conforms, _, text = validate_jsonld(jsonld, format="location_oriented")
        assert not conforms, f"SHACL Validation unexpectedly passed for {file.name}: \n{text}"
    
