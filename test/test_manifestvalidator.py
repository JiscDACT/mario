import json
import os
import tempfile

from mario.metadata import Metadata, Item
from mario.validation import ManifestValidator


def get_validator():
    metadata = Metadata(name="Test Metadata")
    sex = Item()
    sex.name = "Sex"
    sex.set_property(
        "domain",
        ["Female", "Male", "Unknown"]
    )
    sex.set_property(
        "years",
        "(2021/22 and prior)"
    )
    metadata.add_item(sex)
    region = Item()
    region.name = "Region"
    region.set_property(
        "domain",
        ["London", "North East"]
    )
    region.set_property(
        "years",
        "(2018/19 onwards)")
    metadata.add_item(region)
    return ManifestValidator(metadata)

def get_valid_manifest():

    return {
        "requested_by": "Test User",
        "prepared_by": "Test Analyst",
        "datasource": "Students",
        "order_reference": 123,
        "customer_reference": "ABC",
        "format": "csv",
        "access_until": "2027-01-01",
        "name": "Test Dataset",
        "scope": "student",
        "context": "superset",
        "dimensions": ["Sex"],
        "measures": [
            {
                "name": "Student Count",
                "aggregation": "sum"
            }
        ],
        "years": ["2020/21"],
        "constraints": [
            {
                "item": "Sex",
                "allowedValues": ["Female"]
            }
        ]
    }

def create_manifest_file(manifest):
    temp_file = tempfile.NamedTemporaryFile(
        delete=False,
        suffix=".json",
        mode="w",
        encoding="utf-8"
    )
    json.dump(manifest, temp_file)
    temp_file.close()
    return temp_file.name

def test_valid_manifest():
    validator = get_validator()
    manifest_path = create_manifest_file(
        get_valid_manifest()
    )
    errors, warnings = validator.validate(manifest_path)
    assert errors == []
    assert warnings == []
    os.remove(manifest_path)

def test_missing_required_property():
    validator = get_validator()
    manifest = get_valid_manifest()
    del manifest["requested_by"]
    manifest_path = create_manifest_file(manifest)
    errors, warnings = validator.validate(manifest_path)
    assert "Missing required property 'requested_by'" in errors
    os.remove(manifest_path)

def test_no_dimensions():
    validator = get_validator()
    manifest = get_valid_manifest()
    manifest["dimensions"] = []
    manifest_path = create_manifest_file(manifest)
    errors, warnings = validator.validate(manifest_path)
    assert "No dimensions specified" in errors
    os.remove(manifest_path)

def test_no_measures():
    validator = get_validator()
    manifest = get_valid_manifest()
    manifest["measures"] = []
    manifest_path = create_manifest_file(manifest)
    errors, warnings = validator.validate(manifest_path)
    assert "No measures specified" in errors
    os.remove(manifest_path)

def test_measure_missing_aggregation():
    validator = get_validator()
    manifest = get_valid_manifest()
    manifest["measures"] = [
        {
            "name": "Student Count"
        }
    ]
    manifest_path = create_manifest_file(manifest)
    errors, warnings = validator.validate(manifest_path)
    assert "Measure at index 0 is missing 'aggregation'" in errors
    os.remove(manifest_path)

def test_constraint_missing_allowed_values():
    validator = get_validator()
    manifest = get_valid_manifest()
    manifest["constraints"] = [
        {
            "item": "Sex"
        }
    ]
    manifest_path = create_manifest_file(manifest)
    errors, warnings = validator.validate(manifest_path)
    assert (
        "Constraint at index 0 is missing 'allowedValues'"
        in errors
    )
    os.remove(manifest_path)

def test_constraint_domain_validation():
    validator = get_validator()
    manifest = get_valid_manifest()
    manifest["constraints"] = [
        {
            "item": "Sex",
            "allowedValues": ["Other"]
        }
    ]
    manifest_path = create_manifest_file(manifest)
    errors, warnings = validator.validate(manifest_path)
    assert "'Other' is not in domain of 'Sex'" in errors
    os.remove(manifest_path)

def test_valid_format():
    validator = get_validator()
    manifest = get_valid_manifest()
    manifest["format"] = "csv"
    manifest_path = create_manifest_file(manifest)
    errors, warnings = validator.validate(manifest_path)
    assert errors == []
    os.remove(manifest_path)

def test_invalid_format_list():
    validator = get_validator()
    manifest = get_valid_manifest()
    manifest["format"] = ["banana"]
    manifest_path = create_manifest_file(manifest)
    errors, warnings = validator.validate(manifest_path)
    assert "Unsupported format 'banana'" in errors
    os.remove(manifest_path)

def test_constraint_unknown_metadata_item():
    validator = get_validator()
    manifest = get_valid_manifest()
    manifest["constraints"] = [
        {
            "item": "Unknown Field",
            "allowedValues": ["Test"]
        }
    ]
    manifest_path = create_manifest_file(manifest)
    errors, warnings = validator.validate(manifest_path)
    assert ("Constraint item 'Unknown Field' does not exist in metadata" in errors)
    os.remove(manifest_path)

def test_year_scope_validation_prior():
    validator = get_validator()
    manifest = get_valid_manifest()
    manifest["years"] = ["2024/25"]
    manifest_path = create_manifest_file(manifest)
    errors, warnings = validator.validate(manifest_path)
    assert len(errors) == 0
    assert (
        "'Sex' is not available for requested year(s): "
        "2024/25. Valid scope is '(2021/22 and prior)'"
        in warnings)
    os.remove(manifest_path)

def test_year_scope_validation_onwards():
    validator = get_validator()
    manifest = get_valid_manifest()
    manifest["dimensions"] = ["Region"]
    manifest["years"] = ["2017/18"]
    manifest_path = create_manifest_file(manifest)
    errors, warnings = validator.validate(manifest_path)
    assert (
        "'Region' is not available for requested year(s): "
        "2017/18. Valid scope is '(2018/19 onwards)'"
        in warnings)
    os.remove(manifest_path)

def test_year_scope_validation_valid():
    validator = get_validator()
    manifest = get_valid_manifest()
    manifest["dimensions"] = ["Region"]
    manifest["years"] = ["2024/25"]
    manifest_path = create_manifest_file(manifest)
    errors, warnings = validator.validate(manifest_path)
    assert errors == []
    assert warnings == []
    os.remove(manifest_path)