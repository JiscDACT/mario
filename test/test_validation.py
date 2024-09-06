import os

import pytest

from mario.data_extractor import DataExtractor, Configuration
from mario.dataset_specification import dataset_from_json
from mario.metadata import metadata_from_json, Item
from mario.validation import DataFrameValidator, HyperValidator


def test_no_nulls():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    # postal code has NULLs
    dataset.dimensions.remove('Postal Code')
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    hyper_path = os.path.join('test', 'orders.hyper')
    configuration = Configuration(
        file_path=hyper_path
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )

    validator = DataFrameValidator(
        dataset_specification=dataset,
        metadata=metadata,
        data=extractor.get_data_frame()
    )

    assert validator.validate_data(allow_nulls=False)
    assert len(validator.errors) == 0

    hyper_validator = HyperValidator(
        dataset_specification=dataset,
        metadata=metadata,
        hyper_file_path=hyper_path
    )

    assert hyper_validator.validate_data(allow_nulls=False)
    assert len(hyper_validator.errors) == 0


def test_nulls():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    # postal code has NULLs
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    hyper_path = os.path.join('test', 'orders.hyper')
    configuration = Configuration(
        file_path=hyper_path
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )

    validator = DataFrameValidator(
        dataset_specification=dataset,
        metadata=metadata,
        data=extractor.get_data_frame()
    )
    validator.validate_data(allow_nulls=True)
    assert len(validator.errors) == 0
    assert "Validation warning: 'Postal Code' contains NULLs" in validator.warnings

    with pytest.raises(ValueError):
        validator.validate_data(allow_nulls=False)
    assert len(validator.errors) == 1
    assert "Validation error: 'Postal Code' contains NULLs" in validator.errors


def test_nulls_hyper():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    hyper_path = os.path.join('test', 'orders.hyper')

    validator = HyperValidator(
        dataset_specification=dataset,
        metadata=metadata,
        hyper_file_path=hyper_path
    )
    validator.validate_data(allow_nulls=True)
    assert len(validator.errors) == 0
    assert "Validation warning: 'Postal Code' contains NULLs" in validator.warnings

    with pytest.raises(ValueError):
        validator.validate_data(allow_nulls=False)
    assert len(validator.errors) == 1
    assert "Validation error: 'Postal Code' contains NULLs" in validator.errors


def test_missing_column():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    dataset.dimensions.append('bigliness')
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    item = Item()
    item.name='bigliness'
    metadata.add_item(item)
    hyper_path = os.path.join('test', 'orders.hyper')

    configuration = Configuration(
        file_path=hyper_path
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )

    validator = DataFrameValidator(
        dataset_specification=dataset,
        metadata=metadata,
        data=extractor.get_data_frame(minimise=False)
    )

    with pytest.raises(ValueError):
        validator.validate_data(allow_nulls=True)
    assert len(validator.errors) == 1
    assert validator.errors[0] == "Validation error: 'bigliness' in specification is missing from dataset"


def test_missing_column_hyper():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    dataset.dimensions.append('bigliness')
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    item = Item()
    item.name='bigliness'
    metadata.add_item(item)
    hyper_path = os.path.join('test', 'orders.hyper')

    hyper_validator = HyperValidator(
        dataset_specification=dataset,
        metadata=metadata,
        hyper_file_path=hyper_path
    )
    with pytest.raises(ValueError):
        hyper_validator.validate_data(allow_nulls=True)
    assert len(hyper_validator.errors) == 1
    assert hyper_validator.errors[0] == "Validation error: 'bigliness' in specification is missing from dataset"

# TODO
# Range checks

def test_domain_check():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    item = metadata.get_metadata('Ship Mode')
    item.set_property('domain', ["First Class", "Second Class", "Standard Class"])
    hyper_path = os.path.join('test', 'orders.hyper')

    configuration = Configuration(
        file_path=hyper_path
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )

    validator = DataFrameValidator(
        dataset_specification=dataset,
        metadata=metadata,
        data=extractor.get_data_frame(minimise=False)
    )

    with pytest.raises(ValueError):
        validator.validate_data(allow_nulls=True)
    assert len(validator.errors) == 1
    assert validator.errors[0] == "Validation error: 'Same Day' is not in domain of 'Ship Mode'"


def test_domain_check_hyper():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    item = metadata.get_metadata('Ship Mode')
    item.set_property('domain', ["First Class", "Second Class", "Standard Class"])
    hyper_path = os.path.join('test', 'orders.hyper')

    validator = HyperValidator(
        dataset_specification=dataset,
        metadata=metadata,
        hyper_file_path=hyper_path
    )

    with pytest.raises(ValueError):
        validator.validate_data(allow_nulls=True)
    assert len(validator.errors) == 1
    assert validator.errors[0] == "Validation error: 'Same Day' is not in domain of 'Ship Mode'"


def test_range_check():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    item = metadata.get_metadata('Discount')
    item.set_property('range', [0.0, 0.2])
    hyper_path = os.path.join('test', 'orders.hyper')

    configuration = Configuration(
        file_path=hyper_path
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )

    validator = DataFrameValidator(
        dataset_specification=dataset,
        metadata=metadata,
        data=extractor.get_data_frame(minimise=False)
    )

    with pytest.raises(ValueError):
        validator.validate_data(allow_nulls=True)
    assert len(validator.errors) == 1
    assert validator.errors[0] == "Validation error: 'Discount': '0.8' is greater than '0.2'"


def test_range_check_hyper():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    item = metadata.get_metadata('Discount')
    item.set_property('range', [0.0, 0.2])
    hyper_path = os.path.join('test', 'orders.hyper')

    validator = HyperValidator(
        dataset_specification=dataset,
        metadata=metadata,
        hyper_file_path=hyper_path
    )

    with pytest.raises(ValueError):
        validator.validate_data(allow_nulls=True)
    assert len(validator.errors) == 1
    assert validator.errors[0] == "Validation error: 'Discount': '0.8' is greater than '0.2'"


def test_multiple_errors_hyper():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    item = metadata.get_metadata('Discount')
    item.set_property('range', [0.0, 0.2])
    item = metadata.get_metadata('Ship Mode')
    item.set_property('domain', ["First Class", "Second Class", "Standard Class"])
    hyper_path = os.path.join('test', 'orders.hyper')

    validator = HyperValidator(
        dataset_specification=dataset,
        metadata=metadata,
        hyper_file_path=hyper_path
    )

    with pytest.raises(ValueError):
        validator.validate_data(allow_nulls=False)
    assert len(validator.errors) == 3
    assert "Validation error: 'Discount': '0.8' is greater than '0.2'" in validator.errors
    assert "Validation error: 'Same Day' is not in domain of 'Ship Mode'" in validator.errors
    assert "Validation error: 'Postal Code' contains NULLs" in validator.errors


def test_pattern_validation_passes():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    dataset.dimensions.append('Order Identifier')
    file_path = os.path.join('test', 'orders.csv')

    configuration = Configuration(
        file_path=file_path
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )

    validator = DataFrameValidator(
        dataset_specification=dataset,
        metadata=metadata,
        data=extractor.get_data_frame(minimise=False)
    )

    validator.validate_data(allow_nulls=True)
    assert len(validator.errors) == 0


def test_pattern_validation_fails():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    dataset.dimensions.append('Order Identifier')
    item = metadata.get_metadata('Order Identifier')
    item.set_property('pattern', 'US-20\d\d-\d{6}')
    file_path = os.path.join('test', 'orders.csv')

    configuration = Configuration(
        file_path=file_path
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )

    validator = DataFrameValidator(
        dataset_specification=dataset,
        metadata=metadata,
        data=extractor.get_data_frame(minimise=False)
    )

    with pytest.raises(ValueError):
        validator.validate_data(allow_nulls=True)
    assert len(validator.errors) == 102
    assert "Validation error: 'Order Identifier': 'CA-2019-115238' does not match the pattern 'US-20\\d\\d-\\d{6}'" in validator.errors

