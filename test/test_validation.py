import os

import pytest

from mario.data_extractor import DataExtractor, Configuration
from mario.dataset_specification import dataset_from_json
from mario.metadata import metadata_from_json, Item
from mario.validation import DataFrameValidator, HyperValidator, Validator, SqlValidator


def get_validator(nulls=False, hyperfile=False):
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    # postal code has NULLs
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    filename = 'orders_with_nulls.hyper' if nulls else 'orders.hyper'
    hyper_path = os.path.join('test', filename)

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
    hyper_validator = HyperValidator(
        dataset_specification=dataset,
        metadata=metadata,
        hyper_file_path=hyper_path
    )
    if hyperfile:
        return hyper_validator
    return validator


def test_no_nulls():
    validator = get_validator()
    assert validator.validate_data(allow_nulls=False)
    assert len(validator.errors) == 0


def test_no_nulls_hyper():
    validator = get_validator(hyperfile=True)
    assert validator.validate_data(allow_nulls=False)
    assert len(validator.errors) == 0


def test_nulls():
    validator = get_validator(nulls=True)
    validator.validate_data(allow_nulls=True)
    assert len(validator.errors) == 0
    assert "Validation warning: 'Postal Code' contains NULLs" in validator.warnings

    with pytest.raises(ValueError):
        validator.validate_data(allow_nulls=False)
    assert len(validator.errors) == 1
    assert "Validation error: 'Postal Code' contains NULLs" in validator.errors


def test_nulls_hyper():
    validator = get_validator(nulls=True, hyperfile=True)
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
    item.name = 'bigliness'
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
    hyper_path = os.path.join('test', 'orders_with_nulls.hyper')

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


def test_get_hierarchy_data():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
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

    dimensions = validator.__get_data_for_hierarchy__('Product')
    assert len(dimensions) == 1849
    assert len(dimensions.columns) == 2


def test_check_hierarchies():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
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
    validator.check_hierarchies()

    assert validator.errors == ["Inconsistent hierarchy: 92024 at level Postal Code is represented in multiple higher level categories ('United States', 'West', 'California', 'Encinitas') and ('United States', 'West', 'California', 'San Diego')."]


def test_check_hierarchies_hyper():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    file_path = os.path.join('test', 'orders.hyper')

    validator = HyperValidator(
        dataset_specification=dataset,
        metadata=metadata,
        hyper_file_path=file_path
    )
    validator.check_hierarchies()

    assert validator.errors == ["Inconsistent hierarchy: 92024 at level Postal Code is represented in multiple higher level categories ('United States', 'West', 'California', 'Encinitas') and ('United States', 'West', 'California', 'San Diego')."]


def test_category_anomalies():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    file_path = os.path.join('test', 'orders.csv')

    configuration = Configuration(
        file_path=file_path
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )

    # Introduce a segmentation variable
    df = extractor.get_data_frame()
    df['Year'] = df['Ship Date'].str[-4:]

    validator = DataFrameValidator(
        dataset_specification=dataset,
        metadata=metadata,
        data=extractor.get_data_frame(minimise=False)
    )
    validator.check_category_anomalies('Year')

    assert "Validation warning: 'Ship Mode' has potentially anomalous data when segmented by 'Year'" in validator.warnings


def test_category_anomalies_hyper():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    file_path = os.path.join('test', 'orders.hyper')

    configuration = Configuration(
        file_path=file_path
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )

    # Introduce a segmentation variable
    df = extractor.get_data_frame()
    df['Year'] = df['Ship Date'].astype(str).str[0:4]

    output_file_path = os.path.join('output', 'orders_with_segmentation.hyper')
    extractor.save_data_as_hyper(file_path=output_file_path, minimise=False)

    validator = HyperValidator(
        dataset_specification=dataset,
        metadata=metadata,
        hyper_file_path=output_file_path
    )
    validator.check_category_anomalies('Year')

    assert "Validation warning: 'Ship Mode' has potentially anomalous data when segmented by 'Year'" in validator.warnings


def test_all_checks():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    file_path = os.path.join('test', 'orders.csv')

    configuration = Configuration(
        file_path=file_path
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )

    # Introduce a segmentation variable
    df = extractor.get_data_frame()
    df['Year'] = df['Ship Date'].str[-4:]

    validator = DataFrameValidator(
        dataset_specification=dataset,
        metadata=metadata,
        data=extractor.get_data_frame(minimise=False)
    )
    with pytest.raises(ValueError):
        validator.validate_data(check_hierarchies=True, detect_anomalies=True, segmentation='Year')
    assert len(validator.errors) == 1
    assert len(validator.warnings) == 11


def test_all_checks_sql():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))

    configuration = Configuration(
        view='superstore',
        schema='dev',
        connection_string=os.environ.get("CONNECTION_STRING")
    )

    validator = SqlValidator(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )

    with pytest.raises(ValueError):
        validator.validate_data(check_hierarchies=True, detect_anomalies=False)

    assert len(validator.errors) == 2
    assert len(validator.warnings) == 7


def test_checks_iteratively():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
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
    for item in dataset.items:
        validator.errors = []
        validator.warnings = []
        validator.validate_data_item(item, allow_nulls=False)
        assert validator.errors == []
        if item == 'Ship Date':
            assert len(validator.warnings) == 2
        elif item in ['Country/Region', 'State/Province', 'City', 'Product Name', 'Sales', 'Profit']:
            assert len(validator.warnings) == 1
        else:
            assert len(validator.warnings) == 0


def test_check_anomalies_single_field():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    file_path = os.path.join('test', 'orders.csv')

    configuration = Configuration(
        file_path=file_path
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )

    # Introduce a segmentation variable
    df = extractor.get_data_frame()
    df['Year'] = df['Ship Date'].str[-4:]

    validator = DataFrameValidator(
        dataset_specification=dataset,
        metadata=metadata,
        data=extractor.get_data_frame(minimise=False)
    )
    validator.check_item_for_anomalies('Ship Mode', 'Year')
    assert "Validation warning: 'Ship Mode' has potentially anomalous data when segmented by 'Year'" in validator.warnings

