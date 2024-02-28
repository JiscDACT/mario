import os
import shutil
import tempfile

import pandas as pd
import pytest

from mario.data_extractor import DataExtractor, Configuration, StreamingDataExtractor, HyperFile
from mario.dataset_specification import dataset_from_json
from mario.metadata import metadata_from_json
from mario.query_builder import ViewBasedQueryBuilder


def test_csv_to_csv():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        file_path=os.path.join('test', 'orders.csv')
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    extractor.validate_data()
    with tempfile.NamedTemporaryFile(suffix='.csv') as file:
        extractor.save_data_as_csv(file_path=file.name)


def test_csv_to_hyper():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        file_path=os.path.join('test', 'orders.csv')
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    with tempfile.NamedTemporaryFile(suffix='.hyper') as file:
        extractor.save_data_as_hyper(file_path=file.name)


def test_hyper_to_csv():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        file_path=os.path.join('test', 'orders.hyper')
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    with pytest.raises(ValueError):
        extractor.validate_data(allow_nulls=False)
    with tempfile.NamedTemporaryFile(suffix='.csv') as file:
        extractor.save_data_as_csv(file_path=file.name)


def test_hyper_to_csv_without_nulls():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    # postal code has NULLs
    dataset.dimensions.remove('Postal Code')
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        file_path=os.path.join('test', 'orders.hyper')
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    extractor.validate_data(allow_nulls=False)
    with tempfile.NamedTemporaryFile(suffix='.csv') as file:
        extractor.save_data_as_csv(file_path=file.name)


def test_stream_sql_to_csv():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        connection_string=os.environ.get('CONNECTION_STRING'),
        schema="dev",
        view="superstore",
        query_builder=ViewBasedQueryBuilder
    )
    extractor = StreamingDataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    file = tempfile.NamedTemporaryFile(suffix='.csv')
    extractor.stream_sql_to_csv(file_path=file.name, chunk_size=1000)
    df = pd.read_csv(file)
    assert len(df) == 10194


def test_stream_sql_to_hyper():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        connection_string=os.environ.get('CONNECTION_STRING'),
        schema="dev",
        view="superstore",
        query_builder=ViewBasedQueryBuilder
    )
    extractor = StreamingDataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    folder = tempfile.TemporaryDirectory()
    file = os.path.join(folder.name, 'data.hyper')
    extractor.stream_sql_to_hyper(file_path=file, chunk_size=1000)
    import pantab
    from tableauhyperapi import TableName
    df = pantab.frame_from_hyper(source=file, table=TableName('Extract', 'Extract'))
    assert len(df) == 10194
    shutil.rmtree(folder.name)


