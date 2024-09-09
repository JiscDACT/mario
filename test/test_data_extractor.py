import os
import shutil
import tempfile
import logging

import pandas as pd
import pytest

from mario.data_extractor import DataExtractor, Configuration, StreamingDataExtractor, DataFrameExtractor
from mario.dataset_specification import dataset_from_json
from mario.metadata import metadata_from_json
from mario.query_builder import ViewBasedQueryBuilder, SubsetQueryBuilder

from test.mocks import MockQueryBuilder

logger = logging.getLogger(__name__)


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
        file.close()
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
        file.close()
        extractor.save_data_as_hyper(file_path=file.name)


def test_hyper_with_nulls_to_csv():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        file_path=os.path.join('test', 'orders_with_nulls.hyper')
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    with pytest.raises(ValueError):
        extractor.validate_data(allow_nulls=False)
    with tempfile.NamedTemporaryFile(suffix='.csv') as file:
        file.close()
        extractor.save_data_as_csv(file_path=file.name)


def test_hyper_without_nulls_to_csv():
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
    extractor.validate_data(allow_nulls=False)
    with tempfile.NamedTemporaryFile(suffix='.csv') as file:
        file.close()
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


def test_stream_sql_to_csv_with_compression():
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
    gzip_path = extractor.stream_sql_to_csv(file_path=file.name, chunk_size=1000, compress_using_gzip=True)
    df = pd.read_csv(gzip_path)
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


def test_stream_sql_to_csv_with_validation():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    # Restrict ship modes so that validation fails
    metadata.get_metadata('Ship Mode').set_property('domain', ["First Class", "Second Class"])
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
    with pytest.raises(ValueError):
        extractor.stream_sql_to_csv(file_path=file.name, validate=True, chunk_size=1000)


def test_stream_sql_to_csv_with_minimisation():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    # Remove ship mode
    dataset.dimensions.remove('Ship Mode')
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
    extractor.stream_sql_to_csv(file_path=file.name, validate=False, minimise=True, chunk_size=1000)
    df = pd.read_csv(file.name)
    assert 'Ship Mode' not in df.columns


def test_column_mapping():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    # Rename 'region' to area
    dataset.dimensions.remove('Region')
    dataset.dimensions.append('Area')
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    meta = metadata.get_metadata('Region')
    meta.name = 'Area'
    meta.set_property('output_name', 'Region')
    assert metadata.get_metadata('Area') is not None
    assert metadata.get_metadata('Region') is None
    assert 'Area' in dataset.dimensions
    assert 'Region' not in dataset.dimensions

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
    extractor.stream_sql_to_csv(
        file_path=file.name,
        validate=True,
        chunk_size=1000
    )
    df = pd.read_csv(file.name)
    assert 'Region' in df.columns


def test_stream_to_csv_using_bcp():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")
    if not os.environ.get('BCP'):
        pytest.skip('BCP not available')
    conn = os.environ.get('CONNECTION_STRING')

    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        connection_string=conn,
        schema='dbo',
        query_builder=MockQueryBuilder
    )
    extractor = StreamingDataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    file = tempfile.NamedTemporaryFile(suffix='.csv')
    file.close()  # dumb Windows hack
    extractor.stream_sql_to_csv_using_bcp(
        table_name='v_mario_test',
        output_file_path=file.name,
        database_name=os.environ.get('DATABASE'),
        use_view=True,
        server_url=os.environ.get('SERVER'),
        delete_when_finished=True
    )


def test_hyper_totals():
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
    extractor.validate_data()
    assert extractor.get_total() == 2326534.3543


def test_csv_totals():
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
    assert extractor.get_total() == 2326534.3543
    assert extractor.get_total(measure='Sales') == 2326534.3543
    assert round(extractor.get_total(measure='Profit'), 4) == 292296.8146


def test_csv_total_profit():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    dataset.measures = ['Profit']
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
    assert round(extractor.get_total(), 4) == 292296.8146


def test_csv_total_no_measures():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    dataset.measures = []
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
    assert extractor.get_total() == 10194
    with pytest.raises(ValueError):
        extractor.get_total(measure='Sales')


def test_stream_sql_subset_to_csv_with_total():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    dataset.measures = ['Sales', 'Profit']
    dataset.dimensions = ['Product Name']
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        connection_string=os.environ.get('CONNECTION_STRING'),
        schema="dev",
        view="superstore",
        query_builder=SubsetQueryBuilder
    )
    extractor = StreamingDataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    file = tempfile.NamedTemporaryFile(suffix='.csv')
    extractor.stream_sql_to_csv(file_path=file.name, chunk_size=1000)
    df = pd.read_csv(file)
    assert len(df) == 1849
    assert round(extractor.get_total(), 4) == 2326534.3543
    assert round(extractor.get_total(measure='Sales'), 4) == 2326534.3543
    assert round(extractor.get_total(measure='Profit'), 4) == 292296.8146


def test_stream_sql_view_to_csv_with_total():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    dataset.measures = ['Sales']
    dataset.dimensions = ['Product Name']
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
    total = extractor.get_total()
    df = pd.read_csv(file)
    assert len(df) == 10194
    assert round(total, 2) == 2326534.35


def test_validate_data_on_streaming_extractor():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    # Restrict ship modes so that validation fails
    metadata.get_metadata('Ship Mode').set_property('domain', ["First Class", "Second Class"])
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
    with pytest.raises(ValueError):
        extractor.validate_data()


def test_dataframe_extractor():
    df = pd.read_csv(os.path.join('test', 'orders.csv'))
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    extractor = DataFrameExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        dataframe=df
    )
    assert extractor.validate_data()
    assert extractor.get_total() == 2326534.3543
