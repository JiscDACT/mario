import pandas as pd
import pantab

from mario.dataset_specification import dataset_from_json, Constraint
from mario.metadata import metadata_from_json
from mario.dataset_builder import DatasetBuilder, Format
from mario.data_extractor import Configuration, HyperFile, DataExtractor, DataFrameExtractor, \
    StreamingDataExtractor, PartitioningExtractor
import os
import pytest

from mario.query_builder import ViewBasedQueryBuilder


def test_build_and_export_hyper_as_csv():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(file_path=os.path.join('test', 'orders_with_nulls.hyper'))
    extractor = HyperFile(configuration=configuration, dataset_specification=dataset, metadata=metadata)
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    output_path = os.path.join('output', 'test_build_and_export_csv')
    path = os.path.join(output_path, 'output.csv')
    os.makedirs(output_path, exist_ok=True)

    # Build as a gzipped file using validation
    builder.build(file_path=path, output_format=Format.CSV, compress_using_gzip=True, validate=True, allow_nulls=True)
    df = pd.read_csv(path + '.gz')
    assert len(df) > 0

    # Build without allowing NULLs
    with pytest.raises(ValueError):
        builder.build(file_path=path, output_format=Format.CSV, validate=True, allow_nulls=False)

    # Build with row numbers
    builder.build(file_path=path, output_format=Format.CSV, allow_nulls=True, include_row_numbers=True)
    df = pd.read_csv(path)
    assert 'row_number' in df.columns

    # Build without row numbers
    builder.build(file_path=path, output_format=Format.CSV, allow_nulls=True, include_row_numbers=False)
    df = pd.read_csv(path)
    assert 'row_number' not in df.columns


def test_build_and_export_df_as_csv():
    from tableauhyperapi import TableName
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(file_path=os.path.join('test', 'orders_with_nulls.hyper'))
    df = pantab.frame_from_hyper(source=configuration.file_path, table=TableName('Extract','Extract'))
    extractor = DataFrameExtractor(dataframe=df, dataset_specification=dataset, metadata=metadata)
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    output_path = os.path.join('output', 'test_build_and_export_csv')
    path = os.path.join(output_path, 'output.csv')
    os.makedirs(output_path, exist_ok=True)

    # Build as a gzipped file using validation
    builder.build(file_path=path, output_format=Format.CSV, compress_using_gzip=True, validate=True, allow_nulls=True)
    df = pd.read_csv(path + '.gz')
    assert len(df) > 0

    # Build without allowing NULLs
    with pytest.raises(ValueError):
        builder.build(file_path=path, output_format=Format.CSV, validate=True, allow_nulls=False)

    # Build with row numbers
    builder.build(file_path=path, output_format=Format.CSV, allow_nulls=True, include_row_numbers=True)
    df = pd.read_csv(path)
    assert 'row_number' in df.columns

    # Build without row numbers
    builder.build(file_path=path, output_format=Format.CSV, allow_nulls=True, include_row_numbers=False)
    df = pd.read_csv(path)
    assert 'row_number' not in df.columns


def test_build_and_export_default_as_csv():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(file_path=os.path.join('test', 'orders_with_nulls.hyper'))
    extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    output_path = os.path.join('output', 'test_build_and_export_csv')
    path = os.path.join(output_path, 'output.csv')
    os.makedirs(output_path, exist_ok=True)

    # Build as a gzipped file using validation
    builder.build(file_path=path, output_format=Format.CSV, compress_using_gzip=True, validate=True, allow_nulls=True)
    df = pd.read_csv(path + '.gz')
    assert len(df) > 0

    # Build without allowing NULLs
    with pytest.raises(ValueError):
        builder.build(file_path=path, output_format=Format.CSV, validate=True, allow_nulls=False)

    # Build with row numbers
    builder.build(file_path=path, output_format=Format.CSV, allow_nulls=True, include_row_numbers=True)
    df = pd.read_csv(path)
    assert 'row_number' in df.columns

    # Build without row numbers
    builder.build(file_path=path, output_format=Format.CSV, allow_nulls=True, include_row_numbers=False)
    df = pd.read_csv(path)
    assert 'row_number' not in df.columns


def test_build_and_export_sql_as_csv():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        connection_string=os.environ.get('CONNECTION_STRING'),
        schema="dev",
        view="superstore_with_nulls",
        query_builder=ViewBasedQueryBuilder
    )
    extractor = StreamingDataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    output_path = os.path.join('output', 'test_build_and_export_csv')
    path = os.path.join(output_path, 'output.csv')
    os.makedirs(output_path, exist_ok=True)

    # Build as a gzipped file using validation
    builder.build(file_path=path, output_format=Format.CSV, compress_using_gzip=True, validate=True, allow_nulls=True)
    df = pd.read_csv(path + '.gz')
    assert len(df) > 0

    # Build without allowing NULLs
    with pytest.raises(ValueError):
        builder.build(file_path=path, output_format=Format.CSV, validate=True, allow_nulls=False)

    # Build with row numbers
    builder.build(file_path=path, output_format=Format.CSV, allow_nulls=True, include_row_numbers=True)
    df = pd.read_csv(path)
    assert 'row_number' in df.columns

    # Build without row numbers
    builder.build(file_path=path, output_format=Format.CSV, allow_nulls=True, include_row_numbers=False)
    df = pd.read_csv(path)
    assert 'row_number' not in df.columns


def test_build_and_export_partitioned_sql_as_csv():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    constraint = Constraint()
    constraint.item = 'Category'
    constraint.allowed_values = ['Furniture', 'Office Supplies']
    dataset.constraints.append(constraint)

    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        connection_string=os.environ.get('CONNECTION_STRING'),
        schema="dev",
        view="superstore_with_nulls",
        query_builder=ViewBasedQueryBuilder
    )
    extractor = PartitioningExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata, partition_column='Category')
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    output_path = os.path.join('output', 'test_build_and_export_csv')
    path = os.path.join(output_path, 'output.csv')
    os.makedirs(output_path, exist_ok=True)

    # Build as a gzipped file using validation
    builder.build(file_path=path, output_format=Format.CSV, compress_using_gzip=True, validate=True, allow_nulls=True)
    df = pd.read_csv(path + '.gz')
    assert len(df) > 0

    # Build without allowing NULLs
    with pytest.raises(ValueError):
        builder.build(file_path=path, output_format=Format.CSV, validate=True, allow_nulls=False)

    # Build with row numbers
    builder.build(file_path=path, output_format=Format.CSV, allow_nulls=True, include_row_numbers=True)
    df = pd.read_csv(path)
    assert 'row_number' in df.columns

    # Build without row numbers
    builder.build(file_path=path, output_format=Format.CSV, allow_nulls=True, include_row_numbers=False)
    df = pd.read_csv(path)
    assert 'row_number' not in df.columns