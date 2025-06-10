import shutil
import tempfile

import pandas as pd
import pantab
from tableauhyperapi import TableName

from mario.dataset_specification import dataset_from_json, Constraint
from mario.metadata import metadata_from_json
from mario.dataset_builder import DatasetBuilder, Format
from mario.data_extractor import Configuration, HyperFile, DataExtractor, DataFrameExtractor, \
    StreamingDataExtractor, PartitioningExtractor
import os
import pytest

from mario.query_builder import ViewBasedQueryBuilder


def setup_dataset_builder_test(test):
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    output_path = os.path.join('output', test)
    shutil.rmtree(output_path, ignore_errors=True)
    os.makedirs(output_path)
    shutil.copyfile(src=os.path.join('test', 'orders_with_nulls.hyper'), dst=os.path.join(output_path, 'orders_with_nulls.hyper'))
    return output_path, dataset, metadata


def run_consistency_checks(builder, output_path):
    """
    Generates outputs in various formats with various output options to check
    they are being created consistently
    :param builder:
    :param output_path:
    :return:
    """

    # CSV
    path = os.path.join(output_path, 'output.csv')

    # Build as a gzipped csv file using validation
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

    # Now onto Hypers
    table_name = TableName('Extract', 'Extract')
    path = os.path.join(output_path, 'output.hyper')
    if os.path.exists(path):
        os.remove(path)

    # Build hyper without allowing NULLs
    with pytest.raises(ValueError):
        builder.build(file_path=path, output_format=Format.HYPER, validate=True, allow_nulls=False)

    # Build hyper with row numbers
    shutil.copyfile(src=os.path.join('test', 'orders_with_nulls.hyper'), dst=os.path.join(output_path, 'orders_with_nulls.hyper'))
    builder.build(file_path=path, output_format=Format.HYPER, allow_nulls=True, include_row_numbers=True)
    df = pantab.frame_from_hyper(source=path, table=table_name)
    assert 'row_number' in df.columns
    os.remove(path)

    # Build hyper without row numbers
    shutil.copyfile(src=os.path.join('test', 'orders_with_nulls.hyper'), dst=os.path.join(output_path, 'orders_with_nulls.hyper'))
    builder.build(file_path=path, output_format=Format.HYPER, allow_nulls=True, include_row_numbers=False)
    df = pantab.frame_from_hyper(source=path, table=table_name)
    assert 'row_number' not in df.columns

    # Excel
    path = os.path.join(output_path, 'output.xlsx')

    #
    # Currently the Excel builder relies on get_data_frame() which isn't supported
    # with streaming-based extractors, so we need to see if its supported
    #
    streaming = False
    try:
        builder.data.get_data_frame()
    except NotImplementedError:
        streaming = True

    if not streaming:
        # Build without allowing NULLs
        with pytest.raises(ValueError):
            builder.build(file_path=path, output_format=Format.EXCEL_PIVOT, validate=True, allow_nulls=False)

        # Build with row numbers
        builder.build(file_path=path, output_format=Format.EXCEL_PIVOT, allow_nulls=True, include_row_numbers=True)
        df = pd.read_excel(path)
        assert 'row_number' in df.columns

        # Build without row numbers
        builder.build(file_path=path, output_format=Format.EXCEL_PIVOT, allow_nulls=True, include_row_numbers=False)
        df = pd.read_excel(path)
        assert 'row_number' not in df.columns

        # Build with defaults
        builder.build(file_path=path, output_format=Format.EXCEL_PIVOT)
        df = pd.read_excel(path)
        assert 'row_number' not in df.columns


def test_dataset_builder_with_hyperfile_extractor():
    output_path, dataset, metadata = setup_dataset_builder_test('test_dataset_builder_with_hyperfile_extractor')

    configuration = Configuration(file_path=os.path.join(output_path, 'orders_with_nulls.hyper'))
    extractor = HyperFile(configuration=configuration, dataset_specification=dataset, metadata=metadata)
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)

    run_consistency_checks(builder, output_path)


def test_dataset_builder_with_dataframe_extractor():
    from tableauhyperapi import TableName
    output_path, dataset, metadata = setup_dataset_builder_test('test_dataset_builder_with_dataframe_extractor')

    configuration = Configuration(file_path=os.path.join(output_path, 'orders_with_nulls.hyper'))
    df = pantab.frame_from_hyper(source=configuration.file_path, table=TableName('Extract','Extract'))
    extractor = DataFrameExtractor(dataframe=df, dataset_specification=dataset, metadata=metadata)
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)

    run_consistency_checks(builder, output_path)


def test_dataset_builder_with_default_extractor():
    output_path, dataset, metadata = setup_dataset_builder_test('test_dataset_builder_with_default_extractor')

    configuration = Configuration(file_path=os.path.join(output_path, 'orders_with_nulls.hyper'))
    extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)

    run_consistency_checks(builder, output_path)


def test_dataset_builder_with_streaming_extractor():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    output_path, dataset, metadata = setup_dataset_builder_test('test_dataset_builder_with_streaming_extractor')

    configuration = Configuration(
        connection_string=os.environ.get('CONNECTION_STRING'),
        schema="dev",
        view="superstore_with_nulls",
        query_builder=ViewBasedQueryBuilder
    )
    extractor = StreamingDataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)

    run_consistency_checks(builder, output_path)


def test_dataset_builder_with_partitioning_extractor():
    # Skip this test if we don't have a connection string
    if not os.environ.get('CONNECTION_STRING'):
        pytest.skip("Skipping SQL test as no database configured")

    output_path, dataset, metadata = setup_dataset_builder_test('test_dataset_builder_with_streaming_extractor')

    constraint = Constraint()
    constraint.item = 'Category'
    constraint.allowed_values = ['Furniture', 'Office Supplies']
    dataset.constraints.append(constraint)

    configuration = Configuration(
        connection_string=os.environ.get('CONNECTION_STRING'),
        schema="dev",
        view="superstore_with_nulls",
        query_builder=ViewBasedQueryBuilder
    )
    extractor = PartitioningExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata, partition_column='Category')
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)

    run_consistency_checks(builder, output_path)

@pytest.mark.skip(reason='test manually')
def test_hyper_to_pivot():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    dataset.measures = []
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        file_path=os.path.join('test', 'orders.hyper')
    )
    extractor = HyperFile(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    output_folder = os.path.join('output', 'test_hyper_to_pivot')
    os.makedirs(output_folder, exist_ok=True)
    output_file = os.path.join(output_folder, 'orders.xlsx')
    dataset_builder = DatasetBuilder(
        metadata=metadata,
        dataset_specification=dataset,
        data=extractor
    )
    dataset_builder.build(
        file_path=output_file,
        output_format=Format.EXCEL_PIVOT,
        template_path='excel_template.xlsx',
    )


def test_dataset_builder_use_temp_dir():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    dataset.measures = []
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        file_path=os.path.join('test', 'orders.hyper')
    )
    extractor = HyperFile(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        output_folder = os.path.join(temp_dir, 'test_hyper_to_pivot')
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, 'orders.xlsx')

        shutil.copyfile(src='excel_template.xlsx', dst=os.path.join(temp_dir, 'excel_template_copy.xlsx'))
        dataset_builder = DatasetBuilder(
            metadata=metadata,
            dataset_specification=dataset,
            data=extractor
        )
        dataset_builder.build(
            file_path=output_file,
            output_format=Format.EXCEL_PIVOT,
            template_path=os.path.join(temp_dir,'excel_template_copy.xlsx'),
        )
