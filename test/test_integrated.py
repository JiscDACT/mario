import os

import pytest

from mario.data_extractor import Configuration, HyperFile, DataExtractor
from mario.dataset_builder import DatasetBuilder, Format
from mario.dataset_specification import dataset_from_json, Constraint, dataset_from_manifest
from mario.metadata import metadata_from_json, metadata_from_manifest
from mario.query_builder import SubsetQueryBuilder


def test_integration_tdsx():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(file_path=os.path.join('test', 'orders.hyper'))
    extractor = HyperFile(configuration=configuration, dataset_specification=dataset, metadata=metadata)
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    path = os.path.join('output', dataset.collection, dataset.name + '.tdsx')
    os.makedirs(os.path.join('output', dataset.collection), exist_ok=True)
    builder.build(file_path=path, output_format=Format.TABLEAU_PACKAGED_DATASOURCE)


def test_integration_csv():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(file_path=os.path.join('test', 'orders.hyper'))
    extractor = HyperFile(configuration=configuration, dataset_specification=dataset, metadata=metadata)
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    path = os.path.join('output', dataset.collection, dataset.name + '.csv')
    os.makedirs(os.path.join('output', dataset.collection), exist_ok=True)
    builder.build(file_path=path, output_format=Format.CSV)


def test_integration_excel():
    dataset = dataset_from_manifest(os.path.join('test', 'manifest_superstore.json'))
    metadata = metadata_from_manifest(os.path.join('test', 'manifest_superstore.json'))
    configuration = Configuration(file_path=os.path.join('test', 'orders.hyper'))
    extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    path = os.path.join('output', 'test_integration_excel', 'data.xlsx')
    os.makedirs(os.path.join('output', 'test_integration_excel'), exist_ok=True)
    builder.build(file_path=path, output_format=Format.EXCEL_PIVOT, template_path='excel_template.xlsx')


@pytest.mark.skip
def test_sql_extraction():
    # Set up local test database, drivers and connection string to run this
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    # Add constraints so we test parameter generation
    constraint = Constraint()
    constraint.item = 'Ship Mode'
    constraint.allowed_values = ['First Class']
    dataset.constraints.append(constraint)
    constraint = Constraint()
    constraint.item = 'City'
    constraint.allowed_values = ['Dallas']
    dataset.constraints.append(constraint)

    configuration = Configuration(
        connection_string=os.environ.get("CONNECTION_STRING"),
        schema='dbo',
        view='superstore',
        query_builder=SubsetQueryBuilder
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    extractor.validate_data()
    sql_path = os.path.join('output', 'test_sql_extraction', 'query.sql')
    csv_path = os.path.join('output', 'test_sql_extraction', 'data.csv')
    os.makedirs(os.path.join('output', 'test_sql_extraction'), exist_ok=True)
    extractor.save_data_as_csv(csv_path)
    extractor.save_query(sql_path)


@pytest.mark.skip
def test_sql_extraction_using_manifest():
    # Set up local test database, drivers and connection string to run this
    dataset = dataset_from_manifest(os.path.join('test', 'manifest_superstore.json'))
    metadata = metadata_from_manifest(os.path.join('test', 'manifest_superstore.json'))
    configuration = Configuration(
        connection_string=os.environ.get("CONNECTION_STRING"),
        schema='dbo',
        view='superstore',
        query_builder=SubsetQueryBuilder
    )
    extractor = DataExtractor(
        dataset_specification=dataset,
        metadata=metadata,
        configuration=configuration
    )
    extractor.validate_data()
    sql_path = os.path.join('output', 'test_sql_extraction_using_manifest', 'query.sql')
    csv_path = os.path.join('output', 'test_sql_extraction_using_manifest', 'data.csv')
    os.makedirs(os.path.join('output', 'test_sql_extraction_using_manifest'), exist_ok=True)
    extractor.save_data_as_csv(csv_path)
    extractor.save_query(sql_path)
