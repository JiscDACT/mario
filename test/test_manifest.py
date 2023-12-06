import os

from mario.data_extractor import Configuration, DataExtractor
from mario.dataset_specification import dataset_from_json, dataset_from_manifest
from mario.query_builder import SubsetQueryBuilder, ViewBasedQueryBuilder
from mario.metadata import metadata_from_json, metadata_from_manifest


def test_manifest_subset():
    dataset = dataset_from_manifest(os.path.join('test', 'manifest.json'))
    metadata = metadata_from_manifest(os.path.join('test', 'manifest.json'))
    configuration = Configuration(
        view='v_student_fpe',
        schema='dbo',
        query_builder=SubsetQueryBuilder
    )
    extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
    path = os.path.join('output', dataset.collection, dataset.name + '.sql')
    os.makedirs(os.path.join('output', dataset.collection), exist_ok=True)
    extractor.save_query(file_path=path)


def test_spec_subset():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        view='v_student_fpe',
        schema='dbo',
        query_builder=SubsetQueryBuilder
    )
    extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
    path = os.path.join('output', dataset.collection, dataset.name + '_subset.sql')
    os.makedirs(os.path.join('output', dataset.collection), exist_ok=True)
    extractor.save_query(file_path=path)


def test_spec_view():
    dataset = dataset_from_json(os.path.join('test', 'dataset.json'))
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    configuration = Configuration(
        view='v_student_fpe',
        schema='dbo',
        query_builder=ViewBasedQueryBuilder
    )
    extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
    path = os.path.join('output', dataset.collection, dataset.name + '_view.sql')
    os.makedirs(os.path.join('output', dataset.collection), exist_ok=True)
    extractor.save_query(file_path=path)