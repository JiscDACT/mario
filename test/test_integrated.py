import os

from mario.data_extractor import Configuration, HyperFile
from mario.dataset_builder import DatasetBuilder, Format
from mario.dataset_specification import dataset_from_json
from mario.metadata import metadata_from_json


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