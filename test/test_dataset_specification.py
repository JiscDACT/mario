import os

from mario.dataset_specification import dataset_from_json


def test_import_dataset():
    specification_file = os.path.join('test', 'dataset.json')
    specification = dataset_from_json(file_path=specification_file)

    assert specification.collection == 'superstore'
    assert len(specification.measures) == 4
    assert specification.get_property('description') is not None
