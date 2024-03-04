import os

from mario.dataset_specification import dataset_from_json, dataset_from_manifest, dataset_from_excel


def test_import_dataset():
    specification_file = os.path.join('test', 'dataset.json')
    specification = dataset_from_json(file_path=specification_file)

    assert specification.collection == 'superstore'
    assert len(specification.measures) == 4
    assert specification.get_property('description') is not None


def test_import_dataset_from_manifest():
    specification_file = os.path.join('test', 'manifest.json')
    specification = dataset_from_manifest(file_path=specification_file)

    assert specification.name == 'test@jisc.ac.uk'
    assert specification.collection == 'manifest_test'


def test_import_dataset_from_excel():
    specification_file = os.path.join('test', 'SpecificationInputTemplate.xlsx')
    specification = dataset_from_excel(file_path=specification_file)

    assert specification.name == 'Item 1'
    assert specification.get_property('Item Number') == 'Item 1'
    assert specification.collection == 'Student Record and AP Student Record'
    assert specification.measures == ['FPE']
    assert specification.constraints[0].allowed_values == [2021, 2022, 2023]
    assert specification.constraints[1].item == 'Onward use category'
    assert specification.constraints[1].allowed_values == [1]
    assert specification.dimensions == ['Sex', 'HE Provider']