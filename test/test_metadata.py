import os
import tempfile

from mario.metadata import metadata_from_json, metadata_from_excel


def test_load_metadata():
    metadata_file = os.path.join('test', 'metadata.json')
    metadata = metadata_from_json(file_path=metadata_file)
    assert metadata.get_metadata('Ship Mode') is not None
    assert metadata.get_metadata('Ship Mode').description is not None
    assert metadata.get_metadata('Ship Mode').get_property('groups') == ['Shipping']
    assert metadata.get_property('description') is not None


def test_save_metadata():
    metadata_file = os.path.join('test', 'metadata.json')
    metadata = metadata_from_json(file_path=metadata_file)
    metadata.set_property("fruit", "banana")
    with tempfile.NamedTemporaryFile() as file:
        metadata.save(file_path=file.name)
        metadata = metadata_from_json(file.name)
        assert metadata.get_property('fruit') == 'banana'


def test_load_tdsa():
    metadata_file = os.path.join('test', 'tdsa.json')
    metadata = metadata_from_json(file_path=metadata_file)
    assert metadata.get_metadata('Ship Mode') is not None


def test_load_excel():
    metadata_file = os.path.join('test', 'spec_example.xlsx')
    metadata = metadata_from_excel(file_path=metadata_file)
    assert metadata.get_metadata('ShipMode') is not None
    assert metadata.get_metadata('Region') is not None
