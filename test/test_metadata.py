import os
import tempfile

from mario.metadata import metadata_from_json


def test_load_metadata():
    metadata_file = os.path.join('test', 'metadata.json')
    metadata = metadata_from_json(file_path=metadata_file)
    assert metadata.get_metadata('Ship Mode') is not None
    assert metadata.get_metadata('Ship Mode').description is not None
    assert metadata.get_metadata('Ship Mode').get_property('groups') == ['Shipping']


def test_save_metadata():
    metadata_file = os.path.join('test', 'metadata.json')
    metadata = metadata_from_json(file_path=metadata_file)
    with tempfile.NamedTemporaryFile() as file:
        metadata.save(file_path=file.name)


def test_load_tdsa():
    metadata_file = os.path.join('test', 'tdsa.json')
    metadata = metadata_from_json(file_path=metadata_file)