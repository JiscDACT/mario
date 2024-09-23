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
        file.close()
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
    domain = metadata.get_metadata('Nationality (UK/ EU/ Non-EU/ Unknown) (2022/23 onwards)').get_property('domain')
    assert 'UK' in domain
    assert '23 onwards' not in domain
    assert metadata.get_metadata('ShipMode (First Class / Same Day / Second Class/Standard Class)') is not None
    assert metadata.get_metadata('Region') is not None
    assert metadata.get_metadata('ShipMode (First Class / Same Day / Second Class/Standard Class)').get_property('domain') is not None
    domain = metadata.get_metadata('ShipMode (First Class / Same Day / Second Class/Standard Class)').get_property('domain')
    assert 'First Class' in domain


def test_get_hierarchies():
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    hierarchies = metadata.get_hierarchies()
    assert len(hierarchies) == 2
    assert 'Product' in hierarchies
    assert 'Location' in hierarchies


def test_get_hierarchy():
    metadata = metadata_from_json(os.path.join('test', 'metadata.json'))
    products = metadata.get_hierarchy('Product')
    assert products == ['Category', 'Product Name']