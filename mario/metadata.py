import json
from typing import List
from mario.base import MarioBase


class Item(MarioBase):

    def __init__(self):
        super().__init__()
        self._name = ''
        self._description = ''

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, value):
        self._description = value

    def to_json(self):
        json_representation = {
            "name": self.name,
            "description": self.description
        }
        for key, value in self._properties.items():
            json_representation[key] = value
        return json_representation


class Metadata(Item):

    def __init__(self, name: str = None):
        super().__init__()
        self._items: List[Item] = []
        self.name = name
        if self.name is None:
            self.name = 'Metadata'

    def get_metadata(self, name: str):
        for item in self._items:
            if item.name == name:
                return item
        return None

    @property
    def items(self):
        return self._items

    def add_item(self, item: Item) -> None:
        self._items.append(item)

    def merge_items(self, metadata) -> None:
        for item in metadata.items:
            self.add_item(item)

    def save(self, file_path: str = None) -> None:
        json_representation = {
            "collection": {
                "name": self.name,
                "items": []
            }
        }
        for prop in self.properties:
            json_representation['collection'][prop] = self.get_property(prop)

        for item in self._items:
            json_representation['collection']['items'].append(item.to_json())

        with open(file_path, mode='w') as file:
            json.dump(json_representation, file, default=vars)


def metadata_from_json(file_path: str = None):
    """ Factory method for creating a Metadata instance from a JSON file"""
    metadata = Metadata()

    with open(file_path) as metadata_file:
        metadata_json = json.load(metadata_file)

    if 'collection' in metadata_json:
        collection = metadata_json['collection']
        metadata.name = collection['name']
        items = 'items'
        name = 'name'
    else:
        collection = metadata_json['datasource']
        metadata.name = collection['name']
        items = 'fields'
        name = 'fieldName'

    metadata.add_properties(source=collection, exclude=[name, items])

    for item in collection[items]:
        metadata_item = Item()
        metadata_item.name = item[name]
        if 'description' in item:
            metadata_item.description = item['description']
        metadata_item.add_properties(source=item, exclude=[name, 'description'])
        metadata.add_item(metadata_item)

    return metadata


def metadata_from_manifest(file_path=None):
    """ Factory method for creating a Metadata instance from a JSON file in manifest format"""
    metadata = Metadata()

    with open(file_path) as metadata_file:
        metadata_json = json.load(metadata_file)

    collection = metadata_json
    metadata.name = metadata_json['datasource']
    items = 'items'
    name = 'fieldName'

    metadata.add_properties(source=collection, exclude=[name, items])

    for item in collection[items]:
        metadata_item = Item()
        metadata_item.name = item[name]
        if 'description' in item:
            metadata_item.description = item['description']
        metadata_item.add_properties(source=item, exclude=[name, 'description'])
        metadata.add_item(metadata_item)

    # For TDSA manifests, need to also add the measure as an item
    if 'measure' in metadata_json:
        measure_metadata_item = Item()
        measure_metadata_item.name = metadata_json['measure']
        metadata.add_item(measure_metadata_item)

    return metadata

