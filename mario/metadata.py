import json
from typing import List


class Item:

    def __init__(self):
        self._name = ''
        self._description = ''
        self._properties = {}

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

    def set_property(self, name, value):
        self._properties[name] = value

    def get_property(self, name):
        if name in self._properties:
            return self._properties[name]
        return None

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
        collection = 'collection'
        items = 'items'
        name = 'name'
    else:
        collection = 'datasource'
        items = 'fields'
        name = 'fieldName'

    metadata.name = metadata_json[collection]['name']

    for item in metadata_json[collection][items]:
        metadata_item = Item()
        metadata_item.name = item[name]
        metadata_item.description = item['description']
        for prop in item:
            if prop not in [name, 'description']:
                metadata_item.set_property(prop, item[prop])
        metadata.add_item(metadata_item)

    return metadata
