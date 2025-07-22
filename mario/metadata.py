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

    def get_hierarchies(self):
        """ Returns a list of all hierarchies in the metadata """
        hierarchies = set()
        for item in self.items:
            if item.get_property('hierarchies') is not None:
                for hierarchy in item.get_property('hierarchies'):
                    hierarchies.add(hierarchy['hierarchy'])
        return list(hierarchies)

    def get_hierarchy(self, name):
        """ Returns an ordered list of the item names in the specified hierarchy """
        items = []
        for item in self.items:
            if item.get_property('hierarchies') is not None:
                for hierarchy in item.get_property('hierarchies'):
                    if hierarchy['hierarchy'] == name:
                        items.append({'name': item.name, 'level': hierarchy['level']})

        # Sort the list of dictionaries by 'level'
        sorted_items = sorted(items, key=lambda x: x['level'])
        # Extract the 'name' values in order
        item_names_in_order = [item['name'] for item in sorted_items]
        return item_names_in_order

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

        with open(file_path, mode='w', encoding='utf-8') as file:
            json.dump(json_representation, file, default=vars)


def metadata_from_json(file_path: str = None) -> Metadata:
    """ Factory method for creating a Metadata instance from a JSON file"""
    metadata = Metadata()

    with open(file_path, encoding='utf-8') as metadata_file:
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


def metadata_from_manifest(file_path=None) -> Metadata:
    """ Factory method for creating a Metadata instance from a JSON file in manifest format"""
    metadata = Metadata()

    with open(file_path, encoding='utf-8') as metadata_file:
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
        metadata_item.add_properties(source=item, exclude=[name])
        metadata.add_item(metadata_item)

    # For TDSA manifests, need to also add the measure as an item
    if 'measure' in metadata_json:
        measure_metadata_item = Item()
        measure_metadata_item.name = metadata_json['measure']
        metadata.add_item(measure_metadata_item)

    return metadata


def metadata_from_excel(
        file_path: str = None,
        name: str = "Metdata",
        sheet_name: str = 'Pick list',
        field_name_column: str = 'Field + Options combined'
) -> Metadata:
    """ Factory method for creating a Metadata instance from an Excel file"""
    import pandas as pd
    import re
    pick_list = pd.read_excel(open(file_path, 'rb'), sheet_name=sheet_name, skiprows=1, header=0)
    pick_list.reset_index()
    pick_list.fillna('', inplace=True)
    pick_list.dropna(how='all', axis=1, inplace=True)

    metadata = Metadata()
    metadata.name = name
    metadata.set_property('source', file_path)
    for index, row in pick_list.iterrows():
        if isinstance(row[field_name_column], str):
            data_item = Item()
            data_item.name = row[field_name_column]
            for key, value in row.to_dict().items():
                data_item.set_property(key, value)
            # Domain splits
            pattern = re.compile("\\(([^)]*)\\)")
            found = re.findall(pattern, data_item.name)
            if found:
                domain = [x.strip() for x in found[0].split('/')]
                if len(domain) > 1:
                    data_item.set_property('domain', domain)
            metadata.add_item(data_item)

    return metadata
