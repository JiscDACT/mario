import json
from typing import List
from mario.base import MarioBase


class DatasetSpecification(MarioBase):
    """
    Base class/interface for dataset specifications. Use an implementation specific to the
    format of specification used.
    """

    def __init__(self):
        super().__init__()
        self._name = ''
        self._collection: str = ''
        self._measures: List[str] = []
        self._dimensions: List[str] = []

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def collection(self):
        return self._collection

    @collection.setter
    def collection(self, value):
        self._collection = value

    @property
    def enquiry(self):
        return ''

    @property
    def measures(self):
        return self._measures

    @measures.setter
    def measures(self, value):
        self._measures = value

    @property
    def dimensions(self):
        return self._dimensions

    @dimensions.setter
    def dimensions(self, value):
        self._dimensions = value

    @property
    def items(self):
        return self.dimensions + self.measures

    def save(self, file_path: str):
        json_representation = {
            "name": self.name,
            "collection": self.collection,
            "dimensions": self.dimensions,
            "measures": self.measures
        }
        with open(file_path, 'w') as file:
            json.dump(json_representation, file, default=vars)


def dataset_from_json(file_path: str = None) -> DatasetSpecification:
    with open(file_path, mode='r') as source_file:
        spec = json.load(source_file)

    dataset_specification = DatasetSpecification()
    dataset_specification.name = spec['name']
    dataset_specification.collection = spec['collection']
    dataset_specification.measures = spec['measures']
    dataset_specification.dimensions = spec['dimensions']
    dataset_specification.add_properties(source=spec, exclude=['name', 'collection', 'measures', 'dimensions'])

    return dataset_specification
