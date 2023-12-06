import json
from typing import List
from mario.base import MarioBase


class Constraint:
    def __init__(self):
        self.item = None
        self.allowed_values = []


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
        self._constraints: List[Constraint] = []

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
    def constraints(self):
        return self._constraints

    @constraints.setter
    def constraints(self, value):
        self._constraints = value

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


def dataset_from_manifest(file_path: str = None):
    with open(file_path, mode='r') as source_file:
        spec = json.load(source_file)

    dataset_specification = DatasetSpecification()

    fields = []
    constraints = []
    for dimension in spec['items']:
        if 'allowedValues' in dimension and len(dimension['allowedValues']) > 0:
            constraint = Constraint()
            constraint.item = dimension['fieldName']
            constraint.allowed_values = dimension['allowedValues']
            constraints.append(constraint)
        else:
            fields.append(dimension['fieldName'])
    if 'onwardUseCategory' in spec:
        constraint = Constraint()
        constraint.item = 'Onward use category '+str(spec['onwardUseCategory'])
        constraint.allowed_values = [1]
        constraints.append(constraint)
    if 'years' in spec:
        constraint = Constraint()
        constraint.item = 'Academic year start'
        constraint.allowed_values = get_year_starts(spec['years'])
        constraints.append(constraint)

    dataset_specification.constraints = constraints
    dataset_specification.measures = [spec['measure']]
    dataset_specification.dimensions = fields
    dataset_specification.name = spec['client']
    dataset_specification.collection = spec['orderRef']
    dataset_specification.add_properties(source=spec, exclude=['name', 'orderRef', 'measure', 'items'])

    return dataset_specification


def get_year_starts(years):
    """
    Converts the array of academic year strings for a request into an array of year start integers
    :return: an array of integers for each year start
    """
    year_starts = []
    for year in years:
        year_start = int(year.split('/')[0])
        year_starts.append(year_start)
    return year_starts
