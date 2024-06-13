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
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(json_representation, file, default=vars)


def dataset_from_json(file_path: str = None) -> DatasetSpecification:
    with open(file_path, mode='r', encoding='utf-8') as source_file:
        spec = json.load(source_file)

    dataset_specification = DatasetSpecification()
    dataset_specification.name = spec['name']
    dataset_specification.collection = spec['collection']
    dataset_specification.measures = spec['measures']
    dataset_specification.dimensions = spec['dimensions']
    dataset_specification.add_properties(source=spec, exclude=['name', 'collection', 'measures', 'dimensions'])

    return dataset_specification


def dataset_from_manifest(file_path: str = None):
    with open(file_path, mode='r', encoding='utf-8') as source_file:
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


def dataset_from_excel(file_path: str):
    from openpyxl import load_workbook
    from openpyxl.worksheet.worksheet import Worksheet

    content = {
        'Enquiry Number': 'A3',
        'Item Number': 'C2',
        'Organisation Name': 'A6',
        'Licence expiry date': 'A9',
        'Data Format': 'C5',
        'Onward use': 'C13',
        'Additional restrictions': 'C15'
    }

    dataset_specification = DatasetSpecification()

    # Load sheet
    workbook = load_workbook(filename=file_path, read_only=True)
    sheet: Worksheet = workbook.get_sheet_by_name('InputTemplate')

    # Read general spec values
    dataset_specification.name = sheet['C2'].value
    dataset_specification.collection = sheet['C8'].value
    for prop in content.items():
        value = sheet[prop[1]].value
        dataset_specification.set_property(prop[0], value)

    # Read fields
    cell_range = 'E3:E100'
    for row in sheet[cell_range]:
        for cell in row:
            if cell.value:
                dataset_specification.dimensions.append(cell.value)

    # Measure
    dataset_specification.measures = [sheet['C22'].value]

    # Constraints
    constraints = []

    # Years
    spec_year = sheet['C11'].value
    year_start = int(spec_year[0:4])
    year_end = year_start
    spec_year = spec_year.replace("—", "-")
    if spec_year.find('-') != -1:
        year_end = int(spec_year.split('-')[1].strip())

    years_constraint = Constraint()
    years_constraint.item = 'Academic year'
    years_constraint.allowed_values = list(range(year_start, year_end + 1))
    constraints.append(years_constraint)

    # Onward use
    onward_use_constraint = Constraint()
    onward_use_constraint.item = 'Onward use category'
    onward_use_constraint.allowed_values = [sheet['C13'].value]
    constraints.append(onward_use_constraint)

    # Other restrictions - c15 to c19
    for cell in ['C15', 'C16', 'C17', 'C18', 'C19']:
        if sheet[cell].value is not None:
            constraint = Constraint()
            constraint.item = sheet[cell].value
            constraints.append(constraint)

    dataset_specification.constraints = constraints

    # Deal with windows greedy file handler madness
    workbook.close()
    import gc
    gc.collect()

    return dataset_specification
