import os

from mario.data_extractor import DataExtractor, Configuration
from mario.dataset_specification import dataset_from_json
from mario.dataset_builder import DatasetBuilder, Format
from mario.metadata import metadata_from_json

# Build a TD extract in Excel
dataset = dataset_from_json("data_spec.xlsx")
metadata = metadata_from_json('metadata.json')
configuration = Configuration(connection_string=os.environ.get('odbc_connection_string'))
extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
# Either put automation code in here, or override the base class
builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
path = os.path.join(dataset.enquiry, dataset.name + '.xlsx')
builder.build(file_path=path, output_format=Format.EXCEL_PIVOT)

# Build a Heidi Plus Datasource in TDSX
dataset = dataset_from_json("dataset.json")
metadata = metadata_from_json('metadata.json')
configuration = Configuration(
    connection_string=os.environ.get('odbc_connection_string'),
    view='dbo.v_heidi_plus_student_fpe'
)
extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
path = os.path.join('datasources', dataset.collection, dataset.name + '.tdsx')
builder.build(file_path=path, output_format=Format.TABLEAU_PACKAGED_DATASOURCE)
