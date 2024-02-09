# General purpose data pipelines 

A set of base classes and helpers for 
common data pipeline tasks, whether using
in local control scripts or in Airflow tasks.

## !WARNING!
This is a public repository for ease of deployment.
Be especially careful not to commit anything
that shouldn't be here.

# Installing
Use pip to install:

`pip install mario-data-pipelines`

From v0.20, the default installation doesn't support Tableau Hyper files or TDSX packages. To
install with those features use

`pip install mario-data-pipelines[Tableau]`

...or add to requirements.txt.

# Using

## Modules

Mario has the following standard modules:

* `dataset_specification`: for working with dataset 
specifications, including loading from files.
* `metadata`: for working with 
metadata, including loading from JSON or Excel (SpecBuilder).
* `data_extractor`: for extracting and supplying
data, whether its building SQL queries, downloading a 
single view, or a wrapper around reading a local file. It also
has standard data QA functions.
* `dataset_builder`: for building outputs in a range of
formats including CSV, Excel, Tableau and PowerBI
* `query_builder`: for building SQL queries - passed as 
part of the configuration for a data_extractor.

These build on one another, so the classes defined
in the modules are used as the input parameter types
for the constructors in others, e.g. the `DataExtractor`
constructor takes a `DatasetSpecification` and `Metadata`, 
while the `DatasetBuilder` constructor takes a 
`DatasetSpecification`,`Metadata` and `DataExtractor`.

_NOTE: As of the current release, only some
functionality has been implemented._

## Example: Tailored data
_Note that the DataWarehouseQueryBuilder isn't part of the Mario 
project, but extends the QueryBuilder base class it defines._
~~~
dataset = dataset_from_excel("SpecificationInput.xlsx")
metadata = metadata_from_excel('Metadata.xlsx')
configuration = Configuration(
    connection_string=os.environ.get('odbc_connection_string'),
    query_builder=DataWarehouseQueryBuilder
    )
extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
if extractor.validate_data():
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    path = os.path.join(dataset.enquiry, dataset.name + '.xlsx')
    builder.build(file_path=path, output_format=Format.EXCEL_PIVOT)
~~~

## Example: Heidi Plus

~~~
dataset = dataset_from_json("dataset.json")
metadata = metadata_from_json('metadata.json')
configuration = Configuration(
    connection_string=os.environ.get('odbc_connection_string'),
    view='v_heidi_plus_student_fpe',
    schema='dbo',
    query_builder=ViewQueryBuilder
)
extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
if extractor.validate_data():
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    path = os.path.join('datasources', dataset.collection, dataset.name + '.tdsx')
    builder.build(file_path=path, output_format=Format.TABLEAU_PACKAGED_DATASOURCE)
~~~

## Example: TDSA
~~~
dataset = dataset_from_manifest("manifest.json")
metadata = metadata_from_manifest('manifest.json')
configuration = Configuration(
    connection_string=os.environ.get('odbc_connection_string'),
    view='v_student_fpe',
    schema='dbo',
    query_builder=SubsetQueryBuilder
)
extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
if extractor.validate_data():
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    path = os.path.join('datasources', dataset.collection, dataset.name + '.tdsx')
    builder.build(file_path=path, output_format=Format.TABLEAU_PACKAGED_DATASOURCE)
~~~

## Example: Using an Apache Airflow hook

~~~
dataset = dataset_from_manifest("manifest.json")
metadata = metadata_from_manifest('manifest.json')
configuration = Configuration(
    hook=PostgresHook(conn_id='postgres-default'),
    view='v_student_fpe',
    schema='dbo',
    query_builder=SubsetQueryBuilder
)
extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
if extractor.validate_data():
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    path = os.path.join('datasources', dataset.collection, dataset.name + '.tdsx')
    builder.build(file_path=path, output_format=Format.TABLEAU_PACKAGED_DATASOURCE)
~~~


## Example: Multiple outputs
The same builder can be used to build multiple outputs 
from the same source, in different formats:
~~~
    builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
    builder.build(file_path=tableau_path, output_format=Format.TABLEAU_PACKAGED_DATASOURCE)
    builder.build(file_path=excel_path, output_format=Format.EXCEL_PIVOT)
    builder.build(file_path=csv_path, output_format=Format.CSV)
    builder.build(file_path=pbix_path, output_format=Format.POWERBI_PACKAGE)
~~~


## Roadmap

* Create DataSpecification from "SpecificationInputTemplate.xlsx" 
* Excel Pivot output format
* Excel Info Sheet output
* PowerBI PBIX output format


# Building and releasing new versions
TODO