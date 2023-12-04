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

...or add to requirements.txt.

# Using

## Example: Tailored data
Note that we haven't yet incorporated the SQL builder for TD,
but this is what the actual control script for 'do TD' would
look like.
~~~
dataset = dataset_from_json("data_spec.xlsx")
metadata = metadata_from_json('metadata.json')
configuration = Configuration(connection_string=os.environ.get('odbc_connection_string'))
extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
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
    view='dbo.v_heidi_plus_student_fpe'
)
extractor = DataExtractor(configuration=configuration, dataset_specification=dataset, metadata=metadata)
builder = DatasetBuilder(dataset_specification=dataset, metadata=metadata, data=extractor)
path = os.path.join('datasources', dataset.collection, dataset.name + '.tdsx')
builder.build(file_path=path, output_format=Format.TABLEAU_PACKAGED_DATASOURCE)
~~~

# Building and releasing new versions
