from mario.athena import AthenaConfiguration, AthenaStreamingDataExtractor
from mario.dataset_specification import DatasetSpecification
from mario.query_builder import SubsetQueryBuilder
from mario.metadata import Metadata, Item
import os
import shutil
import pandas as pd


def get_test_conf():
    dataset = DatasetSpecification()
    dataset.dimensions = [
        "Academic Year",
        "Mode of study",
        "Country of HE provider"
    ]
    dataset.measures = ['Number']
    dataset.name = 'student_open_data'
    metadata = Metadata()
    academic_year = Item()
    academic_year.name = 'Academic Year'
    mode_of_study = Item()
    mode_of_study.name = 'Mode of study'
    country_of_he_provider = Item()
    country_of_he_provider.name = 'Country of HE provider'
    number_field = Item()
    number_field.name = 'Number'
    metadata.add_item(academic_year)
    metadata.add_item(mode_of_study)
    metadata.add_item(number_field)
    metadata.add_item(country_of_he_provider)
    return dataset, metadata


def test_athena_stream():
    shutil.rmtree('output/test_athena', ignore_errors=True)
    os.makedirs('output/test_athena', exist_ok=True)

    dataset, metadata = get_test_conf()
    cfg = AthenaConfiguration()
    cfg.query_builder = SubsetQueryBuilder
    cfg.schema = 'demo'
    cfg.view = 'student_open_data'

    extractor = AthenaStreamingDataExtractor(
        configuration=cfg,
        metadata=metadata,
        dataset_specification=dataset
    )

    extractor.stream_sql_to_csv(
        file_path='output/test_athena/test.csv',
        minimise=True,
        compress_using_gzip=False,
        do_not_modify_source=True
    )

    # Load and test
    df = pd.read_csv('output/test_athena/test.csv')
    for column in dataset.dimensions:
        assert column in df.columns
    assert 'Number' in df.columns
    assert len(df.columns) == len(dataset.items)

    extractor.stream_sql_to_hyper(
        file_path='output/test_athena/test.hyper',
        minimise=True,
        compress_using_gzip=False,
        do_not_modify_source=True
    )


def test_athena_count():

    dataset, metadata = get_test_conf()
    cfg = AthenaConfiguration()
    cfg.query_builder = SubsetQueryBuilder
    cfg.schema = 'demo'
    cfg.view = 'student_open_data'

    extractor = AthenaStreamingDataExtractor(
        configuration=cfg,
        metadata=metadata,
        dataset_specification=dataset
    )

    total = extractor.get_total(measure=dataset.measures[0])
    print("total", total)  # 28,733,910

