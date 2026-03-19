from copy import copy

import pytest

from mario.athena import AthenaConfiguration, AthenaStreamingDataExtractor
from mario.dataset_specification import DatasetSpecification
from mario.query_builder import SubsetQueryBuilder
from mario.metadata import Metadata, Item
import os
import shutil
import pandas as pd

from mario.validation import SqlValidator

AWS_PROFILE = os.environ.get('AWS_PROFILE')
AWS_ATHENA_RESULTS_DIR = os.environ.get('AWS_ATHENA_RESULTS_DIR')
AWS_REGION = os.environ.get('AWS_REGION')


class MockHook:
    def __init__(self, extractor: AthenaStreamingDataExtractor):
        self.extractor = extractor

    def get_conn(self):
        return self.extractor.get_connection()


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
    mode_of_study.set_property('domain', ['Full-time', 'Part-time'])
    country_of_he_provider = Item()
    country_of_he_provider.name = 'Country of HE provider'
    country_of_he_provider.set_property('domain', ['England', 'Wales', 'Scotland', 'Northern Ireland'])
    number_field = Item()
    number_field.name = 'Number'
    metadata.add_item(academic_year)
    metadata.add_item(mode_of_study)
    metadata.add_item(number_field)
    metadata.add_item(country_of_he_provider)

    config = AthenaConfiguration()
    config.aws_s3_staging_dir = AWS_ATHENA_RESULTS_DIR
    config.aws_region_name = AWS_REGION

    return dataset, metadata, config


def test_athena_stream_sql_to_csv():
    # Skip this test if we don't have AWS env vars
    if not AWS_PROFILE or not AWS_ATHENA_RESULTS_DIR or not AWS_REGION:
        pytest.skip("Skipping Athena test as AWS not configured")

    shutil.rmtree('output/test_athena', ignore_errors=True)
    os.makedirs('output/test_athena', exist_ok=True)

    dataset, metadata, cfg = get_test_conf()
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


def test_athena_count():
    if not AWS_PROFILE or not AWS_ATHENA_RESULTS_DIR or not AWS_REGION:
        pytest.skip("Skipping Athena test as AWS not configured")

    dataset, metadata, cfg = get_test_conf()
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


def test_athena_save_data_as_csv():
    if not AWS_PROFILE or not AWS_ATHENA_RESULTS_DIR or not AWS_REGION:
        pytest.skip("Skipping Athena test as AWS not configured")

    shutil.rmtree('output/test_athena', ignore_errors=True)
    os.makedirs('output/test_athena', exist_ok=True)

    dataset, metadata, cfg = get_test_conf()
    cfg.query_builder = SubsetQueryBuilder
    cfg.schema = 'demo'
    cfg.view = 'student_open_data'

    extractor = AthenaStreamingDataExtractor(
        configuration=cfg,
        metadata=metadata,
        dataset_specification=dataset
    )

    extractor.save_data_as_csv(
        file_path='output/test_athena/test.csv',
        minimise=False,
        compress_using_gzip=False,
        do_not_modify_source=True
    )

    # Load and test
    df = pd.read_csv('output/test_athena/test.csv')
    for column in dataset.dimensions:
        assert column in df.columns
    assert 'Number' in df.columns
    assert len(df.columns) == len(dataset.items)


def test_athena_validate():
    if not AWS_PROFILE or not AWS_ATHENA_RESULTS_DIR or not AWS_REGION:
        pytest.skip("Skipping Athena test as AWS not configured")

    shutil.rmtree('output/test_athena', ignore_errors=True)
    os.makedirs('output/test_athena', exist_ok=True)

    dataset, metadata, cfg = get_test_conf()
    cfg.query_builder = SubsetQueryBuilder
    cfg.schema = 'demo'
    cfg.view = 'student_open_data'

    extractor = AthenaStreamingDataExtractor(
        configuration=cfg,
        metadata=metadata,
        dataset_specification=dataset
    )
    mock_hook = MockHook(extractor)
    cfg_validator = copy(cfg)
    cfg_validator.hook = mock_hook
    validator = SqlValidator(
        dataset_specification=dataset,
        configuration=cfg_validator,
        metadata=metadata
    )
    validator.validate_data(allow_nulls=False)



