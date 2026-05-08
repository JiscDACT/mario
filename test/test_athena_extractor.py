from copy import copy

import pytest

from mario.athena import AthenaConfiguration, AthenaDataExtractor
from mario.dataset_specification import DatasetSpecification, Constraint
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
    def __init__(self, extractor: AthenaDataExtractor):
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


def test_athena_count():
    if not AWS_PROFILE or not AWS_ATHENA_RESULTS_DIR or not AWS_REGION:
        pytest.skip("Skipping Athena test as AWS not configured")

    dataset, metadata, cfg = get_test_conf()
    cfg.query_builder = SubsetQueryBuilder
    cfg.schema = 'demo'
    cfg.view = 'student_open_data'

    extractor = AthenaDataExtractor(
        configuration=cfg,
        metadata=metadata,
        dataset_specification=dataset
    )

    total = extractor.get_total(measure=dataset.measures[0])
    assert total == 28_733_910


def test_athena_save_data_as_csv():
    if not AWS_PROFILE or not AWS_ATHENA_RESULTS_DIR or not AWS_REGION:
        pytest.skip("Skipping Athena test as AWS not configured")

    shutil.rmtree('output/test_athena', ignore_errors=True)
    os.makedirs('output/test_athena', exist_ok=True)

    dataset, metadata, cfg = get_test_conf()
    cfg.query_builder = SubsetQueryBuilder
    cfg.schema = 'demo'
    cfg.view = 'student_open_data'

    extractor = AthenaDataExtractor(
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
    for dimension in dataset.dimensions:
        assert dimension in df.columns
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

    extractor = AthenaDataExtractor(
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


def test_athena_save_data_as_csv_with_gzip():
    if not AWS_PROFILE or not AWS_ATHENA_RESULTS_DIR or not AWS_REGION:
        pytest.skip("Skipping Athena test as AWS not configured")

    shutil.rmtree('output/test_athena', ignore_errors=True)
    os.makedirs('output/test_athena', exist_ok=True)

    dataset, metadata, cfg = get_test_conf()
    cfg.query_builder = SubsetQueryBuilder
    cfg.schema = 'demo'
    cfg.view = 'student_open_data'

    extractor = AthenaDataExtractor(
        configuration=cfg,
        metadata=metadata,
        dataset_specification=dataset
    )

    extractor.save_data_as_csv(
        file_path='output/test_athena/test.csv',
        minimise=False,
        compress_using_gzip=True,
        do_not_modify_source=True
    )

    # Load and test
    df = pd.read_csv('output/test_athena/test.csv.gz')
    for column in dataset.dimensions:
        assert column in df.columns
    assert 'Number' in df.columns
    for dimension in dataset.dimensions:
        assert dimension in df.columns
    assert len(df.columns) == len(dataset.items)


def test_athena_with_constraints():
    if not AWS_PROFILE or not AWS_ATHENA_RESULTS_DIR or not AWS_REGION:
        pytest.skip("Skipping Athena test as AWS not configured")

    shutil.rmtree('output/test_athena_with_constraints', ignore_errors=True)
    os.makedirs('output/test_athena_with_constraints', exist_ok=True)

    dataset, metadata, cfg = get_test_conf()
    level_of_study = Item()
    level_of_study.name = 'Level of study'
    level_constraint = Constraint()
    level_constraint.item = level_of_study.name
    level_constraint.allowed_values = ['Postgraduate (research)', 'Postgraduate (taught)']
    metadata.add_item(level_of_study)
    dataset.dimensions.append(level_of_study.name)
    dataset.constraints.append(level_constraint)
    cfg.query_builder = SubsetQueryBuilder
    cfg.schema = 'demo'
    cfg.view = 'student_open_data'

    extractor = AthenaDataExtractor(
        configuration=cfg,
        metadata=metadata,
        dataset_specification=dataset
    )

    extractor.save_data_as_csv(
        file_path='output/test_athena_with_constraints/test.csv',
        minimise=False,
        compress_using_gzip=False,
        do_not_modify_source=True
    )

    # Load and test
    df = pd.read_csv('output/test_athena_with_constraints/test.csv')
    for column in dataset.dimensions:
        assert column in df.columns
    assert 'Number' in df.columns
    for dimension in dataset.dimensions:
        assert dimension in df.columns
    assert len(df.columns) == len(dataset.items)
    assert len(df['Level of study'].unique()) == 2


def test_athena_with_constraints_and_apostrophes():
    if not AWS_PROFILE or not AWS_ATHENA_RESULTS_DIR or not AWS_REGION:
        pytest.skip("Skipping Athena test as AWS not configured")

    shutil.rmtree('output/test_athena_with_constraints_and_apostrophes', ignore_errors=True)
    os.makedirs('output/test_athena_with_constraints_and_apostrophes', exist_ok=True)

    dataset, metadata, cfg = get_test_conf()
    provider = Item()
    provider.name = 'HE Provider'
    provider_constraint = Constraint()
    provider_constraint.item = provider.name
    provider_constraint.allowed_values = ["Queen's University Belfast", "City St George's, University of London"]
    metadata.add_item(provider)
    dataset.dimensions.append(provider.name)
    dataset.constraints.append(provider_constraint)
    cfg.query_builder = SubsetQueryBuilder
    cfg.schema = 'demo'
    cfg.view = 'student_open_data'

    extractor = AthenaDataExtractor(
        configuration=cfg,
        metadata=metadata,
        dataset_specification=dataset
    )

    extractor.save_data_as_csv(
        file_path='output/test_athena_with_constraints_and_apostrophes/test.csv',
        minimise=False,
        compress_using_gzip=False,
        do_not_modify_source=True
    )

    # Load and test
    df = pd.read_csv('output/test_athena_with_constraints_and_apostrophes/test.csv')
    for column in dataset.dimensions:
        assert column in df.columns
    assert 'Number' in df.columns
    for dimension in dataset.dimensions:
        assert dimension in df.columns
    assert len(df.columns) == len(dataset.items)
    assert len(df['HE Provider'].unique()) == 2
    assert "Queen's University Belfast" in df['HE Provider'].unique()