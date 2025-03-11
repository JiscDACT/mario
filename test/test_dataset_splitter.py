import shutil

import pytest

from mario.dataset_splitter import DatasetSplitter
import os
import csv


def count_rows_in_csv(file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        row_count = sum(1 for row in reader) - 1  # Subtract 1 to exclude the header row
    return row_count


def test_dataset_splitter_output_dir_has_subdirectories():
    output_dir = os.path.join('output', 'test_dataset_splitter_output_dir_has_subdirectories')
    os.makedirs(os.path.join(output_dir, 'fruit', 'banana'), exist_ok=True)
    with pytest.raises(ValueError):
        DatasetSplitter(
            field='Region',
            source_path=os.path.join('test', 'test_split_source'),
            output_path=output_dir
        )
    shutil.rmtree(output_dir)


def test_dataset_splitter_output_dir_has_deep_subdirectories():
    output_dir = os.path.join('output', 'test_dataset_splitter_output_dir_has_deep_subdirectories')
    os.makedirs(os.path.join(output_dir, 'fruit', 'banana', 'yellow'), exist_ok=True)
    with pytest.raises(ValueError):
        DatasetSplitter(
            field='Region',
            source_path=os.path.join('test', 'test_split_source'),
            output_path=output_dir
        )
    shutil.rmtree(output_dir)


def test_dataset_splitter_source_dir_does_not_exist():
    output_dir = os.path.join('output', 'test_dataset_splitter_source_dir_does_not_exist')
    os.makedirs(output_dir, exist_ok=True)
    with pytest.raises(FileNotFoundError):
        DatasetSplitter(
            field='Region',
            source_path=os.path.join('test', 'no_such_folder'),
            output_path=output_dir
        )
    shutil.rmtree(output_dir)


def test_dataset_splitter_source_dir_has_subdirectories():
    source_dir = os.path.join('output', 'test_dataset_splitter_source_dir_has_subdirectories_source')
    os.makedirs(os.path.join(source_dir, 'For_Supply'), exist_ok=True)
    output_dir = os.path.join('output', 'test_dataset_splitter_source_dir_has_subdirectories_output')
    os.makedirs(output_dir, exist_ok=True)
    with pytest.raises(ValueError):
        DatasetSplitter(
            field='Region',
            source_path=source_dir,
            output_path=output_dir
        )
    shutil.rmtree(source_dir)
    shutil.rmtree(output_dir)


def test_dataset_splitter_source_and_output_are_the_same():
    source_dir = os.path.join('output', 'test_dataset_splitter_source_and_output_are_the_same')
    output_dir = os.path.join('output', 'test_dataset_splitter_source_and_output_are_the_same')
    os.makedirs(output_dir, exist_ok=True)
    with pytest.raises(ValueError):
        DatasetSplitter(
            field='Region',
            source_path=source_dir,
            output_path=output_dir
        )

    shutil.rmtree(output_dir)


def test_dataset_splitter_no_files_to_split():
    source_dir = os.path.join('output', 'test_dataset_splitter_no_files_to_split_source')
    output_dir = os.path.join('output', 'test_dataset_splitter_no_files_to_split_output')
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(source_dir, exist_ok=True)
    shutil.copyfile(os.path.join('test', 'README.md'), os.path.join(output_dir, 'README.md'))
    with pytest.raises(ValueError) as e:
        data_splitter = DatasetSplitter(
            field='Region',
            source_path=source_dir,
            output_path=output_dir
        )
        data_splitter.split_files()
    shutil.rmtree(source_dir)
    shutil.rmtree(output_dir)


def test_dataset_splitter():
    # Get the total rows from the test CSV
    total_rows_original = count_rows_in_csv(os.path.join('test', 'test_split_source', 'orders.csv'))

    dataset_splitter = DatasetSplitter(
        field='Region',
        source_path=os.path.join('test', 'test_split_source'),
        output_path=os.path.join('output', 'test_dataset_splitter')
    )
    dataset_splitter.split_files()
    dataset_splitter.copy_other_files()
    assert 'Central' in os.listdir(os.path.join('output', 'test_dataset_splitter'))
    assert 'orders.csv' in os.listdir(os.path.join('output', 'test_dataset_splitter', 'Central'))
    assert 'orders.xlsx' in os.listdir(os.path.join('output', 'test_dataset_splitter', 'Central'))
    assert 'README.txt' in os.listdir(os.path.join('output', 'test_dataset_splitter', 'Central'))

    assert len(os.listdir(os.path.join('output', 'test_dataset_splitter'))) == 4

    total_rows = 0
    for region in ['Central', 'East', 'West', 'South']:
        total_rows += count_rows_in_csv(os.path.join('output', 'test_dataset_splitter', region, 'orders.csv'))
    assert total_rows_original == total_rows


def test_dataset_splitter_pivot():

    dataset_splitter = DatasetSplitter(
        field='Category',
        source_path=os.path.join('test', 'test_split_pivot'),
        output_path=os.path.join('output', 'test_dataset_splitter_pivot')
    )
    dataset_splitter.split_files()
    dataset_splitter.copy_other_files()
    assert 'Furniture' in os.listdir(os.path.join('output', 'test_dataset_splitter_pivot'))
    assert len(os.listdir(os.path.join('output', 'test_dataset_splitter_pivot'))) == 3


