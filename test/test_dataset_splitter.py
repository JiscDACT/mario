from mario.dataset_splitter import DatasetSplitter
import os
import csv


def count_rows_in_csv(file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        row_count = sum(1 for row in reader) - 1  # Subtract 1 to exclude the header row
    return row_count


def test_dataset_splitter():
    # Get the total rows from the test CSV
    total_rows_original = count_rows_in_csv(os.path.join('test', 'test_split_source', 'orders.csv'))

    dataset_splitter = DatasetSplitter(
        field='Region',
        source_path=os.path.join('test', 'test_split_source'),
        output_path=os.path.join('output', 'test_split_source')
    )
    dataset_splitter.split_files()
    dataset_splitter.copy_other_files()
    assert 'Central' in os.listdir(os.path.join('output', 'test_split_source'))
    assert len(os.listdir(os.path.join('output', 'test_split_source'))) == 4

    total_rows = 0
    for region in ['Central', 'East', 'West', 'South']:
        total_rows += count_rows_in_csv(os.path.join('output', 'test_split_source', region, 'orders.csv'))
    assert total_rows_original == total_rows


def test_dataset_splitter_pivot():

    dataset_splitter = DatasetSplitter(
        field='Category',
        source_path=os.path.join('test', 'test_split_pivot'),
        output_path=os.path.join('output', 'test_split_pivot')
    )
    dataset_splitter.split_files()
    dataset_splitter.copy_other_files()
    assert 'Furniture' in os.listdir(os.path.join('output', 'test_split_pivot'))
    assert len(os.listdir(os.path.join('output', 'test_split_pivot'))) == 3


