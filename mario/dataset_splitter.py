"""
A utility for partitioning dataset files by a particular field
"""
import os
import shutil
import csv
import logging

from openpyxl import Workbook

from mario.excel_pivot_utils import get_unique_values_for_workbook

logger = logging.getLogger(__name__)


class DatasetSplitter:

    def __init__(self, field: str, source_path: str, output_path: str):
        self.field = field
        self.source_path = source_path
        self.output_path = output_path
        self.other_files = []

        # If the source path doesn't exist, raise an error
        if not os.path.exists(self.source_path):
            raise FileNotFoundError("Source folder doesn't exist.")

        # If the source path and output path are the same, raise an error
        if self.source_path == self.output_path:
            raise ValueError("Source and output folders can't be the same.")

        # If the output path contains subdirectories, raise an error
        if os.path.exists(self.output_path):
            if any(os.path.relpath(root, self.output_path).count(os.sep) > 0 for root, dirs, _ in os.walk(self.output_path)):
                raise ValueError("Output folder contains subdirectories so is probably not something you "
                                 "want to delete and overwrite.")

        # If the source path contains subdirectories, raise an error
        if next(os.walk(self.source_path))[1]:
            raise ValueError("Source folder contains subdirectories so is probably not something you "
                             "want to split as the subdirectories aren't going to be processed.")

        # Delete the output path if it exists
        if os.path.exists(self.output_path):
            logger.info(f"Removing output path at {self.output_path}")
            shutil.rmtree(self.output_path)

        os.makedirs(self.output_path)

    def get_output_path(self, file, field_value):
        return os.path.join(self.output_path, field_value, file)

    def split_files(self):
        # Iterate over each file in the source folder
        split_files_count = 0
        for file in os.listdir(self.source_path):
            if os.path.isfile(os.path.join(self.source_path, file)):
                if file.endswith('.xlsx'):
                    self.split_excel(file)
                    split_files_count += 1
                elif file.endswith('.csv'):
                    self.split_csv(file)
                    split_files_count += 1
                elif file.endswith('.csv.gz'):
                    self.split_gzipped_csv(file)
                    split_files_count += 1
        if split_files_count == 0:
            raise ValueError("No files were split")

    def copy_other_file(self, file):
        logger.info(f"Copying file {file} to output directory")
        for output_directory in os.listdir(self.output_path):
            if os.path.isdir(os.path.join(self.output_path, output_directory)):
                shutil.copyfile(
                    os.path.join(self.source_path, file),
                    os.path.join(self.output_path, output_directory, file))

    def copy_other_files(self) -> None:
        """
        Iterates over the source folder, and copies each non-data file into
        each 'split' output directory present
        :return: None
        """
        for file in os.listdir(self.source_path):
            if not file.endswith('.xlsx') and not file.endswith('.csv') and not file.endswith('.csv.gz'):
                self.other_files.append(file)
        for file in self.other_files:
            self.copy_other_file(file)

    def process_batch(self, batch, column_name, file_handles, file_name):
        for row in batch:
            value = row[column_name]
            if value not in file_handles:
                os.makedirs(os.path.join(self.output_path, value), exist_ok=True)
                file_path = self.get_output_path(field_value=value, file=file_name)
                file_handles[value] = open(file_path, 'w', newline='')
                writer = csv.DictWriter(file_handles[value], fieldnames=row.keys())
                writer.writeheader()
            writer = csv.DictWriter(file_handles[value], fieldnames=row.keys())
            writer.writerow(row)

    def split_gzipped_csv(self, file_name: str, batch_size=10000):
        import gzip

        logger.info(f"Splitting gzipped CSV file {file_name}")

        # Dict for holding open file handles
        file_handles = {}

        # Path to the CSV to split
        file_path = os.path.join(self.source_path, file_name)

        output_file_name = file_name.rstrip('.gz')

        with gzip.open(file_path, 'rt', newline='') as infile:
            reader = csv.DictReader(infile)

            # Process the input file in batches
            batch = []
            for row in reader:
                batch.append(row)
                if len(batch) >= batch_size:
                    self.process_batch(batch, self.field, file_handles, output_file_name)
                    batch = []

            # Process any remaining rows in the last batch
            if batch:
                self.process_batch(batch, self.field, file_handles, output_file_name)

        # Close all open file handles
        for handle in file_handles.values():
            handle.close()

    def split_csv(self, file_name: str, batch_size=10000, compression=None):

        logger.info(f"Splitting CSV file {file_name}")

        # Dict for holding open file handles
        file_handles = {}

        # Path to the CSV to split
        file_path = os.path.join(self.source_path, file_name)

        # Open the input CSV file for reading
        with open(file_path, 'r') as infile:
            reader = csv.DictReader(infile)

            # Process the input file in batches
            batch = []
            for row in reader:
                batch.append(row)
                if len(batch) >= batch_size:
                    self.process_batch(batch, self.field, file_handles, file_name)
                    batch = []

            # Process any remaining rows in the last batch
            if batch:
                self.process_batch(batch, self.field, file_handles, file_name)

        # Close all open file handles
        for handle in file_handles.values():
            handle.close()

    def split_excel(self, file_name: str):
        import pandas as pd
        from openpyxl import load_workbook

        file_path = os.path.join(self.source_path, file_name)
        sheet_names = pd.ExcelFile(file_path).sheet_names

        # Get the values
        try:
            values = get_unique_values_for_workbook(file_path=file_path, field=self.field)
        except ValueError:
            logger.warning("Encountered an Excel file with no data; treating as an 'other' file")
            self.other_files.append(file_name)
            return None

        # Create split workbooks
        for value in values:
            logger.info(f"Splitting for value {value}")
            split_output_path = os.path.join(self.output_path, str(value))
            split_workbook_path = os.path.join(split_output_path, file_name)
            os.makedirs(split_output_path, exist_ok=True)
            shutil.copyfile(src=file_path, dst=split_workbook_path)

            for sheet_name in sheet_names:
                workbook: Workbook = load_workbook(split_workbook_path)
                if len(workbook.get_sheet_by_name(sheet_name)._pivots) > 0:
                    logger.info(f"Splitting excel pivot")
                    self.split_excel_pivot(
                        workbook=workbook,
                        file_path=split_workbook_path,
                        sheet_name=sheet_name,
                        value=value
                    )
                else:
                    logger.info(f"Splitting excel sheet")
                    self.split_excel_table(
                        workbook=workbook,
                        file_path=split_workbook_path,
                        sheet_name=sheet_name,
                        value=value
                    )
                workbook.close()

            # Set active sheet
            workbook: Workbook = load_workbook(split_workbook_path)
            for sheet in workbook:
                workbook[sheet.title].views.sheetView[0].tabSelected = False
            workbook.save(split_workbook_path)
            workbook.close()

    def split_excel_pivot(self, workbook, file_path: str, sheet_name: str, value: str):
        from mario.excel_pivot_utils import replace_pivot_cache_with_subset
        ws = workbook[sheet_name]
        replace_pivot_cache_with_subset(ws, self.field, value)
        workbook.save(file_path)

    def split_excel_table(self, workbook: Workbook, file_path: str, sheet_name: str, value):

        ws = workbook[sheet_name]
        header = [cell.value for cell in ws[1]]  # Read header row

        if self.field not in header:
            return None

        # Create a new sheet for filtered data
        workbook.create_sheet(title=f"{sheet_name}_filtered")
        new_ws = workbook[f"{sheet_name}_filtered"]
        new_ws.append(header)

        # Read rows efficiently
        field_index = header.index(self.field)

        for row in ws.iter_rows(min_row=2, values_only=True):
            if row[field_index] == value:
                new_ws.append(row)

        # Save and remove original sheet
        workbook.remove(ws)
        new_ws.title = sheet_name
        workbook.save(file_path)
