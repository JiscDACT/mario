"""
A utility for partitioning dataset files by a particular field
"""
import os
import shutil
import csv
import logging

logger = logging.getLogger(__name__)


class DatasetSplitter:

    def __init__(self, field: str, source_path: str, output_path: str):
        self.field = field
        self.source_path = source_path
        self.output_path = output_path

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
        if split_files_count == 0:
            raise ValueError("No files were split")

    def copy_other_files(self) -> None:
        """
        Iterates over the source folder, and copies each non-data file into
        each 'split' output directory present
        :return: None
        """
        for file in os.listdir(self.source_path):
            if not file.endswith('.xlsx') and not file.endswith('.csv'):
                logger.info(f"Copying file {file} to output directory")
                for output_directory in os.listdir(self.output_path):
                    if os.path.isdir(os.path.join(self.output_path, output_directory)):
                        shutil.copyfile(
                            os.path.join(self.source_path, file),
                            os.path.join(self.output_path, output_directory, file))

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

    def split_csv(self, file_name: str, batch_size=10000):

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

    def split_excel_pivot(self, file_name: str, file_path: str, sheet_name: str):
        from mario.excel_pivot_utils import get_unique_values, replace_pivot_cache_with_subset
        from openpyxl import load_workbook

        # Get the values to split by
        values = get_unique_values(file_path=file_path, sheet_name=sheet_name, field=self.field)

        for value in values:
            # Copy workbook
            os.makedirs(os.path.join(self.output_path, value), exist_ok=True)
            shutil.copyfile(
                os.path.join(self.source_path, file_name),
                self.get_output_path(file_name, value)
            )

            # Load the split workbook
            wb = load_workbook(self.get_output_path(file_name, value))
            ws = wb[sheet_name]
            for pivot in ws._pivots:
                pivot.cache.refreshOnLoad = True
            wb.save(self.get_output_path(file_name, value))

            # Filter the data in the cache
            replace_pivot_cache_with_subset(ws, self.field, value)

            wb.save(self.get_output_path(file_name, value))

    def split_excel(self, file_name: str, batch_size=10000):
        import pandas as pd
        from openpyxl import load_workbook

        file_path = os.path.join(self.source_path, file_name)
        sheet_names = pd.ExcelFile(file_path, engine='openpyxl').sheet_names
        original_wb = load_workbook(file_path, data_only=True)
        pivot = False
        pivot_sheet_name = None
        for sheet_name in sheet_names:
            ws = original_wb[sheet_name]
            if len(ws._pivots) > 0:
                pivot = True
                pivot_sheet_name = sheet_name
                break

        if pivot:
            logger.info(f"Splitting excel pivot {file_name}")
            self.split_excel_pivot(file_name=file_name, file_path=file_path, sheet_name=pivot_sheet_name)
        else:
            workbook_handles = {}
            for sheet_name in sheet_names:
                logger.info(f"Splitting excel file without pivots {file_path}")
                self.split_excel_table(file_path=file_path, sheet_name=sheet_name, workbook_handles=workbook_handles,
                                       batch_size=batch_size)
                for value, wb in workbook_handles.items():
                    if value is not None:
                        os.makedirs(os.path.join(self.output_path, value), exist_ok=True)
                        output_file_path = self.get_output_path(field_value=value, file=file_name)
                        wb.save(output_file_path)

    def split_excel_table(self, file_path: str, sheet_name: str, workbook_handles, batch_size=10000):
        import pandas as pd
        from openpyxl import Workbook
        from openpyxl.utils.dataframe import dataframe_to_rows

        # Read the header separately
        header = pd.read_excel(file_path, sheet_name=sheet_name, nrows=0).columns.tolist()

        # Get the total number of rows in the sheet
        total_rows = pd.read_excel(file_path, sheet_name=sheet_name).shape[0]

        # Read the sheet in chunks
        for start_row in range(0, total_rows, batch_size):
            chunk = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=range(1, start_row + 1),
                                  nrows=batch_size, header=None, names=header)
            for row in dataframe_to_rows(chunk, index=False, header=False):

                # Skip if this is a sheet without the field present as a column
                # Usually this is a notes page, chart or similar
                if self.field not in chunk.columns:
                    continue

                value = row[chunk.columns.get_loc(self.field)]

                # Don't include a header row
                if self.field == value:
                    continue

                # If we don't already have a workbook for this value, we
                # create one and populate it
                if value not in workbook_handles:
                    workbook_handles[value] = Workbook()
                    # Remove the default sheet created by Workbook()
                    default_sheet = workbook_handles[value].active
                    workbook_handles[value].remove(default_sheet)
                if sheet_name not in workbook_handles[value].sheetnames:
                    ws = workbook_handles[value].create_sheet(title=sheet_name)
                    ws.append(header)  # Write header
                ws = workbook_handles[value][sheet_name]
                ws.append(row)
