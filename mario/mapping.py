from typing import List

import pandas as pd
import csv
import os
import tempfile

from mario.utils import to_snake_case


class FieldMapping:
    """
    Utility class for holding a set of logical-to-physical item mappings
    """

    def __init__(self, query_format, items: List[str]):
        self._format = query_format

        self.as_physical = {}
        self.as_logical = {}

        for item in items:
            self.as_physical[item] = self.map_item(item)
            self.as_logical[self.map_item(item)] = item

    def map_item(self, item):
        if self._format is None:
            return item
        if self._format == 'snake_case':
            return to_snake_case(item)

    def df_to_logical(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renames the columns in the dataframe from their physical to their logical names
        :param df: pandas Dataframe
        :return: pandas Dataframe
        """
        return df.rename(columns=self.as_logical)


def rewrite_csv_header_with_fieldmapping(local_path, field_mapping):
    """
    Replace the first-line header in a CSV using field_mapping, but stream
    all remaining lines without loading the file into memory.
    """
    # Create a temp file in the same directory (safer for atomic replace)
    dir_name = os.path.dirname(local_path)
    fd, temp_path = tempfile.mkstemp(dir=dir_name)
    os.close(fd)

    with open(local_path, "r", encoding="utf-8", newline="") as src, \
         open(temp_path, "w", encoding="utf-8", newline="") as dst:

        reader = csv.reader(src)
        writer = csv.writer(dst, quoting=csv.QUOTE_MINIMAL)

        # --- Read & rewrite only the first line ---
        physical_header = next(reader)
        logical_header = [
            field_mapping.as_logical.get(col, col)
            for col in physical_header
        ]
        writer.writerow(logical_header)

        # --- Stream the rest unchanged ---
        for row in reader:
            writer.writerow(row)

    # Atomic replace of original file
    os.replace(temp_path, local_path)
