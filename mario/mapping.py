from typing import List

import pandas as pd

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
