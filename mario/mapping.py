from typing import List

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
