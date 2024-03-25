from typing import List

from mario.data_extractor import Configuration
from mario.dataset_specification import DatasetSpecification
from mario.metadata import Metadata
from mario.query_builder import QueryBuilder


class MockQueryBuilder(QueryBuilder):

    def __init__(self,
                 configuration: Configuration,
                 metadata: Metadata,
                 dataset_specification: DatasetSpecification
                 ):
        self.sql = 'SELECT * FROM (SELECT TOP 10 name FROM sys.tables) as tmp'

    def create_query(self) -> [str, List[any]]:
        return [self.sql, []]
