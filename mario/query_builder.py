import logging
from typing import List

from pypika import Table, Criterion, PostgreSQLQuery as Query, functions as fn, Parameter

from mario.data_extractor import Configuration
from mario.dataset_specification import DatasetSpecification
from mario.metadata import Metadata

logger = logging.getLogger(__name__)


class QueryBuilder:
    """
    Generic interface for a query builder. Subclass this to create your own custom
    query builders to pass to a DataExtractor in a Configuration.
    """
    def __init__(self,
                 configuration: Configuration,
                 metadata: Metadata,
                 dataset_specification: DatasetSpecification):
        self.configuration = configuration
        self.metadata = metadata
        self.dataset_specification = dataset_specification

    def create_query(self) -> [str, List[any]]:
        raise NotImplementedError


class ViewBasedQueryBuilder(QueryBuilder):
    """
    Creates a very simple 'get everything from the view' query
    """
    def __init__(self,
                 dataset_specification: DatasetSpecification,
                 configuration: Configuration,
                 metadata: Metadata):
        super().__init__(
            configuration=configuration,
            dataset_specification=dataset_specification,
            metadata=metadata
        )

    def create_query(self) -> [str, List[any]]:
        _sql = 'SELECT * FROM [' + self.configuration.schema + '].[' + self.configuration.view + "] WITH(NOLOCK)"
        _params = []
        return [_sql, _params]


class SubsetQueryBuilder(QueryBuilder):
    """
    Class for building SQL queries as selections from a view with aggregation
    """
    def __init__(self,
                 dataset_specification: DatasetSpecification,
                 configuration: Configuration,
                 metadata: Metadata):
        super().__init__(
            configuration=configuration,
            dataset_specification=dataset_specification,
            metadata=metadata
        )
        self.table = Table(self.configuration.view, schema=self.configuration.schema)
        self.years = self.dataset_specification.get_property('years')
        self.parameters = []
        self.onward_use_category = self.dataset_specification.get_property('onwardUseCategory')
        logger.debug("Query builder using table " + str(self.table))

    def contains_subject(self) -> bool:
        for item in self.dataset_specification.items:
            meta = self.metadata.get_metadata(item)
            if meta.get_property('groups') is not None:
                if 'subject' in meta.get_property('groups'):
                    return True
        return False

    def create_query(self) -> [str, List[any]]:
        """
        Constructs the query by combining the selections and constraints.
        :return: an array containing the query prepared statement, and the parameters
        """

        select_fields = self.dataset_specification.items.copy()
        group_fields = select_fields.copy()

        # remove measures from regular select
        for measure in self.dataset_specification.measures:
            group_fields.remove(measure)
            select_fields.remove(measure)
            # Decide column name based on whether 'subject' fields are present
            if measure in ['FPE', 'FTE'] and not self.contains_subject():
                select_fields.append(fn.Sum(self.table[measure], 'Count'))
            else:
                select_fields.append(fn.Sum(self.table[measure], measure))

        q = Query(). \
            from_(self.table). \
            select(*select_fields). \
            groupby(*group_fields)

        q = self.create_constraints(q)
        return [q.get_sql(), self.parameters]

    def create_constraints(self, q):
        """
        Builds a set of constraints from the query; this consists of both
        defined constraints, and the implicit constraint on years. Uses the
        'in' method for determining constraint syntax
        :param q: the Query object
        :return: the Query object with constraints
        """
        clauses = []
        for constraint in self.dataset_specification.constraints:
            column = constraint.item
            self.parameters.extend(constraint.allowed_values)
            placeholders = []
            for i in range(len(constraint.allowed_values)):
                placeholders.append(Parameter('?'))
            clauses.append(self.table[column].isin(placeholders))

        q = q.where(Criterion.all(clauses))
        return q
