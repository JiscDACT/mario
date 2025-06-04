import logging
from copy import copy
from typing import List

from pypika import Table, Criterion, PostgreSQLQuery as Query, functions as fn, Parameter

from mario.data_extractor import Configuration
from mario.dataset_specification import DatasetSpecification
from mario.metadata import Metadata

logger = logging.getLogger(__name__)


def get_formatted_query(query, params):
    formatted_query = copy(query)
    if len(params) > 0:
        for key, value in params.items():
            formatted_query = formatted_query.replace(f'%({key})s', f"'{value}'")
    return formatted_query


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

    def create_totals_query(self, measure=None) -> [str, List[any]]:
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

    def create_totals_query(self, measure=None) -> [str, List[any]]:
        if measure is None:
            _sql = f'SELECT COUNT(*) FROM "' + self.configuration.schema + '"."' + self.configuration.view + '"'
        else:
            _sql = f'SELECT SUM("'+measure+'") FROM "' + self.configuration.schema + '"."' + self.configuration.view + '"'

        _params = []
        return [_sql, _params]

    def create_query(self) -> [str, List[any]]:
        _sql = 'SELECT * FROM "' + self.configuration.schema + '"."' + self.configuration.view + '"'
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
        self.onward_use_category = self.dataset_specification.get_property('onwardUseCategory')
        logger.debug("Query builder using table " + str(self.table))

    def contains_subject(self) -> bool:
        for item in self.dataset_specification.items:
            meta = self.metadata.get_metadata(item)
            if meta.get_property('groups') is not None:
                if 'subject' in meta.get_property('groups'):
                    return True
        return False

    def create_totals_query(self, measure=None) -> [str, List[any]]:
        """
        Constructs a 'total only' query by combining the selections and constraints.
        :return: an array containing the query prepared statement, and the parameters
        TODO make this more generic
        """
        measures = []
        if measure is None:
            for measure in self.dataset_specification.measures:
                # Decide column name based on whether 'subject' fields are present
                if measure in ['FPE', 'FTE'] and not self.contains_subject():
                    measures.append(fn.Sum(self.table[measure], 'Count'))
                else:
                    measures.append(fn.Sum(self.table[measure], measure))
        else:
            measures.append(fn.Sum(self.table[measure], measure))

        q = Query().from_(self.table).select(*measures)

        q, parameters = self.create_constraints(q)
        return [q.get_sql(), parameters]

    def create_query(self) -> [str, List[any]]:
        """
        Constructs the query by combining the selections and constraints.
        :return: an array containing the query prepared statement, and the parameters
        """

        select_fields = []
        for field in self.dataset_specification.items:
            # Don't include calculated fields
            meta = self.metadata.get_metadata(field)
            if not meta.get_property('formula'):
                select_fields.append(field)
        group_fields = select_fields.copy()

        # remove measures from regular select
        measures = []
        for measure in self.dataset_specification.measures:
            if measure in select_fields:
                select_fields.remove(measure)
                group_fields.remove(measure)
                # Decide column name based on whether 'subject' fields are present
                if measure in ['FPE', 'FTE'] and not self.contains_subject():
                    measures.append(fn.Sum(self.table[measure], 'Count'))
                else:
                    measures.append(fn.Sum(self.table[measure], measure))
        select_fields.extend(measures)

        q = Query(). \
            from_(self.table). \
            select(*select_fields). \
            groupby(*group_fields)

        q, parameters = self.create_constraints(q)
        return [q.get_sql(), parameters]

    def create_constraints(self, q):
        """
        Builds a set of constraints from the query; this consists of both
        defined constraints, and the implicit constraint on years. Uses the
        'in' method for determining constraint syntax
        :param q: the Query object
        :return: the Query object with constraints
        """
        clauses = []
        parameters = {}
        for constraint in self.dataset_specification.constraints:
            column = constraint.item
            placeholders = []
            for i in range(len(constraint.allowed_values)):
                parameter_name = column.replace(" ", "_") + str(i)
                # Postgres style.
                # TODO Probably need some way of identifying which parameter style to apply.
                parameter = Parameter('%('+parameter_name+')s')
                placeholders.append(parameter)
                parameters[parameter_name] = constraint.allowed_values[i]
            clauses.append(self.table[column].isin(placeholders))

        q = q.where(Criterion.all(clauses))
        return q, parameters
