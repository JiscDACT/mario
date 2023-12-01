import logging
import shutil

import pandas as pd
import pantab
from pandas import DataFrame
from tableauhyperapi import TableName

from mario.dataset_specification import DatasetSpecification
from mario.metadata import Metadata

logger = logging.getLogger(__name__)


class Configuration:
    """
    Configuration for a data extractor. This can include:
    * A connection string: an ODBC connection string for SQL extraction
    * A view name: A view as the base for the SQL code (H+, TDSA only)
    * A hook: An object or function used to access data, e.g. S3
    * A file path: A data file to load as the source of data, e.g. data.csv
    """

    def __init__(self,
                 connection_string: str = None,
                 hook=None,
                 view: str = None,
                 file_path: str = None
                 ):
        self.connection_string = connection_string
        self.hook = hook
        self.view = view
        self.file_path = file_path


class DataExtractor:

    def __init__(self,
                 configuration: Configuration,
                 dataset_specification: DatasetSpecification,
                 metadata: Metadata):
        self.configuration = configuration
        self.dataset_specification = dataset_specification
        self.metadata = metadata
        self._data = pd.DataFrame()
        if configuration.file_path is not None:
            if configuration.file_path.endswith('.csv'):
                self.__load_from_csv__()
            elif configuration.file_path.endswith('.hyper'):
                self.__load_from_hyper__()
            else:
                raise ValueError("Unsupported file type")
        elif configuration.hook is not None:
            # TODO something here for Airflow hooks
            pass
        else:
            self.__load_from_sql__()

    def __load_from_sql__(self):
        query = self.__build_query__()
        # TODO connect using ODBC connection string
        # TODO run query using pyodbc
        logger.info("Executing query: " + query)

    def __build_query__(self):
        query = ''
        if self.configuration.view is not None:
            query = 'SELECT * FROM ' + self.configuration.view + ' WITH(NOLOCK)'
        else:
            # TODO: Automation 2.0 or TDSA query builder
            pass
        return query

    def __load_from_csv__(self):
        self._data = pd.read_csv(self.configuration.file_path)

    def __load_from_hyper__(self):
        from tableau_builder import hyper_utils
        table_parts = hyper_utils.get_default_table_and_schema(hyper_path=self.configuration.file_path)
        table = TableName(table_parts['schema'], table_parts['table'])
        self._data = pantab.frame_from_hyper(
            source=self.configuration.file_path,
            table=table
        )

    def __minimise_data__(self):
        """ Minimise data so we only keep the columns in the spec """
        columns_to_keep = []
        for item in self.dataset_specification.items:
            if not self.metadata.get_metadata(item).get_property('formula'):
                columns_to_keep.append(item)
        self._data = self._data[columns_to_keep]

    def validate_data(self):
        validation_errors = []
        for item in self.dataset_specification.items:
            metadata = self.metadata.get_metadata(item)
            # Ignore calculated fields
            if metadata.get_property('formula') is None:
                # Check all columns present
                if item not in self._data.columns:
                    validation_errors.append("Item missing: " + item)
                else:
                    # Check domain
                    if metadata.get_property('domain') is not None:
                        data_domain = self._data[item].unique()
                        metadata_domain = metadata.get_property('domain')
                        for element in data_domain:
                            if element not in metadata_domain:
                                validation_errors.append("Domain validation failed for " + item)
                                logger.error("Validation error: '" + str(element) + "' is not in domain of " + item)
                    # Check range
                    if metadata.get_property('range') is not None:
                        min_value = metadata.get_property('range')[0]
                        max_value = metadata.get_property('range')[1]
                        data_min = self._data[item].min()
                        data_max = self._data[item].max()
                        if data_min < min_value:
                            validation_errors.append("Range validation failed for " + item)
                            logger.error("Validation error: '" + str(data_min) + "' is less than " + str(min_value))
                        if data_max > max_value:
                            validation_errors.append("Range validation failed for " + item)
                            logger.error("Validation error: '" + str(data_max) + "' is greater than " + str(max_value))

        if len(validation_errors) != 0:
            for validation_error in validation_errors:
                logger.error(validation_error)
            raise ValueError("Data validation failed - check the logs for details")
        return True

    def get_data_frame(self) -> DataFrame:
        return self._data

    def save_data_as_excel(self, file_path: str, minimise=True):
        if minimise:
            self.__minimise_data__()
        # TODO use OpenPyxl here, or drop this method
        self._data.to_csv(file_path)
        pass

    def save_data_as_csv(self, file_path: str, minimise=True):
        if minimise:
            self.__minimise_data__()
        self._data.to_csv(file_path)

    def save_data_as_hyper(self, file_path: str, table: str = 'Extract', schema: str = 'Extract', minimise=True):
        table_name = TableName(schema, table)
        if minimise:
            self.__minimise_data__()
        pantab.frame_to_hyper(df=self._data, database=file_path, table=table_name)


class HyperFile(DataExtractor):
    """
    Wrapper for a HyperFile as a data extractor - use when no data needs to be extracted,
    and we just want to treat a hyper as a hyper with no conversion to/from dataframe
    """

    def __init__(self,
                 configuration: Configuration,
                 dataset_specification: DatasetSpecification,
                 metadata: Metadata):
        super().__init__(configuration, dataset_specification, metadata)

    def validate_data(self):
        from tableau_builder.hyper_utils import get_default_table_and_schema, check_column_exists, check_type, \
            check_domain, check_range

        validation_errors = []

        table_schema = get_default_table_and_schema(self.configuration.file_path)
        table = table_schema['table']
        schema = table_schema['schema']

        for item in self.dataset_specification.items:
            metadata = self.metadata.get_metadata(item)
            if metadata.get_property('formula') is None:
                if not check_column_exists(
                        hyper_path=self.configuration.file_path,
                        column_name=item,
                        table_name=table,
                        schema_name=schema
                ):
                    validation_errors.append("Item missing: " + item)
                else:
                    if metadata.get_property('datatype') and not check_type(
                            hyper_path=self.configuration.file_path,
                            column_name=item,
                            expected_type=metadata.get_property('datatype'),
                            table_name=table,
                            schema_name=schema
                    ):
                        validation_errors.append("Item is wrong type: " + item)
                    if metadata.get_property('domain') is not None:
                        if not check_domain(
                                hyper_path=self.configuration.file_path,
                                field=item,
                                domain=metadata.get_property('domain'),
                                table_name=table,
                                schema_name=schema
                        ):
                            validation_errors.append("Domain validation failed for " + item)
                    if metadata.get_property('range') is not None:
                        if not check_range(
                                hyper_path=self.configuration.file_path,
                                field=item,
                                min_value=metadata.get_property('range')[0],
                                max_value=metadata.get_property('range')[1],
                                table_name=table,
                                schema_name=schema
                        ):
                            validation_errors.append("Range validation failed for " + item)
        if len(validation_errors) != 0:
            for validation_error in validation_errors:
                logger.error(validation_error)
            raise ValueError("Domain validation failed - check the logs for details")
        return True

    def __minimise_data__(self):
        from tableau_builder import hyper_utils
        columns_to_keep = self.dataset_specification.items
        table = hyper_utils.get_default_table_and_schema(self.configuration.file_path)
        hyper_utils.subset_columns(
            columns_to_keep=columns_to_keep,
            hyper_path=self.configuration.file_path,
            schema_name=table['schema'],
            table_name=table['table']
        )

    def save_data_as_hyper(self, file_path: str, table: str = 'Extract', schema: str = 'Extract', minimise=False):
        if minimise:
            self.__minimise_data__()
        shutil.copyfile(self.configuration.file_path, file_path)
