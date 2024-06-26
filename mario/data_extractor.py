import logging
import shutil
import tempfile

import pandas as pd
from pandas import DataFrame

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
    * A QueryBuilder class: instantiate and use to build a SQL query
    """

    def __init__(self,
                 connection_string: str = None,
                 hook=None,
                 view: str = None,
                 schema: str = None,
                 file_path: str = None,
                 query_builder=None,
                 user: str = None
                 ):
        self.connection_string = connection_string
        self.hook = hook
        self.view = view
        self.schema = schema
        self.file_path = file_path
        self.query_builder = query_builder
        self.user = user


class DataExtractor:

    def __init__(self,
                 configuration: Configuration,
                 dataset_specification: DatasetSpecification,
                 metadata: Metadata):
        self.configuration = configuration
        self.dataset_specification = dataset_specification
        self.metadata = metadata
        self._data = None
        self._query = None
        self._total = 0

    def __load__(self):
        if self.configuration is not None:
            if self.configuration.file_path is not None:
                if self.configuration.file_path.endswith('.csv'):
                    self.__load_from_csv__()
                elif self.configuration.file_path.endswith('.hyper'):
                    self.__load_from_hyper__()
                else:
                    raise ValueError("Unsupported file type")
            elif self.configuration.hook is not None:
                self.__load_from_hook__()
            else:
                self.__load_from_sql__()

    def __load_from_hook__(self):
        self.__build_query__()
        logger.info("Executing query using hook")
        self._data = self.configuration.hook.get_pandas_df(sql=self._query[0], parameters=self._query[1])

    def __load_from_sql__(self):
        self.__build_query__()
        logger.info("Executing query")
        from sqlalchemy import create_engine
        engine = create_engine(self.configuration.connection_string)
        self._data = pd.read_sql(sql=self._query[0], con=engine.connect(), params=self._query[1])

    def __build_query__(self):
        logger.info("Building query")
        self._query = ''
        if self.configuration.query_builder is not None:
            from mario.query_builder import QueryBuilder
            query_builder: QueryBuilder = self.configuration.query_builder(
                configuration=self.configuration,
                metadata=self.metadata,
                dataset_specification=self.dataset_specification)
            self._query = query_builder.create_query()
        else:
            raise NotImplementedError

    def __load_from_csv__(self):
        self._data = pd.read_csv(self.configuration.file_path)

    def __load_from_hyper__(self):
        from tableau_builder import hyper_utils
        import pantab
        from tableauhyperapi import TableName
        table_parts = hyper_utils.get_default_table_and_schema(hyper_path=self.configuration.file_path)
        table = TableName(table_parts['schema'], table_parts['table'])
        self._data = pantab.frame_from_hyper(
            source=self.configuration.file_path,
            table=table
        )

    def __get_column_name__(self, item: str):
        """ Returns the column name for a metadata item"""
        meta = self.metadata.get_metadata(item)
        if meta.get_property('output_name') is not None:
            return meta.get_property('output_name')
        else:
            return meta.name

    def __minimise_data__(self):
        """ Minimise data so we only keep the columns in the spec """
        columns_to_keep = []
        for item in self.dataset_specification.items:
            if self.metadata.get_metadata(item) and not self.metadata.get_metadata(item).get_property('formula'):
                columns_to_keep.append(self.__get_column_name__(item))
        self._data = self._data[columns_to_keep]

    def get_total(self):
        df = self.get_data_frame()
        self._total = df[self.dataset_specification.measures[0]].sum()
        return self._total

    def validate_data(self, allow_nulls=True):
        data = self.get_data_frame()
        validation_errors = []

        # Check for NULLs
        if allow_nulls is not True:
            if len(data.loc[data.isna().any(axis=1)]) > 0:
                validation_errors.append("Dataset contains NULLs")
                logger.warning("Dataset contains NULLs")

        for item in self.dataset_specification.items:
            metadata = self.metadata.get_metadata(item)
            column = self.__get_column_name__(item)
            # Ignore calculated fields
            if metadata.get_property('formula') is None:
                # Check all columns present
                if column not in data.columns:
                    validation_errors.append("Item missing: " + item)
                else:
                    # Check domain
                    if metadata.get_property('domain') is not None:
                        data_domain = data[column].unique()
                        metadata_domain = metadata.get_property('domain')
                        for element in data_domain:
                            if element not in metadata_domain:
                                validation_errors.append("Domain validation failed for " + item)
                                logger.error("Validation error: '" + str(element) + "' is not in domain of " + item)
                        for metadata_element in metadata_domain:
                            if metadata_element not in data_domain:
                                logger.warning(f"Warning: {str(metadata_element)} is not present in the data for {item}")

                    # Check range
                    if metadata.get_property('range') is not None:
                        min_value = metadata.get_property('range')[0]
                        max_value = metadata.get_property('range')[1]
                        data_min = data[column].min()
                        data_max = data[column].max()
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

    def get_data_frame(self, minimise=True) -> DataFrame:
        if self._data is None:
            self.__load__()
        if minimise:
            self.__minimise_data__()
        return self._data

    def save_query(self, file_path: str):
        """
        Output the query used
        :param file_path:
        :return:
        """
        if self._query is None:
            self.__build_query__()
        with open(file_path, mode='w') as file:
            file.write(self._query[0])

    def save_data_as_csv(self, file_path: str, minimise=True):
        if self._data is None:
            self.__load__()
        if minimise:
            self.__minimise_data__()
        self._data.to_csv(file_path)

    def save_data_as_hyper(self, file_path: str, table: str = 'Extract', schema: str = 'Extract', minimise=True):
        import pantab
        from tableauhyperapi import TableName
        if self._data is None:
            self.__load__()
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
        super().__init__(Configuration(), dataset_specification, metadata)
        self.configuration = configuration

    def check_type(self, column_name: str, expected_type: str, table_name: str, schema_name: str):
        from tableau_builder.hyper_utils import get_table
        table = get_table(hyper_path=self.configuration.file_path, table_name=table_name, schema_name=schema_name)
        column = table.get_column_by_name(column_name)
        if expected_type is None:
            expected_type = 'text'
        if str(column.type).lower() == expected_type.lower():
            return True
        elif str(column.type).lower() == 'big_int' and expected_type.lower() == 'int':
            logger.warning(f"Validation warning: {column_name} is big_int rather than int")
            return True
        else:
            logger.error("Validation error: '" + str(
                column.type).lower() + "' is not the expected type (" + expected_type + ") for " + column_name)
            return False

    def validate_data(self, allow_nulls=False):
        from tableau_builder.hyper_utils import get_default_table_and_schema, check_column_exists, \
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
                    if metadata.get_property('datatype') and not self.check_type(
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


class StreamingDataExtractor(DataExtractor):
    """
    Extension to basic data extractor with additional functions
    supporting streaming data from SQL to output formats without
    holding any data in memory using a data frame
    """
    def __init__(self,
                 configuration: Configuration,
                 dataset_specification: DatasetSpecification,
                 metadata: Metadata
                 ):
        super().__init__(configuration, dataset_specification, metadata)
        self._data = None

    def get_data_frame(self, minimise=True) -> DataFrame:
        if self._data is None:
            raise NotImplementedError("Dataframe is not available when using a streaming extractor")
        else:
            return super().get_data_frame(minimise=minimise)

    def validate_data(self, allow_nulls=True):
        if self._data is None:
            logger.warning("Calling validate_data() on a streaming extractor causes the data"
                           "to be re-streamed from source into a temporary file. If this is "
                           "not what you had in mind, first stream the data to file, and "
                           "then load the file in a DataExtractor")
            with tempfile.TemporaryFile() as file:
                self.stream_sql_to_hyper(file_path=file.name, validate=True, allow_nulls=allow_nulls)
        else:
            super().validate_data(allow_nulls=allow_nulls)

    def get_connection(self):
        from sqlalchemy import create_engine
        engine = create_engine(self.configuration.connection_string)
        connection = engine.connect().execution_options(stream_results=True)
        return connection

    def save_data_as_csv(self, file_path: str, minimise=False):
        if minimise:
            raise NotImplementedError('Cannot minimise data when using streaming')
        self.stream_sql_to_csv(file_path=file_path)

    def save_data_as_hyper(self, file_path: str, table: str = 'Extract', schema: str = 'Extract', minimise=False):
        if minimise:
            raise NotImplementedError('Cannot minimise data when using streaming')
        self.stream_sql_to_hyper(
            file_path=file_path,
            table=table,
            schema=schema
        )

    def get_total(self):
        """
        For totals when streaming data we need to run a totals SQL query separate
        from the main query and use the results of this
        :return: the total value of the query
        """
        logger.info("Building totals query")
        if self.configuration.query_builder is not None:
            from mario.query_builder import QueryBuilder
            query_builder: QueryBuilder = self.configuration.query_builder(
                configuration=self.configuration,
                metadata=self.metadata,
                dataset_specification=self.dataset_specification)
            totals_query = query_builder.create_totals_query()
        else:
            raise NotImplementedError

        totals_df = pd.read_sql(totals_query[0], self.get_connection(), params=totals_query[1])
        return totals_df.iat[0, 0]

    def stream_sql_to_hyper(self,
                            file_path: str,
                            table: str = 'Extract',
                            schema: str = 'Extract',
                            validate: bool = False,
                            allow_nulls: bool = False,
                            minimise: bool = False,
                            chunk_size: int = 100000):
        """
        Write From SQL to .hyper using streaming. No data is held in memory
        apart from chunks of rows as they are read.
        Optionally, data can be validated as it is read.
        """
        self.__build_query__()
        logger.info("Executing query")
        from tableauhyperapi import TableName
        from pantab import frame_to_hyper

        connection = self.get_connection()
        table_name = TableName(schema, table)
        for df in pd.read_sql(self._query[0], connection, chunksize=chunk_size):
            if validate or minimise:
                self._data = df
                if validate:
                    self.validate_data(allow_nulls=allow_nulls)
                if minimise:
                    self.__minimise_data__()
                    df = self._data
            frame_to_hyper(df, database=file_path, table=table_name, table_mode='a')

    def stream_sql_to_csv(self,
                          file_path,
                          validate: bool = False,
                          allow_nulls: bool = False,
                          chunk_size: int = 100000,
                          compress_using_gzip: bool = False,
                          minimise: bool = False
                          ):
        """
        Write From SQL to CSV using streaming. No data is held in memory
        apart from chunks of rows as they are read.
        Optionally, data can be validated as it is read.
        """
        self.__build_query__()
        logger.info("Executing query")
        connection = self.get_connection()

        if compress_using_gzip:
            compression_options = dict(method='gzip')
            file_path = file_path + '.gz'
        else:
            compression_options = None

        mode = 'w'
        header = True
        for df in pd.read_sql(self._query[0], connection, chunksize=chunk_size):
            if validate or minimise:
                self._data = df
                if validate:
                    self.validate_data(allow_nulls=allow_nulls)
                if minimise:
                    self.__minimise_data__()
                    df = self._data
            df.to_csv(file_path, mode=mode, header=header, index=False, compression=compression_options)
            if header:
                header = False
                mode = "a"

        return file_path

    def stream_sql_to_csv_using_bcp(self,
                                    table_name: str,
                                    output_file_path: str,
                                    server_url: str,
                                    database_name: str,
                                    use_view=False,
                                    keep_existing_table=False,
                                    delete_when_finished=False):
        """
        Builds table on server, then extracts it to CSV using the bcp program. No data is held in
        memory. Data cannot be validated without reading it in from the generated CSV.
        TODO Note that the generated SQL must start with 'SELECT * FROM' - make this more generic
        TODO Provide an option to not drop/create table if it already exists.
        """
        self.__build_query__()
        logger.info("Executing query")
        from sqlalchemy import text
        import subprocess

        # Get a connection
        connection = self.get_connection()

        table_or_view = 'TABLE' if not use_view else 'VIEW'

        # Drop the table if it already exists
        drop_table_sql = f'DROP {table_or_view} IF EXISTS {self.configuration.schema}.{table_name};'
        connection.execute(text(drop_table_sql))
        connection.commit()
        logger.debug(f'drop {table_or_view}: {drop_table_sql}')

        # Create a table or view using SQL
        sql = self._query[0]
        if use_view:
            create_table_sql = f'CREATE VIEW dbo.{table_name} AS ' + sql
        else:
            create_table_sql = sql.replace('SELECT * FROM (', f'SELECT * INTO dbo.{table_name} FROM (')
        connection.execute(text(create_table_sql))
        connection.commit()
        logger.debug(f'create {table_or_view}: {create_table_sql}')

        # SQL to obtain the column names from the table
        bcp_columns_sql = f'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'{table_name}\''
        bcp_columns = [r[0] for r in connection.execute(text(bcp_columns_sql)).fetchall()]
        bcp_columns_literal = ', '.join(["'" + a + "'" for a in bcp_columns])
        bcp_columns_select = ["CHAR(34)+CAST([" + a + "] AS VARCHAR(max))+CHAR(34)" for a in bcp_columns]

        # Union the column names with the extract to get the complete query
        bcp_select = f"SELECT {bcp_columns_literal} UNION ALL SELECT {','.join(bcp_columns_select)} FROM [dbo].{table_name}"

        # Export to CSV
        user = self.configuration.user
        bcp_code = f'bcp "{bcp_select}" queryout {output_file_path} -d {database_name} -c -C RAW -U {user} -S {server_url} -G'  # 65001 for utf-8 // RAW for same data
        logger.debug('bcp command: ' + bcp_code)
        subprocess.call(bcp_code)

        # Delete table/view when finished
        if delete_when_finished:
            connection.execute(text(drop_table_sql))
            connection.commit()

        # Close connection when done
        connection.close()