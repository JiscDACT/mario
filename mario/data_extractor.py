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
        import pantab
        from tableauhyperapi import TableName
        from mario.hyper_utils import get_default_table_and_schema
        schema, table = get_default_table_and_schema(hyper_path=self.configuration.file_path)
        table = TableName(schema, table)
        self._data = pantab.frame_from_hyper(
            source=self.configuration.file_path,
            table=table
        )

    def __get_column_name__(self, item: str):
        """ Returns the column name for a metadata item"""
        meta = self.metadata.get_metadata(item)
        if meta.get_property('output_name') is not None:
            return meta.get_property('output_name')
        elif meta.get_property('physical_column_name') is not None:
            return meta.get_property('physical_column_name')
        else:
            return meta.name

    def __minimise_data__(self):
        """ Minimise data so we only keep the columns in the spec """
        columns_to_keep = []
        for item in self.dataset_specification.items:
            if self.metadata.get_metadata(item) and not self.metadata.get_metadata(item).get_property('formula'):
                columns_to_keep.append(self.__get_column_name__(item))
        self._data = self._data[columns_to_keep]

    def __get_measure__(self, measure=None):
        if measure is None:
            if len(self.dataset_specification.measures) > 0:
                measure = self.dataset_specification.measures[0]
        else:
            if measure not in self.dataset_specification.measures:
                raise ValueError(f"Measure {measure} does not exist in dataset specification")
        return measure

    def get_total(self, measure=None):
        df = self.get_data_frame()
        measure = self.__get_measure__(measure)
        if measure is None:
            return len(df)
        self._total = df[measure].sum()
        return self._total

    def validate_data(self, allow_nulls=True):
        from mario.validation import DataFrameValidator
        validator = DataFrameValidator(
            self.dataset_specification,
            self.metadata,
            data=self.get_data_frame()
        )
        return validator.validate_data(allow_nulls)

    def get_data_frame(self, minimise=True) -> DataFrame:
        if self._data is None:
            self.__load__()
        if minimise:
            self.__minimise_data__()
        return self._data

    def save_query(self, file_path: str, formatted: bool = False):
        """
        Output the query used
        :param file_path:
        :param formatted: whether to format parameters inline; defaults to False
        :return:
        """
        from mario.query_builder import get_formatted_query
        if self._query is None:
            self.__build_query__()
        sql = self._query[0]
        if formatted:
            sql = get_formatted_query(self._query[0], self._query[1])
        with open(file_path, mode='w') as file:
            file.write(sql)

    def save_data_as_csv(self, file_path: str, minimise=True, compress_using_gzip=False):
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

    def validate_data(self, allow_nulls=True):
        from mario.validation import HyperValidator
        validator = HyperValidator(
            dataset_specification=self.dataset_specification,
            metadata=self.metadata,
            hyper_file_path=self.configuration.file_path
        )
        return validator.validate_data(allow_nulls)

    def __minimise_data__(self):
        from mario.hyper_utils import get_default_table_and_schema, drop_columns_from_hyper
        columns_to_keep = self.dataset_specification.items
        schema, table = get_default_table_and_schema(self.configuration.file_path)
        drop_columns_from_hyper(
            hyper_file_path=self.configuration.file_path,
            columns_to_keep=columns_to_keep,
            schema=schema,
            table=table
        )

    def get_total(self, measure=None):
        from mario.hyper_utils import get_row_count, get_default_table_and_schema, get_total
        schema, table = get_default_table_and_schema(self.configuration.file_path)
        if measure is None:
            return get_row_count(
                hyper_file_path=self.configuration.file_path,
                schema=schema,
                table=table
            )
        else:
            return get_total(
                hyper_file_path=self.configuration.file_path,
                schema=schema,
                table=table,
                measure=measure
            )

    def save_data_as_hyper(self, file_path: str, table: str = 'Extract', schema: str = 'Extract', minimise=False):
        if minimise:
            self.__minimise_data__()
        shutil.copyfile(self.configuration.file_path, file_path)

    def save_data_as_csv(self,
                         file_path: str,
                         minimise=True,
                         compress_using_gzip=False,
                         chunk_size=100000
                         ):
        from mario.hyper_utils import get_default_table_and_schema, add_row_numbers_to_hyper, get_column_list
        import pantab
        import tempfile
        import shutil
        import os

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_hyper = os.path.join(temp_dir, 'temp.hyper')
            shutil.copyfile(
                src=self.configuration.file_path,
                dst=temp_hyper
            )

            schema, table = get_default_table_and_schema(temp_hyper)

            columns = get_column_list(
                hyper_file_path=temp_hyper,
                schema=schema,
                table=table
            )

            logger.debug("Adding row numbers to hyper so we can guarantee ordering")
            if 'row_number' not in columns:
                add_row_numbers_to_hyper(
                    input_hyper_file_path=temp_hyper,
                    schema=schema,
                    table=table
                )
            else:
                # Remove it so we don't include it in the output
                columns.remove('row_number')

            if compress_using_gzip:
                compression_options = dict(method='gzip')
                file_path = file_path + '.gz'
            elif file_path.endswith('.gz'):
                compression_options = dict(method='gzip')
            else:
                compression_options = None

            mode = 'w'
            header = True
            offset = 0
            column_names = ','.join(f'"{column}"' for column in columns)

            sql = f"SELECT {column_names} FROM \"{schema}\".\"{table}\" ORDER BY row_number"

            while True:
                query = f"{sql} LIMIT {chunk_size} OFFSET {offset}"
                df_chunk = pantab.frame_from_hyper_query(temp_hyper, query)
                if df_chunk.empty:
                    break
                df_chunk.to_csv(file_path, index=False, mode=mode, header=header,
                                compression=compression_options)
                offset += chunk_size
                if header:
                    header = False
                    mode = "a"


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

        if self.configuration.hook is not None:
            return self.configuration.hook.get_conn()

        engine = create_engine(self.configuration.connection_string)
        connection = engine.connect().execution_options(stream_results=True)
        return connection

    def save_data_as_csv(self, file_path: str, minimise=False, compress_using_gzip=False):
        if minimise:
            raise NotImplementedError('Cannot minimise data when using streaming')
        self.stream_sql_to_csv(file_path=file_path, compress_using_gzip=compress_using_gzip)

    def save_data_as_hyper(self, file_path: str, table: str = 'Extract', schema: str = 'Extract', minimise=False):
        if minimise:
            raise NotImplementedError('Cannot minimise data when using streaming')
        self.stream_sql_to_hyper(
            file_path=file_path,
            table=table,
            schema=schema
        )

    def get_total(self, measure=None):
        """
        For totals when streaming data we need to run a totals SQL query separate
        from the main query and use the results of this
        :return: the total value of the query
        """
        logger.info("Building totals query")
        measure = self.__get_measure__(measure)
        if self.configuration.query_builder is not None:
            from mario.query_builder import QueryBuilder
            query_builder: QueryBuilder = self.configuration.query_builder(
                configuration=self.configuration,
                metadata=self.metadata,
                dataset_specification=self.dataset_specification)
            totals_query = query_builder.create_totals_query(measure=measure)
        else:
            raise NotImplementedError

        totals_df = pd.read_sql(totals_query[0], self.get_connection(), params=totals_query[1])
        return totals_df.iat[0, 0]

    def stream_sql_to_hyper(self,
                            file_path: str,
                            table: str = 'Extract',
                            schema: str = 'Extract',
                            validate: bool = False,
                            allow_nulls: bool = True,
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
                          allow_nulls: bool = True,
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


class DataFrameExtractor(DataExtractor):
    """
    Wrapper for a DataExtractor using an existing DataFrame
    """

    def __init__(self,
                 dataset_specification: DatasetSpecification,
                 metadata: Metadata,
                 dataframe: DataFrame
                 ):
        super().__init__(configuration=Configuration(), dataset_specification=dataset_specification, metadata=metadata)
        self._data = dataframe


class PartitioningExtractor(DataExtractor):
    """
    A data extractor that loads from SQL in batches using a specified constraint
    to partition by
    """
    def __init__(self,
                 configuration: Configuration,
                 dataset_specification: DatasetSpecification,
                 metadata: Metadata,
                 partition_column: str
                 ):
        super().__init__(configuration, dataset_specification, metadata)
        self._data = None
        self.partition_column = partition_column

    def get_connection(self):
        from sqlalchemy import create_engine

        if self.configuration.hook is not None:
            return self.configuration.hook.get_conn()

        engine = create_engine(self.configuration.connection_string)
        connection = engine.connect().execution_options(stream_results=True)
        return connection

    def __get_partition_values__(self):
        for constraint in self.dataset_specification.constraints:
            if constraint.item == self.partition_column:
                return constraint.allowed_values

    def __build_query_using_partition__(self, partition_value):
        logger.info(f"Building query for partition {self.partition_column} with value {partition_value}")
        from copy import deepcopy
        specification = deepcopy(self.dataset_specification)

        partition_found = False
        for constraint in specification.constraints:
            if constraint.item == self.partition_column:
                constraint.allowed_values = [partition_value]
                partition_found = True

        if not partition_found:
            raise ValueError("Partition not found in constraints")

        if self.configuration.query_builder is not None:
            from mario.query_builder import QueryBuilder
            query_builder: QueryBuilder = self.configuration.query_builder(
                configuration=self.configuration,
                metadata=self.metadata,
                dataset_specification=specification)
            query = query_builder.create_query()
        else:
            raise NotImplementedError

        return query

    def __load_from_sql_using_partition__(self, partition_value):
        logger.info("Executing query")

        query = self.__build_query_using_partition__(partition_value=partition_value)
        from sqlalchemy import create_engine
        engine = create_engine(self.configuration.connection_string)
        if self._data is None:
            self._data = pd.read_sql(sql=query[0], con=engine.connect(), params=query[1])
        else:
            df = pd.read_sql(sql=query[0], con=engine.connect(), params=query[1])
            self._data = pd.concat([self._data, df], axis=0)

    def __load_from_sql__(self):
        for value in self.__get_partition_values__():
            self.__load_from_sql_using_partition__(partition_value=value)

    def get_data_frame(self, minimise=True) -> DataFrame:
        raise NotImplementedError()

    def get_total(self, measure=None):
        raise NotImplementedError()

    def save_data_as_hyper(self,
                           file_path: str,
                           table: str = 'Extract',
                           schema: str = 'Extract',
                           minimise=True):
        self.stream_sql_to_hyper(file_path=file_path, table=table, schema=schema, minimise=minimise)

    def save_data_as_csv(self, file_path: str, minimise=True, compress_using_gzip=False):
        self.stream_sql_to_csv(
            file_path=file_path,
            minimise=minimise,
            compress_using_gzip=compress_using_gzip
        )

    def stream_sql_to_csv(self,
                          file_path,
                          validate: bool = False,
                          allow_nulls: bool = True,
                          chunk_size: int = 100000,
                          compress_using_gzip: bool = False,
                          minimise: bool = False,
                          include_row_numbers: bool = False
                          ):
        """
        Write From SQL to CSV using streaming. No data is held in memory
        apart from chunks of rows as they are read.
        Optionally, data can be validated as it is read.
        """
        from mario.query_builder import get_formatted_query
        logger.info("Executing query")
        connection = self.get_connection()

        if compress_using_gzip:
            compression_options = dict(method='gzip')
            file_path = file_path + '.gz'
        else:
            compression_options = None

        mode = 'w'
        header = True

        row_counter = 0  # Initialize global row counter

        for partition_value in self.__get_partition_values__():
            query = self.__build_query_using_partition__(partition_value=partition_value)
            for df in pd.read_sql(get_formatted_query(query[0],query[1]), connection, chunksize=chunk_size):
                if validate or minimise:
                    self._data = df
                    if validate:
                        self.validate_data(allow_nulls=allow_nulls)
                    if minimise:
                        self.__minimise_data__()
                        df = self._data
                if include_row_numbers:
                    df['row_number'] = range(row_counter, row_counter + len(df))
                    row_counter += len(df)  # Update the counter
                df.to_csv(file_path, mode=mode, header=header, index=False, compression=compression_options)
                if header:
                    header = False
                    mode = "a"

        return file_path

    def stream_sql_to_hyper(self,
                            file_path: str,
                            table: str = 'Extract',
                            schema: str = 'Extract',
                            validate: bool = False,
                            allow_nulls: bool = True,
                            minimise: bool = False,
                            chunk_size: int = 100000,
                            include_row_numbers: bool = False
                            ):
        """
        Write From SQL to .hyper using streaming. No data is held in memory
        apart from chunks of rows as they are read.
        Optionally, data can be validated as it is read.
        """
        self.__build_query__()
        logger.info("Executing query")
        from tableauhyperapi import TableName
        from pantab import frame_to_hyper
        from mario.query_builder import get_formatted_query

        connection = self.get_connection()
        table_name = TableName(schema, table)

        row_counter = 0  # Initialize global row counter

        for partition_value in self.__get_partition_values__():
            query = self.__build_query_using_partition__(partition_value=partition_value)
            for df in pd.read_sql(get_formatted_query(query[0], query[1]), connection, chunksize=chunk_size):
                if validate or minimise:
                    self._data = df
                    if validate:
                        self.validate_data(allow_nulls=allow_nulls)
                    if minimise:
                        self.__minimise_data__()
                        df = self._data
                if include_row_numbers:
                    df['row_number'] = range(row_counter, row_counter + len(df))
                    row_counter += len(df)  # Update the counter
                if len(df) == 0:
                    logger.warning(f"No rows found for partition with value '{partition_value}'")
                else:
                    logger.info(f"Saving {chunk_size} rows to file")
                    frame_to_hyper(df, database=file_path, table=table_name, table_mode='a')


