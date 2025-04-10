import logging
from enum import Enum

from mario.data_extractor import Configuration
from mario.dataset_specification import DatasetSpecification
from mario.metadata import Metadata, Item

logger = logging.getLogger(__name__)


class DataTypes(Enum):
    """
    Base data types, that are mapped onto specific
    types used in Pandas, Hyper, SQL etc
    """
    TEXT = 'text'
    DATE = 'date'
    INT = 'int'
    DOUBLE = 'double'
    DATETIME = 'datetime'
    OBJECT = 'object'


class Validator:
    """
    Base class for validators. Extended for specific data implementations
    such as Pandas Dataframe, Tableau HyperFile etc.
    """

    def __init__(self,
                 dataset_specification: DatasetSpecification,
                 metadata: Metadata
                 ):
        self.dataset_specification = dataset_specification
        self.metadata = metadata
        self.errors = []
        self.warnings = []

    def __get_column_name__(self, item: Item):
        """ Returns the column name for a metadata item"""
        if item.get_property('output_name') is not None:
            return item.get_property('output_name')
        elif item.get_property('physical_column_name') is not None:
            return item.get_property('physical_column_name')
        else:
            return item.name

    def __get_column_data_type__(self, item: Item):
        """ Returns the type of a metadata item from the data"""
        raise NotImplementedError()

    def __get_column_values__(self, item: Item):
        """ Returns an array of the unique values in the data for an item """
        raise NotImplementedError()

    def __get_minimum_maximum_values__(self, item:Item):
        """ Returns the minimum value of an item from the data """
        raise NotImplementedError()

    def __contains_nulls__(self, item: Item):
        """ Returns True if the item has any Null values in the data """
        return None in self.__get_column_values__(item)

    def __get_data_for_hierarchy__(self, name):
        """ Returns the dataframe for a hierarchy """
        raise NotImplementedError()

    def __get_column_with_segmentation__(self, item:Item, segmentation: str):
        """ Returns the data series for an item grouped by the segmentation column specified """
        raise NotImplementedError()

    def __check_hierarchy__(self, name):
        """ Checks whether the named hierarchy conforms to a tree structure """
        # Create a dictionary to store the hierarchy
        hierarchy = {}

        # Get the levels
        levels = self.metadata.get_hierarchy(name)

        # Get the data
        df = self.__get_data_for_hierarchy__(name)

        # Group by all levels except the last one
        grouped = df.groupby(levels[:-1])[levels[-1]].apply(list).reset_index()

        # Check for duplicates at the last level
        for _, row in grouped.iterrows():
            higher_levels = tuple(row[levels[:-1]])
            values = row[levels[-1]]
            for value in values:
                if value not in hierarchy:
                    hierarchy[value] = higher_levels
                elif hierarchy[value] != higher_levels:
                    self.errors.append(f"Inconsistent hierarchy: {value} at level {levels[-1]} is represented in multiple higher level categories {hierarchy[value]} and {higher_levels}.")

    def __check_for_anomalies__(self, series, threshold=1.75):
        """
        Checks a series for anomalies by looking for absolute z-scores above threshold
        :param series: the series to check
        :param threshold:  the threshold level
        :return: True if an anomaly is present
        """
        import numpy as np
        mean = series.mean()
        std = series.std()
        z_scores = (series - mean) / std
        return np.any(np.abs(z_scores) > threshold)

    def check_hierarchies(self):
        """
        Checks the validity of hierarchies, i.e. that they form a tree structure and
        adds any anomalies found to errors/warnings
        """
        for hierarchy in self.metadata.get_hierarchies():
            self.__check_hierarchy__(hierarchy)

    def check_item_for_anomalies(self, item: str, segmentation: str):
        item = self.metadata.get_metadata(item)
        subset = self.__get_column_with_segmentation__(item, segmentation)
        if self.__check_for_anomalies__(subset):
            self.warnings.append(
                f"Validation warning: '{item.name}' has potentially anomalous data when segmented by '{segmentation}'")

    def check_category_anomalies(self, segmentation: str):
        """
        Checks to see if we get anomalies in how categories are split e.g. over time
        and adds them to warnings
        :param segmentation: the field to segment by
        :return: None
        """
        for dimension in self.dataset_specification.dimensions:
            self.check_item_for_anomalies(dimension, segmentation)

    def check_data_type(self, item: Item):
        """ Checks whether the item has the correct data type """
        expected_data_type = item.get_property('datatype')
        if expected_data_type is None:
            return True

        expected_data_type = DataTypes[expected_data_type.upper()]
        actual_data_type = self.__get_column_data_type__(item)

        if actual_data_type == expected_data_type:
            return True

        if actual_data_type == DataTypes.DATETIME and expected_data_type == DataTypes.DATE:
            self.warnings.append(f"Validation warning: '{item.name}': expected {expected_data_type} but was {actual_data_type}")
            return False

        if actual_data_type == DataTypes.OBJECT and expected_data_type == DataTypes.DATE:
            self.warnings.append(f"Validation warning: '{item.name}': expected {expected_data_type} but was {actual_data_type}"
                                 f". This is probably due to a limitation of Pandas not having a native 'Date' type")
            return False

        self.errors.append(f"Validation error: '{item.name}' is of the wrong type - expected {expected_data_type}, actual {actual_data_type}")
        return False

    def check_column_present(self, item: Item):
        """ Checks whether the column for an item is present in the data """
        raise NotImplementedError()

    def check_nulls(self, item: Item, allow_nulls=True):
        """ Checks if there are any null values for an item """
        if self.__contains_nulls__(item):
            if allow_nulls:
                self.warnings.append(f"Validation warning: '{item.name}' contains NULLs")
            else:
                self.errors.append(f"Validation error: '{item.name}' contains NULLs")

    def check_domain(self, item: Item):
        """ Checks that the values for an item conform to the domain in its specification """
        if item.get_property('domain') is not None:
            data_domain = self.__get_column_values__(item)
            metadata_domain = item.get_property('domain')
            for element in data_domain:
                if element not in metadata_domain:
                    self.errors.append(f"Validation error: '{str(element)}' is not in domain of '{item.name}'")
            for metadata_element in metadata_domain:
                if metadata_element not in data_domain:
                    self.warnings.append(f"Validation warning: '{str(metadata_element)}' is in domain of '{item.name}' but not present in the data")

    def check_range(self, item: Item):
        """ Checks whether the values of an item fall within expected range """
        if item.get_property('range') is not None:
            min_value = item.get_property('range')[0]
            max_value = item.get_property('range')[1]
            data_min, data_max = self.__get_minimum_maximum_values__(item)
            if data_min < min_value:
                self.errors.append(f"Validation error: '{item.name}': '{str(data_min)}' is less than '{str(min_value)}'")
            if data_max > max_value:
                self.errors.append(f"Validation error: '{item.name}': '{str(data_max)}' is greater than '{str(max_value)}'")

    def check_pattern_match(self, item: Item):
        """ Checks that the values for an item match the pattern in the specification """
        import re
        if item.get_property('pattern') is not None:
            values = self.__get_column_values__(item)
            pattern = re.compile(item.get_property('pattern'))
            for value in values:
                if value is not None:
                    if not re.match(pattern, str(value)):
                        self.errors.append(f"Validation error: '{item.name}': '{str(value)}' does not match the pattern '{str(item.get_property('pattern'))}'")

    def check_quality_checks(self, item: Item):
        """ Checks whether an item has any quality rules used in validation """
        if item.get_property('formula') is None:
            if item.get_property('range') is None:
                if item.get_property('domain') is None:
                    if item.get_property('pattern') is None:
                        self.warnings.append(f"Validation warning: '{item.name}' has no quality rules.")

    def validate_data_item(self, item, allow_nulls=True):
        metadata = self.metadata.get_metadata(item)
        if self.check_column_present(metadata):
            self.check_nulls(metadata, allow_nulls)
            self.check_domain(metadata)
            self.check_range(metadata)
            self.check_quality_checks(metadata)
            self.check_data_type(metadata)
            self.check_pattern_match(metadata)

    def validate_data(self, allow_nulls=True, check_hierarchies=False, detect_anomalies=False, segmentation=None):
        """
        Performs validation of data
        :param allow_nulls: If True, nulls are shown as warnings rather than errors
        :param check_hierarchies: If True, perform hierarchy consistency check
        :param detect_anomalies: If True, perform anomaly detection
        :param segmentation: Optional - field name to use for anomaly detection
        :return: True if no validation errors are found
        """
        for item in self.dataset_specification.items:
            self.validate_data_item(item, allow_nulls)
        if check_hierarchies:
            self.check_hierarchies()
        if detect_anomalies:
            if segmentation is not None:
                self.check_category_anomalies(segmentation)
            else:
                raise ValueError("A segmentation column, e.g. year, must be supplied to detect anomalies.")

        for warning in self.warnings:
            logger.warning(warning)
        for validation_error in self.errors:
            logger.error(validation_error)
        if len(self.errors) != 0:
            raise ValueError("Data validation failed - check the logs for details")
        return True


class DataFrameValidator(Validator):
    """
    A validator for Pandas Dataframes, or data extractors that
    use a dataframe as their internal representation.
    """
    def __init__(self,
                 dataset_specification: DatasetSpecification,
                 metadata: Metadata,
                 data,
                 ):
        super().__init__(dataset_specification, metadata)
        self.data = data

    def __get_column_data_type__(self, item: Item):
        column = self.__get_column_name__(item)
        data_type = str(self.data[column].dtype).lower()
        if data_type in ['category', 'string']:
            return DataTypes.TEXT
        if data_type in ['float64']:
            return DataTypes.DOUBLE
        if data_type in ['int', 'int64']:
            return DataTypes.INT
        if data_type in ['datetime64', 'datetime64[ns]']:
            return DataTypes.DATETIME
        if data_type in ['object']:
            return DataTypes.OBJECT
        return data_type

    def __get_column_values__(self, item: Item):
        import pandas as pd
        column = self.__get_column_name__(item)
        values = self.data[column].replace({pd.NA: None}).unique()
        return list(values)

    def __get_minimum_maximum_values__(self, item:Item):
        column = self.__get_column_name__(item)
        data_min = self.data[column].min()
        data_max = self.data[column].max()
        return data_min, data_max

    def __get_data_for_hierarchy__(self, name):
        fields = self.metadata.get_hierarchy(name)
        return self.data[fields].drop_duplicates().reset_index(drop=True)

    def check_column_present(self, item: Item):
        if item.get_property('formula') is None:
            column = self.__get_column_name__(item)
            if column not in self.data.columns:
                self.errors.append(f"Validation error: '{item.name}' in specification is missing from dataset")
                return False
            return True
        return False

    def __get_column_with_segmentation__(self, item:Item, segmentation: str):
        column = self.__get_column_name__(item)
        return self.data.groupby(segmentation)[column].nunique()


class HyperValidator(Validator):
    """
    A validator for hyper files
    """

    def __init__(self,
                 dataset_specification: DatasetSpecification,
                 metadata: Metadata,
                 hyper_file_path
                 ):
        super().__init__(dataset_specification, metadata)
        from mario.hyper_utils import get_default_table_and_schema
        self.hyper_file_path = hyper_file_path
        schema, table = get_default_table_and_schema(self.hyper_file_path)
        self.table = table
        self.schema = schema

    def __get_data_for_hierarchy__(self, name):
        from pantab import frame_from_hyper_query
        fields = ', '.join(f'"{s}"' for s in self.metadata.get_hierarchy(name))
        query = f"""
                     SELECT
                         {fields} 
                     FROM "{self.schema}"."{self.table}" 
                     GROUP BY {fields} 
        """
        from tableauhyperapi import HyperProcess, Telemetry, Connection
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, 'test') as hyper:
            with Connection(hyper.endpoint, self.hyper_file_path) as connection:
                results_df = frame_from_hyper_query(connection,query)
        return results_df

    def __get_column_with_segmentation__(self, item:Item, segmentation: str):
        from pantab import frame_from_hyper_query
        column = self.__get_column_name__(item)
        query = f"""
                    SELECT COUNT(DISTINCT "{column}")
                    FROM "{self.schema}"."{self.table}" 
                    GROUP BY "{segmentation}"
        """
        from tableauhyperapi import HyperProcess, Telemetry, Connection
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, 'test') as hyper:
            with Connection(hyper.endpoint, self.hyper_file_path) as connection:
                results_df = frame_from_hyper_query(connection,query)
        return results_df

    def check_column_present(self, item: Item):
        column = self.__get_column_name__(item)
        from mario.hyper_utils import check_column_exists
        if item.get_property('formula') is None:
            if not check_column_exists(
                    hyper_file_path=self.hyper_file_path,
                    column=column,
                    table=self.table,
                    schema=self.schema
            ):
                self.errors.append(f"Validation error: '{item.name}' in specification is missing from dataset")
                return False
            return True
        return False

    def __get_column_data_type__(self, item: Item):
        from mario.hyper_utils import get_table
        from tableauhyperapi import TypeTag
        column = self.__get_column_name__(item)
        table = get_table(hyper_path=self.hyper_file_path, table=self.table, schema=self.schema)
        datatype = table.get_column_by_name(column).type.tag
        if datatype in [TypeTag.TEXT, TypeTag.CHAR]:
            return DataTypes.TEXT
        if datatype in [TypeTag.BIG_INT, TypeTag.INT, TypeTag.SMALL_INT]:
            return DataTypes.INT
        if datatype in [TypeTag.DOUBLE, TypeTag.NUMERIC]:
            return DataTypes.DOUBLE
        if datatype in [TypeTag.DATE]:
            return DataTypes.DATE
        if datatype in [TypeTag.TIMESTAMP, TypeTag.TIMESTAMP_TZ]:
            return DataTypes.DATETIME
        return str(datatype)

    def __get_minimum_maximum_values__(self, item):
        from tableauhyperapi import HyperProcess, Telemetry, Connection
        column = self.__get_column_name__(item)
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, 'test') as hyper:
            with Connection(hyper.endpoint, self.hyper_file_path) as connection:
                with connection.execute_query(
                        'SELECT MIN("' + column + '") FROM "' + self.schema + '"."' + self.table + '"') as result:
                    min_data = [item for row in list(result) for item in row][0]
                with connection.execute_query(
                        'SELECT MAX("' + column + '") FROM "' + self.schema + '"."' + self.table + '"') as result:
                    max_data = [item for row in list(result) for item in row][0]
        return min_data, max_data

    def __get_column_values__(self, item: Item):
        from tableauhyperapi import HyperProcess, Telemetry, Connection
        column = self.__get_column_name__(item)
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, 'test') as hyper:
            with Connection(hyper.endpoint, self.hyper_file_path) as connection:
                with connection.execute_query(
                        'SELECT DISTINCT "' + column + '" FROM "' + self.schema + '"."' + self.table + '"') as result:
                    rows = list(result)
                    hyper_domain = [item for row in rows for item in row]
        return hyper_domain


class SqlValidator(Validator):
    def __init__(self,
                 dataset_specification: DatasetSpecification,
                 metadata: Metadata,
                 configuration: Configuration
                 ):
        super().__init__(dataset_specification, metadata)
        self.connection = self.__get_connection__(configuration.connection_string)
        self.schema = configuration.schema
        self.view = configuration.view

    def __get_connection__(self, connection_string: str):
        from sqlalchemy import create_engine
        engine = create_engine(connection_string)
        connection = engine.connect().execution_options(stream_results=True)
        return connection

    def __get_column_with_segmentation__(self, item: Item, segmentation: str):
        import pandas as pd
        column = self.__get_column_name__(item)
        sql = f"""
                    SELECT COUNT(DISTINCT "{column}")
                    FROM "{self.schema}"."{self.view}" 
                    GROUP BY "{segmentation}"
        """
        return pd.read_sql(sql, self.connection)

    def __get_column_data_type__(self, item: Item):
        import pandas as pd
        column = self.__get_column_name__(item)
        sql = f"SELECT DATA_TYPE AS dt FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{self.view}'" \
              f" AND TABLE_SCHEMA='{self.schema}'" \
              f" AND COLUMN_NAME='{column}'"
        df = pd.read_sql(sql, self.connection)
        type = df.at[0, 'dt']
        if type in ['tinyint', 'int']:
            return DataTypes.INT
        elif type in ['char', 'varchar', 'nvarchar', 'varbinary', 'text']:
            return DataTypes.TEXT
        elif type in ['real', 'decimal', 'double precision']:
            return DataTypes.DOUBLE
        elif type in ['date']:
            return DataTypes.DATE
        elif type in ['datetime']:
            return DataTypes.DATETIME
        else:
            return type

    def __get_minimum_maximum_values__(self, item:Item):
        import pandas as pd
        column = self.__get_column_name__(item)
        sql = f"SELECT min(\"{column}\") AS min_value, max(\"{column}\") as max_value FROM {self.schema}.{self.view}"
        df = pd.read_sql(sql, self.connection)
        min_value = df.at[0, 'min_value']
        max_value = df.at[0, 'max_value']
        return min_value, max_value

    def __get_column_values__(self, item: Item):
        import pandas as pd
        column = self.__get_column_name__(item)
        sql = f"SELECT DISTINCT \"{column}\" AS checkfield FROM {self.schema}.{self.view}"
        df = pd.read_sql(sql, self.connection)
        values = df['checkfield'].to_list()
        return values

    def __get_data_for_hierarchy__(self, name):
        import pandas as pd
        fields = ', '.join(f'"{s}"' for s in self.metadata.get_hierarchy(name))
        sql = f"""
                     SELECT
                         {fields} 
                     FROM "{self.schema}"."{self.view}" 
                     GROUP BY {fields} 
        """
        return pd.read_sql(sql, self.connection)

    def check_column_present(self, item: Item):
        import pandas as pd
        if item.get_property('formula') is not None:
            return False
        sql = f"SELECT COLUMN_NAME as col FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{self.view}' AND TABLE_SCHEMA='{self.schema}'"
        df = pd.read_sql(sql, self.connection)
        values = df['col'].to_list()
        if item.name not in values:
            self.errors.append(f"Validation error: '{item.name}' in specification is missing from dataset")
            return False
        return True
