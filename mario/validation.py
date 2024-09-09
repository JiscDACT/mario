import logging
from enum import Enum

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
        raise NotImplementedError()

    def __get_column_values__(self, item: Item):
        raise NotImplementedError()

    def __get_minimum_maximum_values__(self, item:Item):
        raise NotImplementedError()

    def __contains_nulls__(self, item: Item):
        return None in self.__get_column_values__(item)

    def __get_hierarches__(self):
        hierarchies = set()
        for item in self.metadata.items:
            if item.get_property('hierarchies') is not None:
                for hierarchy in item.get_property('hierarchies'):
                    hierarchies.add(hierarchy['hierarchy'])
        return list(hierarchies)

    def __get_hierarchy__(self, name):
        items = []
        for item in self.metadata.items:
            if item.get_property('hierarchies') is not None:
                for hierarchy in item.get_property('hierarchies'):
                    if hierarchy['hierarchy'] == name:
                        items.append({'name': item.name, 'level': hierarchy['level']})

        # Sort the list of dictionaries by 'level'
        sorted_items = sorted(items, key=lambda x: x['level'])
        # Extract the 'name' values in order
        item_names_in_order = [item['name'] for item in sorted_items]
        return item_names_in_order

    def __get_data_for_hierarchy__(self, name):
        raise NotImplementedError()

    def __check_hierarchy__(self, name):
        # Create a dictionary to store the hierarchy
        hierarchy = {}

        # Get the levels
        levels = self.__get_hierarchy__(name)

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

    def check_hierarchies(self):
        for hierarchy in self.__get_hierarches__():
            self.__check_hierarchy__(hierarchy)

    def check_data_type(self, item: Item):
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
                                 f"This is probably due to a limitation of Pandas not having a native 'Date' type")
            return False

        self.errors.append(f"Validation error: '{item.name}' is of the wrong type - expected {expected_data_type}, actual {actual_data_type}")
        return False

    def check_column_present(self, item: Item):
        raise NotImplementedError()

    def check_nulls(self, item: Item, allow_nulls=True):
        if self.__contains_nulls__(item):
            if allow_nulls:
                self.warnings.append(f"Validation warning: '{item.name}' contains NULLs")
            else:
                self.errors.append(f"Validation error: '{item.name}' contains NULLs")

    def check_domain(self, item: Item):
        if item.get_property('domain') is not None:
            data_domain = self.__get_column_values__(item)
            metadata_domain = item.get_property('domain')
            for element in data_domain:
                if element not in metadata_domain:
                    self.errors.append(f"Validation error: '{str(element)}' is not in domain of '{item.name}'")
            for metadata_element in metadata_domain:
                if metadata_element not in data_domain:
                    self.warnings.append(f"Validation error: '{str(metadata_element)}' is in domain of '{item.name}' but not present in the data")

    def check_range(self, item: Item):
        if item.get_property('range') is not None:
            min_value = item.get_property('range')[0]
            max_value = item.get_property('range')[1]
            data_min, data_max = self.__get_minimum_maximum_values__(item)
            if data_min < min_value:
                self.errors.append(f"Validation error: '{item.name}': '{str(data_min)}' is less than '{str(min_value)}'")
            if data_max > max_value:
                self.errors.append(f"Validation error: '{item.name}': '{str(data_max)}' is greater than '{str(max_value)}'")

    def check_pattern_match(self, item: Item):
        import re
        if item.get_property('pattern') is not None:
            values = self.__get_column_values__(item)
            pattern = re.compile(item.get_property('pattern'))
            for value in values:
                if value is not None:
                    if not re.match(pattern, str(value)):
                        self.errors.append(f"Validation error: '{item.name}': '{str(value)}' does not match the pattern '{str(item.get_property('pattern'))}'")

    def check_quality_checks(self, item: Item):
        if item.get_property('formula') is None:
            if item.get_property('range') is None:
                if item.get_property('domain') is None:
                    if item.get_property('pattern') is None:
                        self.warnings.append(f"Validation warning: '{item.name}' has no quality rules.")

    def validate_data(self, allow_nulls=True):
        for item in self.dataset_specification.items:
            metadata = self.metadata.get_metadata(item)
            if self.check_column_present(metadata):
                self.check_nulls(metadata, allow_nulls)
                self.check_domain(metadata)
                self.check_range(metadata)
                self.check_quality_checks(metadata)
                self.check_data_type(metadata)
                self.check_pattern_match(metadata)

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
        fields = self.__get_hierarchy__(name)
        return self.data[fields].drop_duplicates().reset_index(drop=True)

    def check_column_present(self, item: Item):
        if item.get_property('formula') is None:
            column = self.__get_column_name__(item)
            if column not in self.data.columns:
                self.errors.append(f"Validation error: '{item.name}' in specification is missing from dataset")
                return False
            return True
        return False


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

        from tableau_builder.hyper_utils import get_default_table_and_schema
        self.hyper_file_path = hyper_file_path
        table_schema = get_default_table_and_schema(self.hyper_file_path)
        self.table = table_schema['table']
        self.schema = table_schema['schema']

    def __get_data_for_hierarchy__(self, name):
        from pantab import frame_from_hyper_query
        fields = ', '.join(f'"{s}"' for s in self.__get_hierarchy__(name))
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

    def check_column_present(self, item: Item):
        column = self.__get_column_name__(item)
        from tableau_builder.hyper_utils import check_column_exists
        if item.get_property('formula') is None:
            if not check_column_exists(
                    hyper_path=self.hyper_file_path,
                    column_name=column,
                    table_name=self.table,
                    schema_name=self.schema
            ):
                self.errors.append(f"Validation error: '{item.name}' in specification is missing from dataset")
                return False
            return True
        return False

    def __get_column_data_type__(self, item: Item):
        from tableau_builder.hyper_utils import get_table
        from tableauhyperapi import TypeTag
        column = self.__get_column_name__(item)
        table = get_table(hyper_path=self.hyper_file_path, table_name=self.table, schema_name=self.schema)
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

