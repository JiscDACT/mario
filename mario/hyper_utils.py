"""
Hyper utility functions
"""
import logging
from typing import List

log = logging.getLogger(__name__)


def get_default_table_and_schema(hyper_path) -> (str, str):
    """
    Get the default table and schema from a hyper, assuming its
    a single-table hyper
    :param hyper_path:
    :return:
    """
    from tableauhyperapi import HyperProcess, Connection, Telemetry
    tables = []
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, 'test') as hyper:
        with Connection(hyper.endpoint, hyper_path) as connection:
            # The `connection.catalog` provides us with access to the meta-data we are interested in
            catalog = connection.catalog
            # Iterate over all schemas
            schemas = catalog.get_schema_names()
            for schema_name in schemas:
                # For each schema, iterate over all tables
                schema_tables = catalog.get_table_names(schema=schema_name)
                if len(schema_tables) > 0:
                    tables = schema_tables
    table = tables[0].name.unescaped
    schema = tables[0].schema_name
    if schema is None:
        schema = 'public'
    else:
        schema = schema.name.unescaped
    return schema, table


def check_column_exists(hyper_file_path: str, column: str, schema='public', table='default'):
    columns = get_column_list(hyper_file_path=hyper_file_path, schema=schema, table=table)
    return column in columns


def get_column_list(hyper_file_path: str, schema='public', table='default'):
    """
    Get the list of columns in a table
    :param hyper_file_path:
    :param schema:
    :param table:
    :return:
    """
    from tableauhyperapi import HyperProcess, Telemetry, Connection, TableName
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as connection:
            table_name = TableName(schema, table)
            table_definition = connection.catalog.get_table_definition(table_name)
            columns = [col.name.unescaped for col in table_definition.columns]
    return columns


def filter_column_list(hyper_file_path: str, columns:List[str], schema='public', table='default'):
    """
    Filters a list of columns, removing any that aren't present in the hyper
    :param hyper_file_path:
    :param columns:
    :return:
    """
    filtered_columns = []
    td_columns = get_column_list(hyper_file_path=hyper_file_path, schema=schema, table=table)
    for column in columns:
        for td_column in td_columns:
            if td_column == column:
                filtered_columns.append(column)
    return filtered_columns


def get_table(hyper_path: str, schema='public', table='default'):
    from tableauhyperapi import HyperProcess, Telemetry, Connection, TableName, TableDefinition
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, 'test') as hyper:
        with Connection(hyper.endpoint, hyper_path) as connection:
            table_name_tuple = TableName(schema, table)
            table: TableDefinition = connection.catalog.get_table_definition(table_name_tuple)
    return table


def get_total(hyper_file_path: str, schema='public', table='default', measure='FPE'):
    from tableauhyperapi import HyperProcess, Telemetry, Connection
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as connection:
            total = connection.execute_scalar_query(f"SELECT SUM(\"{measure}\") FROM \"{schema}\".\"{table}\"")
    return total


def get_row_count(hyper_file_path: str, schema='public', table='default'):
    from tableauhyperapi import HyperProcess, Telemetry, Connection
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as connection:
            row_count = connection.execute_scalar_query(f"SELECT COUNT(*) FROM \"{schema}\".\"{table}\"")
    return row_count


def add_row_numbers_to_hyper(input_hyper_file_path: str, schema='public', table='default'):
    from tableauhyperapi import HyperProcess, Telemetry, Connection, TableName, TableDefinition, SqlType

    drop_columns_from_hyper(hyper_file_path=input_hyper_file_path, columns_to_drop=['row_number'], schema=schema, table=table)

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=input_hyper_file_path) as connection:
            input_table_name = TableName(schema, table)
            output_table_name = TableName(schema, table + '_new')

            output_table_definition = TableDefinition(output_table_name,
                                                      connection.catalog.get_table_definition(
                                                          input_table_name).columns)
            output_table_definition.add_column(TableDefinition.Column('row_number', SqlType.big_int()))
            if output_table_name not in connection.catalog.get_table_names(schema):
                connection.catalog.create_table(output_table_definition)
            log.debug("Created output table")

        with Connection(endpoint=hyper.endpoint, database=input_hyper_file_path) as connection:
            query = f"""
                    WITH CTE AS (
                        SELECT *, ROW_NUMBER() OVER() AS row_number
                        FROM \"{schema}\".\"{input_table_name.name.unescaped}\"
                    )
                    INSERT INTO \"{schema}\".\"{output_table_name.name.unescaped}\"
                        SELECT * FROM CTE
                    """
            connection.execute_command(query)
            log.debug(f"Added row numbers to {input_table_name} and output to {output_table_name}")

        with Connection(endpoint=hyper.endpoint, database=input_hyper_file_path) as connection:
            drop_table_query = f"""
                    DROP TABLE \"{schema}\".\"{input_table_name.name.unescaped}\"
            """
            connection.execute_command(drop_table_query)
            log.debug("Dropped old table")
            rename_table_query = f"""
                    ALTER TABLE \"{schema}\".\"{output_table_name.name.unescaped}\" RENAME TO \"{input_table_name.name.unescaped}\"
            """
            connection.execute_command(rename_table_query)
            log.debug("Renamed new table")


def filter_hyper_by_row_number(input_hyper_file_path: str, output_hyper_file_path: str, row_numbers, schema='public', table='default'):
    where = f"row_number NOT IN ({','.join([str(x) for x in row_numbers])})"
    filter_hyper(input_hyper_file_path=input_hyper_file_path,
                 output_hyper_file_path=output_hyper_file_path,
                 where=where,
                 schema=schema,
                 table=table)


def filter_hyper(input_hyper_file_path: str, output_hyper_file_path: str, where: str = "1=1", schema='public', table='default'):
    from tableauhyperapi import HyperProcess, Telemetry, Connection, TableName, TableDefinition
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint) as connection:
            # Connect to the input and output databases
            # Create the output Hyper file or overwrite it
            catalog = connection.catalog
            catalog.drop_database_if_exists(output_hyper_file_path)
            catalog.create_database(output_hyper_file_path)
            catalog.attach_database(output_hyper_file_path, alias="output_database")
            catalog.attach_database(input_hyper_file_path, alias="input_database")

            input_table_name = TableName("input_database", schema, table)
            output_table_name = TableName("output_database", schema, table)
            output_table_definition = TableDefinition(output_table_name, catalog.get_table_definition(
                input_table_name).columns)
            catalog.create_schema_if_not_exists(output_table_name.schema_name)
            catalog.create_table(output_table_definition)
            connection.execute_command(
                f"INSERT INTO {output_table_name} "
                f"(SELECT * FROM {input_table_name} "
                f"WHERE {where} "
                f")")
    log.debug(f"Filtered {input_hyper_file_path} into {output_hyper_file_path}")


def defragment_hyper(input_hyper_file_path: str, output_hyper_file_path: str, schema='public', table='default'):
    filter_hyper(
        input_hyper_file_path=input_hyper_file_path,
        output_hyper_file_path=output_hyper_file_path,
        schema=schema,
        table=table
    )


def drop_columns_from_hyper(hyper_file_path, columns_to_drop=None, columns_to_keep=None, schema='public', table='default'):
    """
    Because there is no way to drop a column in a hyper, we have this stupid workaround
    """
    from tableauhyperapi import HyperProcess, Telemetry, Connection, TableName, TableDefinition, escape_name

    if columns_to_drop is not None:
        columns_to_drop = filter_column_list(
            hyper_file_path=hyper_file_path,
            columns=columns_to_drop,
            schema=schema,
            table=table
        )
    elif columns_to_keep is not None:
        all_columns = get_column_list(hyper_file_path=hyper_file_path, schema=schema, table=table)
        columns_to_drop = []
        for column in all_columns:
            if column not in columns_to_keep:
                columns_to_drop.append(column)
    else:
        raise ValueError("Must supply a list of columns to keep or to drop")

    if columns_to_drop is None or len(columns_to_drop) == 0:
        log.debug(f"No columns to drop from dataset.")
    else:
        log.info(f"- Dropping {len(columns_to_drop)} columns from dataset.")
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as connection:
                # Get the existing table definition
                existing_table_definition = connection.catalog.get_table_definition(TableName(schema, table))

                # Define the new table schema without the columns to drop
                new_columns = [
                    col for col in existing_table_definition.columns if col.name.unescaped not in columns_to_drop
                ]
                new_table_definition = TableDefinition(
                    table_name=TableName(schema, f"{table}_new"),
                    columns=new_columns
                )
                if len(new_columns) != len(existing_table_definition.columns) - len(columns_to_drop):
                    raise ValueError(f"One or more columns specified to drop ({columns_to_drop})"
                                     f" are not present in the dataset.")

            # Create the new table
            with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as connection:
                connection.catalog.create_table(new_table_definition)

            # Copy data
            with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as connection:
                select_columns = ", ".join([escape_name(col.name.unescaped) for col in new_columns])
                query = f"INSERT INTO \"{schema}\".\"{table}_new\" (SELECT {select_columns} FROM \"{schema}\".\"{table}\")"
                connection.execute_command(query)

            with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as connection:
                # Drop the old table
                connection.execute_command(f"DROP TABLE \"{schema}\".\"{table}\"")

                # Rename the new table to the old table name
                connection.execute_command(
                    f"ALTER TABLE \"{schema}\".\"{table}_new\" RENAME TO \"{table}\"")


def create_random_sample(hyper_file_path, sample_size, schema='public', table='default'):
    from tableauhyperapi import HyperProcess, Connection, Telemetry

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as connection:

            # Create a temporary table with the random sample
            connection.execute_command(f"""
                CREATE TABLE random_sample AS
                SELECT *
                FROM \"{schema}\".\"{table}\"
                ORDER BY RANDOM()
                LIMIT {sample_size}
            """)

            connection.execute_command(f"DROP TABLE \"{schema}\".\"{table}\"")
            connection.execute_command(f"ALTER TABLE random_sample RENAME TO \"{table}\"")


def concatenate_hypers(hyper_file_path1, hyper_file_path2, output_hyper_file_path, schema='public', table='default'):
    from pantab import frame_from_hyper, frame_to_hyper
    from tableauhyperapi import TableName
    import pandas as pd

    df1 = frame_from_hyper(hyper_file_path1, table=TableName(schema, table))
    df2 = frame_from_hyper(hyper_file_path2, table=TableName(schema, table))

    df_merged = pd.concat([df1, df2], axis=1)
    frame_to_hyper(df=df_merged, database=output_hyper_file_path, table=TableName(schema, table))