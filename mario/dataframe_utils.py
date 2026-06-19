import pandas as pd
import logging

from mario.metadata import Item, Metadata

logger = logging.getLogger(__name__)


INT_TYPES = {"Int16", "Int32", "Int64"}
SAFE_EARLY_TYPES = {"string", "float64"}


TYPE_MAPPINGS = {
    "tinyint": "Int16",
    "smallint": "Int16",
    "integer": "Int32",
    "int": "Int32",
    "bigint": "Int64",
    "float": "float64",
    "double": "float64",
    "real": "float64",
    "numeric": "float64",
    "decimal": "float64",
    "varchar": "string",
    "nvarchar": "string",
    "char": "string",
    "text": "string",
    "varbinary": "string",
    "boolean": "boolean",
    "timestamp": "datetime64[ns]",
    "date": "datetime64[ns]"
}


def get_column_type(item: Item) -> str:
    """
    :param item: a metadata item
    :return: the Pandas column type for the item, based on its metadata
    """
    source_type = 'string'
    # legacy attribute
    if 'DATA_TYPE' in item.properties:
        logger.warning(f'Item {item.name} uses the legacy "DATA_TYPE" attribute; use "format" instead in future')
        source_type = item.get_property('DATA_TYPE').lower()
    # standard attribute
    if 'format' in item.properties:
        source_type = item.get_property('format').lower()
    if source_type in TYPE_MAPPINGS:
        return TYPE_MAPPINGS[source_type]
    return 'string'


def get_column_types(metadata: Metadata) -> dict[str, str]:
    """
    Returns the set of column type mappings for the metadata
    :param metadata:
    :return:
    """
    column_types = {
        item.name: get_column_type(item)
        for item in metadata.items
    }
    return column_types


def to_bool(x):
    if pd.isna(x):
        return pd.NA

    if isinstance(x, bool):
        return x

    # Handle numeric types explicitly
    if isinstance(x, (int, float)):
        if x == 1:
            return True
        if x == 0:
            return False
        return pd.NA

    x_str = str(x).lower().strip()
    if x_str in {"true", "1", "1.0"}:
        return True
    if x_str in {"false", "0", "0.0"}:
        return False
    return pd.NA


def enforce_schema(df: pd.DataFrame, column_types: dict[str, str]) -> pd.DataFrame:
    for col, dtype in column_types.items():
        if col not in df.columns:
            continue

        if dtype == "float64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        elif dtype in INT_TYPES:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

        elif dtype == "boolean":
            df[col] = df[col].map(to_bool).astype("boolean")

        elif dtype == "datetime64[ns]":
            df[col] = pd.to_datetime(df[col], errors="coerce")

        else:
            df[col] = df[col].astype("string")

    return df


def hyper_to_df(
        rows: list,
        columns: list[str],
        column_types: dict[str, str] | None,
) -> pd.DataFrame:
    """
    Converts Hyper rows to Pandas DataFrame with strict type enforcement.
    """
    df = pd.DataFrame(rows, columns=columns)
    if not column_types:
        return df

    return enforce_schema(df, column_types)


def csv_to_df(file_path: str, column_types: dict[str, str]) -> pd.DataFrame:
    """
    Converts CSV to Pandas DataFrame with strict type enforcement.
    """
    import csv

    with open(file_path, newline="") as f:
        csv_columns = next(csv.reader(f))

    column_types = {k: v for k, v in column_types.items() if k in csv_columns}

    safe_dtype_map = {}
    date_cols = []

    # We read in only types that can't raise errors, but enforce
    # strict types on output
    for col, dtype in column_types.items():
        if dtype in SAFE_EARLY_TYPES:
            safe_dtype_map[col] = dtype
        elif dtype == "datetime64[ns]":
            date_cols.append(col)

    df = pd.read_csv(
        file_path,
        dtype=safe_dtype_map if safe_dtype_map else None,
        parse_dates=date_cols if date_cols else None,
    )

    return enforce_schema(df, column_types)

