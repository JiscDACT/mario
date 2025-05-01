from typing import List
from openpyxl import Workbook
import pandas as pd


def replace_pivot_cache_with_subset(worksheet, field, value) -> None:
    """
    Replaces the cached data for a pivot with a subset filtered by
    the supplied field and value
    :param worksheet: the sheet to filter
    :param field: the field name
    :param value: the field value to filter by
    :return: None
    """
    pivot = worksheet._pivots[0]
    cached_data = pivot.cache
    subset = get_subset(cached_data, field, value)
    pivot_cache = worksheet._pivots[0].cache
    pivot_cache.records.r = subset


def get_subset(pivot_cache, field_name, field_value):
    """
    Returns a subset from pivot cache data using the field
    name and field value as a filter
    :param pivot_cache: the cache to filter
    :param field_name: the name of the field to filter by
    :param field_value: the value to filter by
    :return: a filtered pivot cache
    """
    # Get the index of the field name
    field_index = None
    cache_field = None
    field_value_index = None

    for idx, field in enumerate(pivot_cache.cacheFields):
        if field.name == field_name:
            cache_field = field
            field_index = idx
            break

    # Get the index of the field value
    # Map the indices to actual values
    field_items = cache_field.sharedItems._fields
    for index, item in enumerate(field_items):
        if item.v == field_value:
            field_value_index = index
            break

    if field_index is None:
        raise ValueError(f"Field '{field_name}' not found in pivot cache.")
    if field_value_index is None:
        raise ValueError(f"Value '{field_value}' not found in pivot cache.")

    # Filter the pivot cache records
    filtered_records = []
    for record in pivot_cache.records.r:
        if record._fields[field_index].v == field_value_index:
            filtered_records.append(record)

    return filtered_records


def get_unique_values_in_worksheet(workbook, sheet_name, field) -> List[str]:
    ws = workbook[sheet_name]
    pivot = ws._pivots[0]
    cached_data = pivot.cache

    items = []

    for f in cached_data.cacheFields:
        if f.name == field:
            for item in f.sharedItems._fields:
                if hasattr(item, 'v'):
                    items.append(item.v)
                elif hasattr(item, 'n'):
                    items.append(item.n)
                elif hasattr(item, 'f'):
                    items.append(item.f)
                else:
                    items.append(item)

    if len(items) == 0:
        raise ValueError(f"Column '{field}' not found in pivot_data")

    # Use a set to get unique values
    unique_values = set(items)
    return list(unique_values)


def get_unique_values(file_path, sheet_name, field) -> List[str]:
    """
    Returns all the unique values for a column in a pivot cache
    :param file_path: the workbook file path
    :param sheet_name: the name of the sheet
    :param field: the field to get values for
    :return: a list of unique values for the specified column in the cache
    """
    from openpyxl import load_workbook
    wb = load_workbook(file_path, data_only=True)
    return get_unique_values_in_worksheet(workbook=wb, sheet_name=sheet_name, field=field)


def get_header(file_path: str, sheet_name: str):
    header: List[str] = pd.read_excel(file_path, sheet_name=sheet_name, nrows=0).columns.tolist()
    return header


def sheet_contains_column_name(file_path: str, sheet_name: str, field: str):
    return field in get_header(file_path=file_path, sheet_name=sheet_name)


def get_unique_values_for_workbook(file_path, field):
    from openpyxl import load_workbook
    wb: Workbook = load_workbook(file_path, data_only=True)
    for sheet in wb.sheetnames:
        ws = wb[sheet]
        if len(ws._pivots) > 0:
            return get_unique_values_in_worksheet(workbook=wb, sheet_name=sheet, field=field)
        else:
            if sheet_contains_column_name(file_path=file_path, sheet_name=sheet, field=field):
                return pd.read_excel(file_path, sheet_name=sheet, dtype={field: object}, usecols=[field])[field].unique().tolist()
    raise ValueError("No valid worksheet containing field values")



