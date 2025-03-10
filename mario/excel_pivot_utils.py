def replace_pivot_cache(worksheet, subset):
    pivot_cache = worksheet._pivots[0].cache
    pivot_cache.records.r = subset


def get_subset(pivot_cache, field_name, field_value):
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


def get_values_for_pivot_cache_column(pivot_data, column_name):
    items = []

    for field in pivot_data.cacheFields:
        if field.name == column_name:
            for item in field.sharedItems._fields:
                if hasattr(item, 'v'):
                    items.append(item.v)
                elif hasattr(item, 'n'):
                    items.append(item.n)
                elif hasattr(item, 'f'):
                    items.append(item.f)
                else:
                    items.append(item)

    if len(items) == 0:
        raise ValueError(f"Column '{column_name}' not found in pivot_data")

    # Use a set to get unique values
    unique_values = set(items)
    return list(unique_values)


def read_pivot_cache(file_path, sheet_name):
    from openpyxl import load_workbook
    wb = load_workbook(file_path, data_only=True)
    ws = wb[sheet_name]
    pivot = ws._pivots[0]
    return pivot.cache
