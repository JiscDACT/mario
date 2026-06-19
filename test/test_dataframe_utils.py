import tempfile
from mario.dataframe_utils import get_column_type, get_column_types, hyper_to_df, csv_to_df, to_bool
import pytest
import pandas as pd
import os

from mario.metadata import Item, Metadata


def create_item(name, properties) -> Item:
    item = Item()
    item.name = name
    for prop in properties:
        item.set_property(prop, properties[prop])
    return item


def create_metadata(items) -> Metadata:
    m = Metadata()
    for item in items:
        m.add_item(item)
    return m


def test_get_column_type_case_insensitive():
    item = create_item("col", {"format": "INT"})
    assert get_column_type(item) == "Int32"


@pytest.mark.parametrize(
    "properties,expected",
    [
        ({"DATA_TYPE": "int"}, "Int32"),
        ({"DATA_TYPE": "float"}, "float64"),
        ({"DATA_TYPE": "unknown"}, "string"),
        ({"format": "bigint"}, "Int64"),
        ({"format": "varchar"}, "string"),
        # format overrides DATA_TYPE
        ({"DATA_TYPE": "int", "format": "float"}, "float64"),
        # neither present
        ({}, "string"),
    ],
)
def test_get_column_type(properties, expected):
    item = create_item("col", properties)
    result = get_column_type(item)
    assert result == expected


def test_get_column_types_basic():
    items = [
        create_item("a", {"DATA_TYPE": "int"}),
        create_item("b", {"format": "float"}),
        create_item("c", {}),
    ]
    metadata = create_metadata(items)

    result = get_column_types(metadata)

    assert result == {
        "a": "Int32",
        "b": "float64",
        "c": "string",
    }


def test_get_column_types_empty():
    metadata = create_metadata([])
    assert get_column_types(metadata) == {}


def test_hyper_to_df_no_types():
    rows = [[1, "x"], [2, "y"]]
    cols = ["a", "b"]

    df = hyper_to_df(rows, cols, None)

    assert list(df.columns) == cols
    assert df.iloc[0]["a"] == 1
    assert df.iloc[1]["b"] == "y"


def test_hyper_to_df_with_types():
    rows = [["1", "2.5", "x"], ["3", "bad", "y"]]
    cols = ["int_col", "float_col", "str_col"]

    column_types = {
        "int_col": "Int32",
        "float_col": "float64",
        "str_col": "string",
    }

    df = hyper_to_df(rows, cols, column_types)

    # int_col becomes nullable integer
    assert df["int_col"].dtype == "Int32"

    # float conversion
    assert df["float_col"].dtype == "float64"
    assert pd.isna(df["float_col"].iloc[1])  # "bad" coerced to NaN

    # string conversion
    assert df["str_col"].dtype == "string"


def test_hyper_to_df_missing_column_in_types():
    rows = [["1", "2"]]
    cols = ["a", "b"]

    column_types = {
        "a": "float64",
        "c": "float64",  # not in dataframe
    }

    df = hyper_to_df(rows, cols, column_types)

    assert df["a"].dtype == "float64"
    assert "b" in df.columns  # unchanged


def test_csv_to_df_basic():
    csv_content = "a,b,c\n1,2.5,hello\n3,4.5,world\n"
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as f:
        f.write(csv_content)
        file_path = f.name

    try:
        column_types = {
            "a": "Int32",
            "b": "float64",
            "c": "string",
        }

        df = csv_to_df(file_path, column_types)

        assert list(df.columns) == ["a", "b", "c"]
        assert df["b"].dtype == "float64"
        assert df["c"].dtype == "string"
    finally:
        os.remove(file_path)


def test_csv_to_df_filters_missing_columns():
    csv_content = "a,b\n1,2\n"
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as f:
        f.write(csv_content)
        file_path = f.name

    try:
        column_types = {
            "a": "Int32",
            "b": "float64",
            "c": "string",  # should be ignored
        }

        df = csv_to_df(file_path, column_types)

        assert "c" not in df.columns
    finally:
        os.remove(file_path)


def test_csv_to_df_date_parsing():
    csv_content = "a,date_col\n1,2024-01-01\n2,2024-01-02\n"
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as f:
        f.write(csv_content)
        file_path = f.name

    try:
        column_types = {
            "a": "Int32",
            "date_col": "datetime64[ns]",
        }

        df = csv_to_df(file_path, column_types)

        assert pd.api.types.is_datetime64_any_dtype(df["date_col"])
    finally:
        os.remove(file_path)


def test_csv_to_df_no_date_cols():
    csv_content = "a,b\n1,2\n"
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as f:
        f.write(csv_content)
        file_path = f.name

    try:
        column_types = {"a": "Int32", "b": "float64"}
        df = csv_to_df(file_path, column_types)

        assert df["b"].dtype == "float64"
    finally:
        os.remove(file_path)


def test_hyper_to_df_mixed_numeric_forces_float():
    rows = [["1"], ["2"], ["3.5"], ["bad"]]
    cols = ["a"]

    column_types = {"a": "float64"}

    df = hyper_to_df(rows, cols, column_types)

    assert df["a"].dtype == "float64"
    assert df["a"].iloc[0] == 1.0
    assert df["a"].iloc[2] == 3.5
    assert pd.isna(df["a"].iloc[3])


def test_hyper_to_df_int_enforced():
    rows = [["1"], ["2"], ["bad"]]
    cols = ["a"]

    column_types = {"a": "Int32"}

    df = hyper_to_df(rows, cols, column_types)

    assert df["a"].dtype == "Int32"
    assert df["a"].iloc[0] == 1
    assert pd.isna(df["a"].iloc[2])


def test_full_schema_enforcement():
    rows = [["1", "2.5", "true", "2024-01-01", "x", "bad"]]
    cols = ["int_col", "float_col", "bool_col", "date_col", "str_col", "bad_int"]

    schema = {
        "int_col": "Int32",
        "float_col": "float64",
        "bool_col": "boolean",
        "date_col": "datetime64[ns]",
        "str_col": "string",
        "bad_int": "Int32",
    }

    df = hyper_to_df(rows, cols, schema)

    assert df["int_col"].dtype == "Int32"
    assert df["float_col"].dtype == "float64"
    assert df["bool_col"].dtype == "boolean"
    assert pd.api.types.is_datetime64_any_dtype(df["date_col"])
    assert df["str_col"].dtype == "string"

    assert pd.isna(df["bad_int"].iloc[0])


def test_csv_and_hyper_consistency(tmp_path):
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("a\n1\nbad\n3\n")

    schema = {"a": "Int32"}

    df_csv = csv_to_df(str(csv_file), schema)
    df_hyper = hyper_to_df([["1"], ["bad"], ["3"]], ["a"], schema)

    assert df_csv["a"].dtype == df_hyper["a"].dtype
    assert df_csv["a"].tolist() == df_hyper["a"].tolist()


def test_hyper_to_df_boolean_values():
    rows = [["true"], ["false"], ["1"], ["0"], ["bad"], [None]]
    cols = ["a"]

    df = hyper_to_df(rows, cols, {"a": "boolean"})

    assert df["a"].dtype == "boolean"
    assert df["a"].tolist() == [True, False, True, False, pd.NA, pd.NA]


def test_csv_to_df_boolean():
    csv_content = "a\ntrue\nfalse\n1\n0\nbad\n"
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as f:
        f.write(csv_content)
        file_path = f.name

    try:
        df = csv_to_df(file_path, {"a": "boolean"})
        assert df["a"].dtype == "boolean"
        assert list(df["a"])[:4] == [True, False, True, False]
        assert pd.isna(df["a"].iloc[4])
    finally:
        os.remove(file_path)


def test_hyper_to_df_invalid_datetime():
    rows = [["2024-01-01"], ["bad"]]
    cols = ["a"]

    df = hyper_to_df(rows, cols, {"a": "datetime64[ns]"})

    assert pd.api.types.is_datetime64_any_dtype(df["a"])
    assert pd.isna(df["a"].iloc[1])


@pytest.mark.parametrize("value", [
    True, 1, 1.0, "1", "1.0", "true", "TRUE", " True "
])
def test_to_bool_true_values(value):
    assert to_bool(value) is True


@pytest.mark.parametrize("value", [
    False, 0, 0.0, "0", "0.0", "false", "FALSE", " False "
])
def test_to_bool_false_values(value):
    assert to_bool(value) is False


@pytest.mark.parametrize("value", [
    None, pd.NA, float("nan")
])
def test_to_bool_null_values(value):
    result = to_bool(value)
    assert result is pd.NA or pd.isna(result)


@pytest.mark.parametrize("value", [
    2,
    -1,
    0.5,
    "yes",
    "no",
    "random",
    "1.00",   # not currently supported
    "0.00",
    "1e0",
    "0e0",
])
def test_to_bool_invalid_values(value):
    result = to_bool(value)
    assert result is pd.NA or pd.isna(result)


def test_to_bool_boolean_passthrough():
    assert to_bool(True) is True
    assert to_bool(False) is False


def test_to_bool_returns_expected_types():
    assert isinstance(to_bool(True), bool)
    assert isinstance(to_bool(1), bool)
    assert to_bool("invalid") is pd.NA or pd.isna(to_bool("invalid"))


def test_to_bool_with_series():
    s = pd.Series(["1", "0", "true", "false", "invalid", None])

    result = s.map(to_bool)

    expected = pd.Series([True, False, True, False, pd.NA, pd.NA], dtype="object")

    # Compare values (handle NA properly)
    for r, e in zip(result, expected):
        if pd.isna(e):
            assert pd.isna(r)
        else:
            assert r == e
