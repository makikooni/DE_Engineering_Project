from src.utils.utils import get_table
import pytest
import pandas as pd
from unittest.mock import patch


@patch("src.utils.utils.Connection")
def test_should_return_correct_dataframe_and_string(test_connection):
    test_table_name = "test_table"

    test_connection.run.return_value = [
        ["r1c1", "r1c2", "r1c3"],
        ["r2c1", "r2c2", "r2c3"],
        ["r3c1", "r3c2", "r3c3"],
    ]

    test_connection.columns = [
        {"name": "col1"},
        {"name": "col2"},
        {"name": "col3"},
    ]

    test_df = pd.DataFrame(data=test_connection.run(), columns=["col1", "col2", "col3"])

    test_result_df, test_query = get_table(test_connection, test_table_name)

    assert isinstance(test_result_df, pd.DataFrame)
    assert isinstance(test_query, str)
    assert test_query == f"SELECT * FROM {test_table_name};"
    assert test_result_df.equals(test_df)


@patch("src.utils.utils.Connection")
def test_should_protect_against_sql_injection(test_connection):
    test_table_name = "design; DROP *;"
    test_table_df, test_query = get_table(test_connection, test_table_name)

    assert test_query == 'SELECT * FROM "design; DROP *;";'


def test_should_raise_exception_if_incorrect_input_type():
    with pytest.raises(TypeError):
        get_table([], "table_name")
