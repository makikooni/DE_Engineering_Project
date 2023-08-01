from utils.utils import get_table_db
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, Mock
from pg8000.native import Connection, InterfaceError


@pytest.fixture
def mock_connection():
    test_connection = Mock(spec=Connection)
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
    yield test_connection

def test_should_return_correct_dataframe_and_string(mock_connection):
    test_connection = mock_connection
    test_table_name = "test_table"

    test_df = pd.DataFrame(data=test_connection.run(), columns=["col1", "col2", "col3"])
    test_result_df, test_query = get_table_db(test_connection, test_table_name)

    assert isinstance(test_result_df, pd.DataFrame)
    assert isinstance(test_query, str)
    assert test_query == f"SELECT * FROM {test_table_name};"
    assert test_result_df.equals(test_df)


def test_should_protect_against_sql_injection(mock_connection):
    test_connection = mock_connection
    test_table_name = "design; DROP *;"
    test_table_df, test_query = get_table_db(test_connection, test_table_name)

    assert test_query == 'SELECT * FROM "design; DROP *;";'


def test_should_raise_exception_if_incorrect_connection_input_type():
    with pytest.raises(TypeError):
        get_table_db([], "table_name")

def test_should_raise_exception_if_incorrect_table_name_input_type(mock_connection):
    with pytest.raises(TypeError):
        get_table_db(mock_connection, 2)

def test_should_raise_error_for_invalid_table(mock_connection):
    test_connection = mock_connection
    test_table_name = "design; DROP *;"
    test_connection.run = MagicMock(side_effect=InterfaceError)
    
    with pytest.raises(InterfaceError):
        test_table_df, test_query = get_table_db(test_connection, test_table_name)
