from utils.utils import get_table_db
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, Mock
from pg8000.native import Connection, InterfaceError
import boto3
from moto import mock_s3

@pytest.fixture
def create_s3_client():
    with mock_s3():
        yield boto3.client("s3")


@pytest.fixture
def mock_client(create_s3_client):
    mock_client = create_s3_client
    bucket_name = "test_bucket"
    mock_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    yield mock_client

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

def test_should_return_correct_dataframe_and_string(mock_connection, mock_client):
    test_connection = mock_connection
    test_table_name = "test_table"

    test_df = pd.DataFrame(data=test_connection.run(), columns=["col1", "col2", "col3"])
    with patch("utils.utils.boto3.client") as mocked_client:
        mocked_client.return_value = mock_client

        test_result_df, test_query = get_table_db(test_connection, test_table_name, "test_bucket")

        assert isinstance(test_result_df, pd.DataFrame)
        assert isinstance(test_query, str)
        assert test_query == f"SELECT * FROM {test_table_name};"
        assert test_result_df.equals(test_df)


def test_should_protect_against_sql_injection(mock_connection, mock_client):
    test_connection = mock_connection
    test_table_name = "design; DROP *;"
    with patch("utils.utils.boto3.client") as mocked_client:
        mocked_client.return_value = mock_client
        test_table_df, test_query = get_table_db(test_connection, test_table_name, "test_bucket")

        assert test_query == 'SELECT * FROM "design; DROP *;";'


def test_should_raise_exception_if_incorrect_connection_input_type():
    with pytest.raises(TypeError):
        get_table_db([], "table_name")

def test_should_raise_exception_if_incorrect_table_name_input_type(mock_connection):
    with pytest.raises(TypeError):
        get_table_db(mock_connection, 2)

def test_should_raise_error_for_invalid_table(mock_connection, mock_client):
    test_connection = mock_connection
    test_table_name = "design; DROP *;"
    test_connection.run = MagicMock(side_effect=InterfaceError)
    with patch("utils.utils.boto3.client") as mocked_client:
        mocked_client.return_value = mock_client
        with pytest.raises(InterfaceError):
            test_table_df, test_query = get_table_db(test_connection, test_table_name, "test_bucket")
