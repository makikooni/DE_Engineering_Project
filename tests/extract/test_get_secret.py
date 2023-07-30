from src.extract.extract import get_secret
import pytest
import boto3
from unittest.mock import patch
from moto import mock_secretsmanager


@pytest.fixture
def create_sm_client():
    with mock_secretsmanager():
        yield boto3.client("secretsmanager")


@pytest.fixture
def sm_client(create_sm_client):
    AWS_SECRET_DB_CREDENTIALS_NAME = "test_db_credentials"
    AWS_SECRET_TABLES_NAMES = "test_table_name"

    test_db_credentials = str(
        {
            "username": "test_user",
            "password": "test_password",
            "engine": "test_engine",
            "host": "test_host",
            "port": "test_port",
            "dbname": "test_dbname",
        }
    )
    mocked_sm_client = create_sm_client
    mocked_sm_client.create_secret(
        Name=AWS_SECRET_DB_CREDENTIALS_NAME,
        Description=AWS_SECRET_DB_CREDENTIALS_NAME,
        SecretString=test_db_credentials,
    )

    test_table_names = str(
        {
            "table1": "table1",
            "table2": "table2",
            "table3": "table3",
            "table4": "table4",
            "table5": "table5",
        }
    )

    mocked_sm_client.create_secret(
        Name=AWS_SECRET_TABLES_NAMES,
        Description=AWS_SECRET_TABLES_NAMES,
        SecretString=test_table_names,
    )
    yield mocked_sm_client
    # result = mocked_sm_client.list_secrets()
    # name_of_secrets = [secret['Name'] for secret in result['SecretList'] ]
    # print(name_of_secrets)
    # assert AWS_SECRET_DB_CREDENTIALS_NAME in name_of_secrets
    # assert AWS_SECRET_TABLES_NAMES in name_of_secrets


def test_should_return_a_dict_with_correct_keys(sm_client):
    AWS_SECRET_DB_CREDENTIALS_NAME = "test_db_credentials"

    with patch("src.extract.extract.boto3.client") as mock_client:
        mock_client.get_secret_value.return_value = sm_client.get_secret_value(
            SecretId=AWS_SECRET_DB_CREDENTIALS_NAME
        )

    test_db_credentials_response = get_secret(AWS_SECRET_DB_CREDENTIALS_NAME)

    assert isinstance(test_db_credentials_response, dict)

    res_keys = ["ARN", "Name", "SecretString", "CreatedDate"]
    for key in res_keys:
        assert key in test_db_credentials_response


def test_should_return_a_dict_with_correct_db_credential_data(sm_client):
    AWS_SECRET_DB_CREDENTIALS_NAME = "test_db_credentials"

    with patch("src.extract.extract.boto3.client") as mock_client:
        mock_client.get_secret_value.return_value = sm_client.get_secret_value(
            SecretId=AWS_SECRET_DB_CREDENTIALS_NAME
        )

    test_db_credentials_response = get_secret(AWS_SECRET_DB_CREDENTIALS_NAME)
    assert test_db_credentials_response["SecretString"] == str(
        {
            "username": "test_user",
            "password": "test_password",
            "engine": "test_engine",
            "host": "test_host",
            "port": "test_port",
            "dbname": "test_dbname",
        }
    )


def test_should_return_a_dict_with_correct_table_name_data(sm_client):
    AWS_SECRET_TABLES_NAMES = "test_table_name"

    with patch("src.extract.extract.boto3.client") as mock_client:
        mock_client.get_secret_value.return_value = sm_client.get_secret_value(
            SecretId=AWS_SECRET_TABLES_NAMES
        )

    test_table_names_response = get_secret(AWS_SECRET_TABLES_NAMES)
    assert test_table_names_response["SecretString"] == str(
        {
            "table1": "table1",
            "table2": "table2",
            "table3": "table3",
            "table4": "table4",
            "table5": "table5",
        }
    )


@mock_secretsmanager
def test_raises_exception_if_incorrect_input_arguments():
    AWS_SECRET_TABLES_NAMES = "WRONG_NAME"

    with pytest.raises(KeyError):
        get_secret(AWS_SECRET_TABLES_NAMES)

    AWS_SECRET_TABLES_NAMES = []

    with pytest.raises(TypeError):
        get_secret(AWS_SECRET_TABLES_NAMES)
