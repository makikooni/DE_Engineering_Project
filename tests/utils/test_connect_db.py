from utils.utils import connect_db
from pg8000.native import Connection, InterfaceError
import boto3
from moto import mock_rds
import pytest

def test_should_raise_exception_if_incorrect_credentials():
    test_db_credentials = {
        "username": "test_user",
        "password": "test_password",
        "engine": "test_engine",
        "host": "http://test.com",
        "port": "9090",
        "dbname": "test_dbname",
    }
    with pytest.raises(InterfaceError):
        connection = connect_db(test_db_credentials, db_name="totesys")


def test_should_raise_exception_if_incorrect_input_type():
    test_db_credentials = {
        "username": "test_user",
        "password": "test_password",
        "engine": "test_engine",
        "host": "http://test.com",
        "port": "9090",
        "dbname": "test_dbname",
    }
    with pytest.raises(TypeError):
        connection = connect_db(test_db_credentials, db_name=2)

    with pytest.raises(TypeError):
        connection = connect_db(["test"])


def test_should_raise_exception_if_incorrect_db_data():
    test_db_credentials = {
        "username": "test_user",
        "password": "test_password",
        "engine": "test_engine",
        "port": "9090",
        "dbname": "test_dbname",
    }
    with pytest.raises(KeyError):
        connection = connect_db(test_db_credentials)

    test_db_credentials = {
        "username": "test_user",
        "password": "test_password",
        "engine": "test_engine",
        "host": "http://test.com",
        "port": 9090,
        "dbname": "test_dbname",
    }

    with pytest.raises(ValueError):
        connection = connect_db(test_db_credentials)
