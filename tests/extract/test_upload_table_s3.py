from src.extract.extract import upload_table_s3
import pytest
import boto3
from unittest.mock import patch
from moto import mock_s3
import pandas as pd


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


# @mock_s3
def test_should_correctly_upload_dataframe_as_csv_to_s3(mock_client):
    bucket_name = "test_bucket"
    table_name = "test_table"
    test_data = [
        ["r1c1", "r1c2", "r1c3"],
        ["r2c1", "r2c2", "r2c3"],
        ["r3c1", "r3c2", "r3c3"],
    ]
    test_table_df = pd.DataFrame(data=test_data, columns=["col1", "col2", "col3"])
    upload_table_s3(test_table_df, table_name, bucket_name)

    test_bucket_contents = mock_client.list_objects_v2(Bucket=bucket_name)["Contents"]

    assert test_bucket_contents[0]["Key"] == "test_table.csv"
    assert test_bucket_contents[0]["Size"] > 0


def test_input_arguments_should_be_correct_type():
    test_data = [
        ["r1c1", "r1c2", "r1c3"],
        ["r2c1", "r2c2", "r2c3"],
        ["r3c1", "r3c2", "r3c3"],
    ]
    test_table_df = pd.DataFrame(data=test_data, columns=["col1", "col2", "col3"])

    with pytest.raises(TypeError):
        upload_table_s3("string", "string", "string")
    with pytest.raises(TypeError):
        upload_table_s3(test_table_df, 2, "string")
    with pytest.raises(TypeError):
        upload_table_s3(test_table_df, "string", 2)
