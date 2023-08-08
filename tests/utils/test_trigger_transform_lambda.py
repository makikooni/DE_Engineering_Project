from utils.utils import trigger_transform_lambda
from datetime import datetime
import pytest
import boto3
from unittest.mock import patch
from moto import mock_s3
import pandas as pd
from botocore.exceptions import ClientError


@pytest.fixture
def create_s3_client():
    with mock_s3():
        yield boto3.client("s3")


@pytest.fixture
def mock_client(create_s3_client):
    mock_client = create_s3_client
    bucket_name = "test_ingestion_bucket"
    mock_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    yield mock_client


def test_should_correctly_upload_text_log_file(mock_client):
    with patch("utils.utils.boto3.client") as mocked_client:
        mocked_client.return_value = mock_client
        bucket_name = "test_ingestion_bucket"
        trigger_transform_lambda(bucket_name=bucket_name, prefix="ExtractHistory")
        test_bucket_contents = mock_client.list_objects_v2(Bucket=bucket_name)[
            "Contents"
        ]
        file_name = f"{datetime.now().strftime('%d%m%Y%H%M')}"

        assert test_bucket_contents[0]["Key"] == f"ExtractHistory/{file_name}.txt"


def test_should_raise_exception_if_incorrect_inputs():
    with pytest.raises(TypeError):
        trigger_transform_lambda(bucket_name="bucketname", prefix=2)
    with pytest.raises(TypeError):
        trigger_transform_lambda(bucket_name=["bucket"], prefix="prefix")


def test_should_raise_exception_if_incorrect_bucket_name(mock_client):
    with patch("utils.utils.boto3.client") as mocked_client:
        mocked_client.return_value = mock_client

    with pytest.raises(ClientError):
        trigger_transform_lambda(bucket_name="hello", prefix="ExtractHistory")
