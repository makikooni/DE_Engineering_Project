# import moto.core
from moto import mock_s3
import boto3
import pytest
from pandas.testing import assert_frame_equal
import pandas as pd
from utils.utils import read_csv_to_pandas


@pytest.fixture
def create_s3_client():
    with mock_s3():
        yield boto3.client('s3', region_name='eu-west-2')


@pytest.fixture
def mock_client(create_s3_client):
        '''
        fixture creates 'mock-test-ingestion-va-052023' bucket in 
        mock aws account and uploads 'test.csv' file to it
        '''
        mock_client = create_s3_client
        ingestion_bucket_name = 'mock-test-ingestion-va-052023'
        mock_client.create_bucket(
             Bucket=ingestion_bucket_name, 
             CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
             )
        with open('tests/transform/test_data_csv_files/test_read.csv', 'rb') as data:
            mock_client.upload_fileobj(data, ingestion_bucket_name, 'test.csv')
        yield mock_client


def test_function_returns_dateframe_of_csv_file_content_from_bucket(mock_client):

    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    test_df = read_csv_to_pandas('test', ingestion_bucket_name)

    assert_frame_equal(test_df, df)


def test_function_raises_exception_when_agruments_invalid(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'

    with pytest.raises(Exception):
        read_csv_to_pandas('wrong', ingestion_bucket_name)
    
    with pytest.raises(Exception):
        read_csv_to_pandas('test', 'wrong')