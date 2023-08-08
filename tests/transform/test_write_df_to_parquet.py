# import moto.core
from moto import mock_s3
from datetime import datetime
import boto3
import pytest
from pandas.testing import assert_frame_equal
import awswrangler as wr
import pandas as pd
from pprint import pprint
from utils.utils import write_df_to_parquet


@pytest.fixture
def create_s3_client():
    with mock_s3():
        yield boto3.client('s3', region_name='eu-west-2')


@pytest.fixture
def mock_client(create_s3_client):
        '''
        fixture creates 'mock-test-processed-va-052023' bucket in 
        mock aws account and uploads 'test.csv' file to it
        '''
        mock_client = create_s3_client
        processed_bucket_name = 'mock-test-processed-va-052023'
        mock_client.create_bucket(
             Bucket=processed_bucket_name, 
             CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
             )
        yield mock_client


def test_function_correctly_converts_dataframe_to_parquet(mock_client):

    test_df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    processed_bucket_name = 'mock-test-processed-va-052023'
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    write_df_to_parquet(test_df, 'test_write_parquet', processed_bucket_name, timestamp)

    retrieved_df = wr.s3.read_parquet(path=f's3://{processed_bucket_name}/{timestamp}/test_write_parquet.parquet')
    
    assert len(retrieved_df) == len(test_df)
    assert retrieved_df.columns[0] == test_df.columns[0]
    assert_frame_equal(retrieved_df, test_df, check_dtype=False)


def test_function_puts_dataframe_converted_to_parquet_in_processed_bucket(mock_client):

    test_df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    processed_bucket_name = 'mock-test-processed-va-052023'
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    write_df_to_parquet(test_df, 'test_write_parquet', processed_bucket_name, timestamp)

    assert len(mock_client.list_objects_v2(Bucket=processed_bucket_name)['Contents']) == 1
    assert mock_client.list_objects_v2(Bucket=processed_bucket_name)['Contents'][0]['Key'] == f'{timestamp}/test_write_parquet.parquet'


def test_function_raises_exception_when_agruments_invalid(mock_client):

    processed_bucket_name = 'mock-test-processed-va-052023'
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    with pytest.raises(Exception):
        write_df_to_parquet('wrong', processed_bucket_name, timestamp)
    
    with pytest.raises(Exception):
        write_df_to_parquet('test', 'wrong', timestamp)