import moto.core 
import pytest
import boto3
from moto import mock_s3
import pandas as pd
from src.utils.table_transformations import transform_payment_type


@pytest.fixture
def create_s3_client():
    with mock_s3():
        yield boto3.client('s3', region_name='eu-west-2')


@pytest.fixture
def mock_client(create_s3_client):
        '''
        fixture creates 'test-ingestion-va-052023' bucket in 
        mock aws account and uploads 'test.txt' file to it
        '''
        mock_client = create_s3_client
        ingestion_bucket_name = 'mock-test-ingestion-va-052023'
        processed_bucket_name = 'mock-test-processed-va-052023'
        mock_client.create_bucket(
             Bucket=ingestion_bucket_name, 
             CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
             )
        mock_client.create_bucket(
             Bucket=processed_bucket_name, 
             CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
             )
        with open('tests/transform/test_data_csv_files/test_payment_type.csv', 'rb') as data:
            mock_client.upload_fileobj(data, ingestion_bucket_name, 'test.csv')
        yield mock_client


def test_transform_payment_type_retrieves_csv_file_from_ingestion_s3_bucket_and_puts_parquet_file_in_processed_s3_bucket(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    processed_bucket_name = 'mock-test-processed-va-052023'
    transform_payment_type('test', ingestion_bucket_name, processed_bucket_name)

    assert len(mock_client.list_objects_v2(Bucket=processed_bucket_name)['Contents']) == 1
    assert mock_client.list_objects_v2(Bucket=processed_bucket_name)['Contents'][0]['Key'] == 'dim_payment_type.parquet'


def test_transform_payment_type_transforms_tables_into_correct_parquet_shchema(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    processed_bucket_name = 'mock-test-processed-va-052023'
    transform_payment_type('test', ingestion_bucket_name, processed_bucket_name)
    df = pd.read_parquet(f's3://{processed_bucket_name}/dim_payment_type.parquet')
    pprint(df)
    assert len(df) == 3
    assert list(df.columns) == ['payment_type_id', 'payment_type_name']


def test_transform_payment_type_raises_exception_when_agruments_invalid(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    processed_bucket_name = 'mock-test-processed-va-052023'

    with pytest.raises(Exception):
        transform_payment_type('wrong', ingestion_bucket_name, processed_bucket_name)
    
    with pytest.raises(Exception):
        transform_payment_type('test', 'wrong', processed_bucket_name)

    with pytest.raises(Exception):
        transform_payment_type('test', ingestion_bucket_name, 'wrong')