from moto import mock_s3
from datetime import datetime
import boto3
import pytest
import awswrangler as wr
from utils.table_transformations import transform_currency

@pytest.fixture
def create_s3_client():
    with mock_s3():
        yield boto3.client('s3', region_name='eu-west-2')


@pytest.fixture
def mock_client(create_s3_client):
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
        with open('tests/transform/test_data_csv_files/test_currency.csv', 'rb') as data:
            mock_client.upload_fileobj(data, ingestion_bucket_name, 'test.csv')
        yield mock_client


def test_transform_currency_retrieves_csv_file_from_ingestion_s3_bucket_and_puts_parquet_file_in_processed_s3_bucket(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    processed_bucket_name = 'mock-test-processed-va-052023'
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    transform_currency('test', ingestion_bucket_name, processed_bucket_name, timestamp)

    assert len(mock_client.list_objects_v2(Bucket=processed_bucket_name)['Contents']) == 1
    assert mock_client.list_objects_v2(Bucket=processed_bucket_name)['Contents'][0]['Key'] == f'{timestamp}/dim_currency.parquet'


def test_transform_currency_transforms_tables_into_correct_parquet_shchema(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    processed_bucket_name = 'mock-test-processed-va-052023'
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    transform_currency('test', ingestion_bucket_name, processed_bucket_name,timestamp)
    df = wr.s3.read_parquet(path=f's3://{processed_bucket_name}/{timestamp}/dim_currency.parquet')
    assert len(df) == 3
    assert list(df.columns) == ['currency_id', 'currency_code', 'currency_name']


def test_transform_currency_raises_exception_when_agruments_invalid(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    processed_bucket_name = 'mock-test-processed-va-052023'
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    with pytest.raises(Exception):
        transform_currency('wrong', ingestion_bucket_name, processed_bucket_name, timestamp)
    
    with pytest.raises(Exception):
        transform_currency('test', 'wrong', processed_bucket_name, timestamp)

    with pytest.raises(Exception):
        transform_currency('test', ingestion_bucket_name, 'wrong', timestamp)