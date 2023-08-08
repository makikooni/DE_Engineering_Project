# import moto.core
from datetime import datetime
from moto import mock_s3
import boto3
import pytest
import awswrangler as wr
from utils.table_transformations import transform_location

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
        with open('tests/transform/test_data_csv_files/test_address.csv', 'rb') as data:
            mock_client.upload_fileobj(data, ingestion_bucket_name, 'test.csv')
        yield mock_client


def test_transform_location_retrieves_csv_file_from_ingestion_s3_bucket_and_puts_parquet_file_in_processed_s3_bucket(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    processed_bucket_name = 'mock-test-processed-va-052023'
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    transform_location('test', ingestion_bucket_name, processed_bucket_name, timestamp)

    assert len(mock_client.list_objects_v2(Bucket=processed_bucket_name)['Contents']) == 1
    assert mock_client.list_objects_v2(Bucket=processed_bucket_name)['Contents'][0]['Key'] == f'{timestamp}/dim_location.parquet'


def test_transform_location_transforms_tables_into_correct_parquet_shchema(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    processed_bucket_name = 'mock-test-processed-va-052023'
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    transform_location('test', ingestion_bucket_name, processed_bucket_name, timestamp)
    df = wr.s3.read_parquet(path=f's3://{processed_bucket_name}/{timestamp}/dim_location.parquet')
    assert len(df) == 3
    assert list(df.columns) == ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']


def test_transform_location_raises_exception_when_agruments_invalid(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    processed_bucket_name = 'mock-test-processed-va-052023'
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    with pytest.raises(Exception):
        transform_location('wrong', ingestion_bucket_name, processed_bucket_name, timestamp)
    
    with pytest.raises(Exception):
        transform_location('test', 'wrong', processed_bucket_name, timestamp)

    with pytest.raises(Exception):
        transform_location('test', ingestion_bucket_name, 'wrong', timestamp)