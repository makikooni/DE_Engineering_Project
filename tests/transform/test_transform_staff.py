# import moto.core
from moto import mock_s3
import boto3
import pytest
import awswrangler as wr
from utils.table_transformations import transform_staff

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
        with open('tests/transform/test_data_csv_files/test_staff.csv', 'rb') as data:
            mock_client.upload_fileobj(data, ingestion_bucket_name, 'test_1.csv')
        with open('tests/transform/test_data_csv_files/test_department.csv', 'rb') as data:
            mock_client.upload_fileobj(data, ingestion_bucket_name, 'test_2.csv')
        yield mock_client


def test_transform_staff_retrieves_csv_file_from_ingestion_s3_bucket_and_puts_parquet_file_in_processed_s3_bucket(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    processed_bucket_name = 'mock-test-processed-va-052023'
    transform_staff('test_1', 'test_2', ingestion_bucket_name, processed_bucket_name)

    assert len(mock_client.list_objects_v2(Bucket=processed_bucket_name)['Contents']) == 1
    assert mock_client.list_objects_v2(Bucket=processed_bucket_name)['Contents'][0]['Key'] == 'dim_staff.parquet'


def test_transform_staff_transforms_tables_into_correct_parquet_shchema(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    processed_bucket_name = 'mock-test-processed-va-052023'
    transform_staff('test_1', 'test_2', ingestion_bucket_name, processed_bucket_name)
    df = wr.s3.read_parquet(path=f's3://{processed_bucket_name}/dim_staff.parquet')
    assert len(df) == 3
    assert list(df.columns) == ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']


def test_transform_staff_raises_exception_when_agruments_invalid(mock_client):

    ingestion_bucket_name = 'mock-test-ingestion-va-052023'
    processed_bucket_name = 'mock-test-processed-va-052023'

    with pytest.raises(Exception):
        transform_staff('wrong', 'test_2', ingestion_bucket_name, processed_bucket_name)

    with pytest.raises(Exception):
        transform_staff('test_1', 'wrong', ingestion_bucket_name, processed_bucket_name)

    with pytest.raises(Exception):
        transform_staff('test_1', 'test_2', 'wrong', processed_bucket_name)

    with pytest.raises(Exception):
        transform_staff('test_1', 'test_2', ingestion_bucket_name, 'wrong')