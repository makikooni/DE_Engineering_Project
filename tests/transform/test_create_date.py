from moto import mock_s3
from datetime import datetime
import boto3
import pytest
import awswrangler as wr
from utils.table_transformations import create_date


@pytest.fixture
def create_s3_client():

    with mock_s3():
        yield boto3.client('s3', region_name='eu-west-2')


@pytest.fixture
def mock_client(create_s3_client):

        mock_client = create_s3_client
        processed_bucket_name = 'mock-test-processed-va-052023'
        mock_client.create_bucket(
             Bucket=processed_bucket_name, 
             CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
             )
        yield mock_client


def test_create_date_puts_parquet_file_in_processed_s3_bucket(mock_client):

    test_set = set(['2022-12-01', '2022-12-20', '2023-06-14','2023-08-01'])

    processed_bucket_name = 'mock-test-processed-va-052023'
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    create_date(test_set, processed_bucket_name, timestamp)

    assert len(mock_client.list_objects_v2(Bucket=processed_bucket_name)['Contents']) == 1
    assert mock_client.list_objects_v2(Bucket=processed_bucket_name)['Contents'][0]['Key'] == f'{timestamp}/dim_date.parquet'


def test_create_date_transforms_set_data_into_correct_parquet_schema(mock_client):
    
    test_set = set(['2022-12-01', '2022-12-20', '2023-06-14','2023-08-01'])
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    processed_bucket_name = 'mock-test-processed-va-052023'
    create_date(test_set, processed_bucket_name, timestamp)
    df = wr.s3.read_parquet(path=f's3://{processed_bucket_name}/{timestamp}/dim_date.parquet')
    assert len(df) == 4
    assert list(df.columns) == ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']


def test_transform_design_raises_exception_when_agruments_invalid(mock_client):

    test_set = set(['2022-12-01', '2022-12-20', '2023-06-14','2023-08-01'])

    processed_bucket_name = 'mock-test-processed-va-052023'
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    with pytest.raises(Exception):
        create_date('wrong', processed_bucket_name, timestamp)
    
    with pytest.raises(Exception):
        create_date(test_set, 'wrong', timestamp)