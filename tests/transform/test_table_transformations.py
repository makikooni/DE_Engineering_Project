import logging
import boto3
from utils.table_transformations import transform_counterparty, transform_currency, transform_design, transform_location, transform_payment, transform_payment_type, transform_purchase_order, transform_sales_order, transform_staff, transform_transaction, create_date
from moto import mock_s3
from pprint import pprint
from transform import transformation_lambda_handler
import pytest


@pytest.fixture(scope='function')
def ingestion_s3():
    with mock_s3():
        '''
        fixture creates 'test-ingestion-va-052023' bucket in 
        mock aws account and uploads 'test.txt' file to it
        '''
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test-ingestion-va-052023', CreateBucketConfiguration={'LocationConstraint': 'us-west-1'})
        with open('/Users/angushirst/Northcoders/week-11-project/teststuff/test.txt', 'rb') as data:
            s3.upload_fileobj(data, 'test-ingestion-va-052023', 'test.txt')
        yield s3


@pytest.fixture(scope='function')
def processed_s3():
    with mock_s3():
        '''
        fixture creates 'test-processed-va-052023' bucket in mock aws account
        '''
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test-processed-va-052023', CreateBucketConfiguration={'LocationConstraint': 'us-west-1'})
        yield s3

# @mock_s3
def test_transform_design_retrieves_file_from_ingestion_s3_bucket(ingestion_s3, processed_s3):
    pprint(ingestion_s3.list_objects(Bucket='test-ingestion-va-052023'))
    # pprint(processed_s3.list_objects(Bucket='test-processed-va-052023'))
    assert Falsegi


def test_transform_design_transforms_tables_into_correct_parquet_shchema():

    pass


def test_transform_design_puts_parquet_file_into_processed_s3_bucket():

    pass


def test_transform_design_raises_exception_when_agruments_invalid():
    
    pass