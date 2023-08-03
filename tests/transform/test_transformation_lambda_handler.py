from moto import mock_s3
from moto.core import patch_client
import pytest
import boto3
import unittest
from unittest.mock import patch, Mock, MagicMock
from pprint import pprint
from src.transform import transformation_lambda_handler
from src.utils.table_transformations import transform_design

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
        with open('tests/transform/test_data_csv_files/test_sales_order.csv', 'rb') as data:
            mock_client.upload_fileobj(data, ingestion_bucket_name, 'test.csv')
        yield mock_client

# def test_function_connects_to_ingestion_s3_bucket(mock_client):
#     with patch('src.transform.transformation_lambda_handler') as mock_lambda:
#         with patch('src.utils.table_transformations.transform_design') as mock_transform_design:
#             mock_lambda()

# class test_transformation_lambda_handler(unittest.TestCase):
#      def test_ensures_internal_methods_are_each_called_once(mock_client):
#           with patch('src.transform.transform_design') as mock_transformation_design, \
#             patch('src.transform.transform_design.read_csv_to_pandas'):
#                transformation_lambda_handler()
#                assert mock_transformation_design.call_count == 1

# @mock_client
class test_transformation_lambda_handler(unittest.TestCase):
     def test_ensures_internal_methods_are_each_called_once(self):
          with patch('src.transform.transform_design') as mock_transform_design:
               transformation_lambda_handler()
               assert mock_transform_design.call_count == 1

         

# @mock_s3
# def test_function_identifies_file_changes_within_ingestion_s3_bucket():
#     '''
#     this test should demonstrate the transformation_lambda_handler function 
#     is correctly identify changes in files within the ingestion s3 bucket
#     '''
#     pass


# def test_raises_excpetion_when_bucket_does_not_exist(mock_client):
#   '''
#   demonstrates the function raises an exception when passed an invalid bucket name
#   '''