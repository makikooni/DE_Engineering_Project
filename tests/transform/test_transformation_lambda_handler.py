import pytest
import boto3
from moto import mock_s3
from unittest.mock import patch
from pprint import pprint
from src.transform import transformation_lambda_handler

# @pytest.fixture
# def create_s3_client():
#     with mock_s3():
#         yield boto3.client('s3', region_name='eu-west-2')


# @pytest.fixture
# def mock_client(create_s3_client):
#         '''
#         fixture creates 'test-ingestion-va-052023' bucket in 
#         mock aws account and uploads 'test.txt' file to it
#         '''
#         mock_client = create_s3_client
#         ingestion_bucket_name = 'mock-test-ingestion-va-052023'
#         processed_bucket_name = 'mock-test-processed-va-052023'
#         mock_client.create_bucket(
#              Bucket=ingestion_bucket_name, 
#              CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
#              )
#         # mock_client.create_bucket(
#         #      Bucket=processed_bucket_name, 
#         #      CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
#         #      )
#         # with open('tests/transform/test_data_csv_files/test_sales_order.csv', 'rb') as data:
#         #     mock_client.upload_fileobj(data, ingestion_bucket_name, 'test.csv')
#         yield mock_client

# @patch('src.utils.table_transformations.transform_design')
# def test_function_connects_to_ingestion_s3_bucket(mock_client):
#     '''
#     this test should demonstrate the transformation_lambda_handler 
#     function is able to connect to the ingestion s3 bucket
#     '''
#     ingestion_bucket_name = 'mock-test-ingestion-va-052023'

#     s3_client = boto3.client('s3')
#     response = s3_client.head_bucket(Bucket=ingestion_bucket_name)
#     status_code = response['ResponseMetadata']['HTTPStatusCode']
#     assert status_code == 200

# @patch('src.transform.transformation_lambda_handler')
# def test_function_calls_all_table_transformation_functions(mock_client):
#     with patch('src.utils.table_transformations.transform_design') as transform_design_mock:
#         transformation_lambda_handler(1, 1)
#         assert transform_design_mock.call_count == 1

         

# @mock_s3
# def test_function_identifies_file_changes_within_ingestion_s3_bucket():
#     '''
#     this test should demonstrate the transformation_lambda_handler function 
#     is correctly identify changes in files within the ingestion s3 bucket
#     '''
#     pass


# def test_raises_excpetion_when_bucket_does_not_exist():
#     '''
#     demonstrates the function raises an exception 
#     when passed an invalid bucket name
#     '''
#     pass
