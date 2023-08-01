import pytest
import boto3
from moto import mock_s3
from pprint import pprint
from src.transform import transformation_lambda_handler


def test_testing():
    '''
    '''
    transformation_lambda_handler(1,1)
    assert True


# @mock_s3
# def test_function_connects_to_ingestion_s3_bucket(setup):
#     '''
#     this test should demonstrate the transformation_lambda_handler 
#     function is able to connect to the ingestion s3 bucket
#     '''
#     s3_client = boto3.client('s3')
#     response = s3_client.head_bucket(Bucket=ingestion_bucket_name)
#     status_code = response['ResponseMetadata']['HTTPStatusCode']
#     assert transformation_lambda_handler(1, 1) == 200


@mock_s3
def test_function_identifies_file_changes_within_ingestion_s3_bucket():
    '''
    this test should demonstrate the transformation_lambda_handler function 
    is correctly identify changes in files within the ingestion s3 bucket
    '''
    pass


@mock_s3
def test_function_can_retrieve_data_from_ingestion_s3_bucket():
    '''
    this test demonstrates the transformation_lambda_handler functions 
    ability to retrieve data stored within the ingestion s3 bucket
    '''
    pass


def test_raises_excpetion_when_bucket_does_not_exist():
    '''
    demonstrates the function raises an exception 
    when passed an invalid bucket name
    '''
    pass


@mock_s3
def test_function_connects_to_processed_s3_bucket():
    '''
    this test should demonstrate the transformation_lambda_handler 
    function is able to connect to the processed s3 bucket
    '''
    pass


def test_processed_bucket_contains_correct_parquet_files():
    '''
    test ensures the bucket we're depositing our 
    parquet files into recieves the relevant files
    '''
    
    pass