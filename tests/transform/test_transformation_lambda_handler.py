import pytest
import boto3
from moto import mock_s3
from pprint import pprint
from src.transform.transform import transformation_lambda_handler


def test_testing():
    '''
    '''
    transformation_lambda_handler()
    assert True

# @mock_s3
# def test_function_connects_to_ingestion_s3_bucket(setup):
#     '''
#     this test should demonstrate the transformation_lambda_handler 
#     function is able to connect to the ingestion s3 bucket
#     '''
#     pprint(transformation_lambda_handler(1, 1))
#     assert transformation_lambda_handler(1, 1) == 200

def test_bucket_contains_correct_files():
    '''
    test ensures the bucket we're accessing contains the relevant files
    '''
    
    pass


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
    this test should demonstrate the transformation_lambda_handler function 
    is able to retrieve data stored within the ingestion s3 bucket
    '''
    pass


def test_raises_excpetion_when_bucket_does_not_exist():
    '''
    '''
    pass

@mock_s3
def test_function_connects_to_processed_s3_bucket():
    '''
    this test should demonstrate the transformation_lambda_handler 
    function is able to connect to the processed s3 bucket
    '''
    pass