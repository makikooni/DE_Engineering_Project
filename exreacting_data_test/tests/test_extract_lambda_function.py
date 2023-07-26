from exreacting_data_test.extract_lambda_function import put_table
import pytest
import boto3
from moto import mock_s3
from pprint import pprint

def test_testing_function_imported_correctly():
    assert callable(put_table)

def test_returns_error_when_passed_invalid_table_name():
    with pytest.raises(Exception) as e:
        put_table('123dhdhdh')

@mock_s3
def test_adds_object_to_s3_bucket():
    conn = boto3.client('s3')
    conn.create_bucket(
        Bucket='test_bucket',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
    })
    put_table(tableName='design', bucketName='test_bucket')
    obj = conn.list_objects_v2(Bucket='test_bucket')
    assert obj['Name'] == 'test_bucket'

@mock_s3
def test_for_incorrect_bucket_name():
    conn = boto3.client('s3')
    
    # with pytest.raises(Exception) as e:
    err = put_table(tableName='design', bucketName='test_bucket')
    print(err)