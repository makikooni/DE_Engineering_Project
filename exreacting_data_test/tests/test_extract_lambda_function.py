from exreacting_data_test.extract_lambda_function import put_table
import pytest
import boto3
from moto import (
    mock_s3, 
    mock_secretsmanager
    )
from pprint import pprint

secret_string = '{"username":"project_user_3","password":"I4NX4jLv8i9VdeeM43uWBKPV","engine":"postgres","host":"nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com","port":"5432","dbname":"totesys"}'
def test_testing_function_imported_correctly():
    assert callable(put_table)

def test_returns_error_when_passed_invalid_table_name():
    with pytest.raises(Exception) as e:
        put_table('123dhdhdh')

@mock_s3
@mock_secretsmanager
def test_adds_object_to_s3_bucket():
    # create the secret
    conn = boto3.client('s3')
    conn.create_bucket(
        Bucket='test_bucket',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
    })
    
    boto3.client("secretsmanager").create_secret(
        Name="test_secret",
        SecretString= secret_string 
        )
    put_table(tableName='design',bucketName='test_bucket',secretName='test_secret')
    obj = conn.list_objects_v2(Bucket='test_bucket')
    assert obj['Contents'][0]['Key']== 'design'

@mock_s3
@mock_secretsmanager
def test_for_incorrect_bucket_name():
    conn = boto3.client('s3')
    # to break the test
    # conn.create_bucket(
    #     Bucket='no_bucket',
    #     CreateBucketConfiguration={
    #     'LocationConstraint': 'eu-west-2',
    # })
    boto3.client("secretsmanager").create_secret(
        Name="test_secret",
        SecretString= secret_string 
        )
    with pytest.raises(Exception) as e:
        put_table(tableName='design', bucketName='no_bucket', secretName='test_secret')
    assert 'Not a valid bucket' in str(e.value) 

@mock_secretsmanager
def test_for_secret_not_found():
    # make secret with bad credentials
    # boto3.client("secretsmanager").create_secret(
    #     Name="test_secret",
    #     SecretString= '{"username":"project_user","password":"I4NX4jLv8i9VdeeM4","engine":"posts","host":"nc-data-eng-totesys-production.chp8h1nu.eu-west-2.rds.amazonaws.com","port":"5323","dbname":"tosys"}'
    #     )
    with pytest.raises(Exception) as e:
        put_table(tableName='design', bucketName='test_bucket', secretName='test_secret')
    print(e)
    assert "An error occurred (ResourceNotFoundException) when calling the GetSecretValue operation: Secrets Manager can't find the specified secret."  in str(e.value) 

@mock_secretsmanager
def test_for_secret_with_bad_credentials():
    # make secret with bad credentials
    boto3.client("secretsmanager").create_secret(
        Name="test_secret",
        SecretString= '{"username":"project_user","password":"I4NX4jLv8i9VdeeM4","engine":"posts","host":"nc-data-eng-totesys-production.chp8h1nu.eu-west-2.rds.amazonaws.com","port":"5323","dbname":"tosys"}'
        )
    with pytest.raises(Exception) as e:
        put_table(tableName='design', bucketName='test_bucket', secretName='test_secret')
    print(e)
    assert "Error connecting to the database"  in str(e.value) 
    