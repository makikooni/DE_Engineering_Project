from exreacting_data_test.extract_lambda_function import put_table
import pytest
import boto3
from moto import (
    mock_s3, 
    mock_secretsmanager
    )
from pprint import pprint


def test_testing_function_imported_correctly():
    assert callable(put_table)

def test_returns_error_when_passed_invalid_table_name():
    with pytest.raises(Exception) as e:
        put_table('123dhdhdh')

@mock_s3
def test_adds_object_to_s3_bucket():
    # create the secret
    conn = boto3.client('s3')
    conn.create_bucket(
        Bucket='test_bucket',
        CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
    })
    put_table(tableName='design', bucketName='test_bucket')
    obj = conn.list_objects_v2(Bucket='test_bucket')
    pprint(obj)
    assert obj['Name'] == 'test_bucke'

# @mock_s3
# def test_for_incorrect_bucket_name():
#     conn = boto3.client('s3')
#     # with pytest.raises(Exception) as e:
#     err = put_table(tableName='design', bucketName='test_bucket')
#     print(err)

# @mock_secretsmanager
# def test_for():
#     # make secret with bad credentials
#     client = boto3.client('secretsmanager')
#     secrets = client.list_secrets()
#     pprint(secrets)
#     # client.create_secret(
#     #     ClientRequestToken = f'${uuid.uuid1()}',
#     #     Description='My secret',
#     #     Name=f'{secret_identifier}',
#     #     SecretString=secret_string
#     # )
#     # try to push new object to the bucket
#     # spy on the console for connection to db not established
#     assert 0