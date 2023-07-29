# from src.extract.extract import extraction_lambda_handler
# import pytest
# import boto3
# from moto import (
#     mock_s3, 
#     mock_secretsmanager
#     )

# def test_lambda_should_return_dict_with_key_status_and_code_200():
#     '''
#         example:
#         extraction_lambda_handler(event,context)    // {"statusCode": 200}
#     '''

#     pass

# def 





















# secret_string = '{"username":"project_user_3","password":"I4NX4jLv8i9VdeeM43uWBKPV","engine":"postgres","host":"nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com","port":"5432","dbname":"totesys"}'

# @mock_secretsmanager
# def test_for_secret_not_found():
#     # make secret with bad credentials
#     # boto3.client("secretsmanager").create_secret(
#     #     Name="test_secret",
#     #     SecretString= '{"username":"project_user","password":"I4NX4jLv8i9VdeeM4","engine":"posts","host":"nc-data-eng-totesys-production.chp8h1nu.eu-west-2.rds.amazonaws.com","port":"5323","dbname":"tosys"}'
#     #     )
#     with pytest.raises(Exception) as e:
#         extraction_lambda_handler(1,2)
    
#     assert "The requested secret was not found"  in str(e.value) 

# @mock_secretsmanager
# def test_for_secret_with_bad_credentials():
#     # make secret with bad credentials
#     boto3.client("secretsmanager").create_secret(
#         Name="test_secret",
#         SecretString= '{"username":"project_user","password":"I4NX4jLv8i9VdeeM4","engine":"posts","host":"nc-data-eng-totesys-production.chp8h1nu.eu-west-2.rds.amazonaws.com","port":"5323","dbname":"tosys"}'
#         )
#     with pytest.raises(Exception) as e:
#         extraction_lambda_function(tableName='design', bucketName='test_bucket', secretName='test_secret')
#     print(e)
#     assert "Error connecting to the database"  in str(e.value) 
    
# @mock_s3
# @mock_secretsmanager
# def test_adds_object_to_s3_bucket():
#     # create the secret
#     conn = boto3.client('s3')
#     conn.create_bucket(
#         Bucket='test_bucket',
#         CreateBucketConfiguration={
#         'LocationConstraint': 'eu-west-2',
#     })
    
#     boto3.client("secretsmanager").create_secret(
#         Name="test_secret",
#         SecretString= secret_string 
#         )
#     extraction_lambda_handler(event, context)
#     obj = conn.list_objects_v2(Bucket='test_bucket')
#     assert obj['Contents'][0]['Key']== 'design'

# @mock_s3
# @mock_secretsmanager
# def test_for_incorrect_bucket_name():
#     conn = boto3.client('s3')
#     # to break the test
#     # conn.create_bucket(
#     #     Bucket='no_bucket',
#     #     CreateBucketConfiguration={
#     #     'LocationConstraint': 'eu-west-2',
#     # })
#     boto3.client("secretsmanager").create_secret(
#         Name="test_secret",
#         SecretString= secret_string 
#         )
#     with pytest.raises(Exception) as e:
#         extraction_lambda_function(tableName='design', bucketName='no_bucket', secretName='test_secret')
#     assert 'Not a valid bucket' in str(e.value) 
