import json
import logging
import boto3
import pandas as pd
from sqlalchemy import create_engine
from botocore.exceptions import ClientError
def update_table(table_name, secret_name = 'warehouse'):
    # get warehouse credentails from AWS secrets
    secretsmanager = boto3.client('secretsmanager')
    try:
        db_credentials = secretsmanager.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as _e:
        if _e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("in error")
            raise Exception("The requested secret was not found")
        else:
            print(_e)
   
    try:
        # conecting to the process bucket
        process_bucket = 'processed-va-052023'
        s3_resource = boto3.resource('s3')   
        s3_client = boto3.client('s3')
        response = s3_client.head_bucket(Bucket=process_bucket)
        # credentails for the warehouse
        db_creds = json.loads(db_credentials['SecretString'])
        host = db_creds['host']
        port = db_creds['port']
        database = db_creds['dbname']
        user = db_creds['username']
        password = db_creds['password']
        # connecting to warehouse
        warehouse_db = create_engine('postgresql://' + user + ':' + password + '@' + \
            host + ':' + port + '/' + database , echo = "debug")
        # get the table from the s3 and put it in a pandas dataframe
        table = pd.read_parquet(f's3://{process_bucket}/{table_name}')
        pd.DataFrame({table})
        
    except Exception as _e:
        raise _e

update_table('test_dim_design.parquet')
