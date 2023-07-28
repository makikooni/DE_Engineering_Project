import json
import logging
from pprint import pprint
import pg8000
import boto3
import pandas as pd
from botocore.exceptions import ClientError

def load_data(secret_name = 'warehouse'):
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
        connection = pg8000.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        desgin_table = pd.read_parquet(f's3://{process_bucket}/test_dim_design.parquet')
        
    except Exception as _e:
        raise _e

load_data()
