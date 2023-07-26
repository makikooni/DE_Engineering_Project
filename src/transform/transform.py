
import os
import logging
import boto3
from pprint import pprint
import csv
import pandas as pd

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def transformation_lambda_handler():
    # pd.set_option('display.max_columns', None)
    ingestion_bucket_name = 'test-va-0423'
    try:
        s3_client = boto3.client('s3')
        s3_resource = boto3.resource('s3')
        response = s3_client.head_bucket(Bucket=ingestion_bucket_name)
        status_code = response['ResponseMetadata']['HTTPStatusCode']
        if status_code != 200:
            raise Exception('the bucket may not exist, or, you may not have the correct permissions')
        
        design_table = pd.read_csv(f's3://{ingestion_bucket_name}/for_room_2')
        pprint(design_table)





    except Exception as e:
        print('except')
        print(e)
        pass

'''
- connect with the relevant bucket

- list bucket file contents, assuming connection was successful

- 


'''








'''

- function will trigger whenever a new file appears in ingestion s3 bucket (check sprint)

- connect to ingestion s3 bucket

- fetch data from ingestion s3 bucket

- transform data into fact & dimension tables (star schema)

- upload data to processer in parquet format



'''