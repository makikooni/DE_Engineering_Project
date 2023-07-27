'''
Defines lambda function responsible for extracting the data
from the database and depositing it in the ingestion bucket
'''

import os
import logging
import boto3

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def extraction_lambda_handler(event, context):
    logger.info(f'Hello World!')
    raise NameError
    return {
        'statusCode': 200
    }
