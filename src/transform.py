import logging
from datetime import datetime
from utils.table_transformations import (
    transform_design, transform_payment_type,
    transform_location, transform_transaction,
    transform_currency, transform_staff,
    transform_payment, transform_purchase_order,
    transform_counterparty, transform_sales_order,
    create_date)
from utils.utils import get_last_job_timestamp, log_latest_job_transform
from botocore.exceptions import ClientError

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def transform_lambda_handler(event, context):
    """
    Entry point for the transformation Lambda function: triggered by a put object event within the s3 ingestion bucket.
    It processes the data through a series of transformation functions and stores the results in the s3 processed bucket.


    Args:
        event (dict): The event data; containing information about the triggered S3 object. The expected data structure follows:
            {
                'Records': [
                    {
                        's3': {
                            'bucket': {'name': 'bucket-name'},
                            'object': {'key': 'object-key'}
                        }
                    }
                ]
            }

        context (LambdaContext): 
            Runtime information about the lambda function.

    Returns:
        None.
    
    Raises:
        ValueError: 
            Raised in the case when event_bucket_name does not match INGESTION_BUCKET_NAME or when 
            event_obj_name (derived from the event object) does not conform to the expected filename structure.

        ClientError: 
            Raised if an issue with s3 operations arises.
    """
    INGESTION_BUCKET_NAME = "ingestion-va-052023"
    PROCESSED_BUCKET_NAME = 'processed-va-052023'

    event_bucket_name = event['Records'][0]['s3']['bucket']['name']
    event_obj_name = event['Records'][0]['s3']['object']['key']

    if event_bucket_name != INGESTION_BUCKET_NAME:
        raise ValueError(f'Lambda triggered by {event_bucket_name} bucket,\
                         expected {INGESTION_BUCKET_NAME} bucket')
    if event_obj_name[-3:] != 'txt' and \
            'ExtractHistory' not in event_obj_name:
        raise ValueError('Wrong extraction trigger file')

    last_timestamp = get_last_job_timestamp(INGESTION_BUCKET_NAME)
    EXTRACT_JOB_TIMESTAMP = last_timestamp.strftime("%Y%m%d%H%M%S")
    try:
        dates_for_dim_date = set()

        transform_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        transform_design(
            f'{EXTRACT_JOB_TIMESTAMP}/design',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            transform_timestamp)
        transform_payment_type(
            f'{EXTRACT_JOB_TIMESTAMP}/payment_type',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            transform_timestamp)
        transform_location(
            f'{EXTRACT_JOB_TIMESTAMP}/address',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            transform_timestamp)
        transform_transaction(
            f'{EXTRACT_JOB_TIMESTAMP}/transaction',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            transform_timestamp)
        transform_staff(
            f'{EXTRACT_JOB_TIMESTAMP}/staff',
            f'{EXTRACT_JOB_TIMESTAMP}/department',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            transform_timestamp)
        transform_currency(
            f'{EXTRACT_JOB_TIMESTAMP}/currency',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            transform_timestamp)
        transform_counterparty(
            f'{EXTRACT_JOB_TIMESTAMP}/counterparty',
            f'{EXTRACT_JOB_TIMESTAMP}/address',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            transform_timestamp)
        transform_sales_order(
            f'{EXTRACT_JOB_TIMESTAMP}/sales_order',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            dates_for_dim_date,
            transform_timestamp)
        transform_purchase_order(
            f'{EXTRACT_JOB_TIMESTAMP}/purchase_order',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            dates_for_dim_date,
            transform_timestamp)
        transform_payment(
            f'{EXTRACT_JOB_TIMESTAMP}/payment',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            dates_for_dim_date,
            transform_timestamp)
        create_date(
            dates_for_dim_date,
            PROCESSED_BUCKET_NAME,
            transform_timestamp)

        log_latest_job_transform(PROCESSED_BUCKET_NAME, transform_timestamp)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.error('Bucket does not exist')
            raise e
        if e.response['Error']['Code'] == '403':
            logger.error('Invalid permissions for bucket')
        else:
            logger.error('transform_lambda_handler ')
            raise e
