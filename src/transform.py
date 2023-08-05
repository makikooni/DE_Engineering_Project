import logging
from utils.table_transformations import (
    transform_design, transform_payment_type,
    transform_location, transform_transaction,
    transform_currency, transform_staff,
    transform_payment, transform_purchase_order,
    transform_counterparty, transform_sales_order,
    create_date)
from botocore.exceptions import ClientError

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def transform_lambda_handler(event, context):
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
    try:
        dates_for_dim_date = set()

        transform_design(
            'design',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME)
        transform_payment_type(
            'payment_type',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME)
        transform_location(
            'address',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME)
        transform_transaction(
            'transaction',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME)
        transform_staff(
            'staff',
            'department',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME)
        transform_currency(
            'currency',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME)
        transform_counterparty(
            'counterparty',
            'address',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME)
        transform_sales_order(
            'sales_order',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            dates_for_dim_date)
        transform_purchase_order(
            'purchase_order',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            dates_for_dim_date)
        transform_payment(
            'payment',
            INGESTION_BUCKET_NAME,
            PROCESSED_BUCKET_NAME,
            dates_for_dim_date)
        create_date(dates_for_dim_date, PROCESSED_BUCKET_NAME)

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.error('Bucket does not exist')
            raise e
        if e.response['Error']['Code'] == '403':
            logger.error('Invalid permissions for bucket')
        else:
            logger.error('transform_lambda_handler ')
            raise e
###############################
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)


# def transform_lambda_handler(event, context):
#     logger.info("#=#=#=#=#=#=#=# TRANSFORM LAMBDA =#=#=#=#=#=#=#=#")
#     logger.info("Hello from the Transform Lambda :)")

# import json
# with open("tests/transform/test_valid_event.json") as v:
#     event = json.loads(v.read())
# transform_lambda_handler(event, {})