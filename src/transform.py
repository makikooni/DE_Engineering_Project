import logging
import boto3
from src.utils.table_transformations import transform_design, transform_payment_type, transform_location, transform_transaction, transform_currency, transform_staff, transform_payment, transform_purchase_order, transform_counterparty, transform_sales_order, create_date
from pprint import pprint
from botocore.exceptions import ClientError

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def transformation_lambda_handler(event, context=1):
    # run in terminal to view pq table --> parquet-tools show s3://{target_bucket}/{file}.parquet
    try:
        event_bucket_name = event['Records'][0]['s3']['bucket']['name']
        event_obj_name = event['Records'][0]['s3']['object']['key']
        # 'ExtractHistory/*.txt
        if event_obj_name == 'extraction_complete.txt':
            ingestion_bucket_name = event_bucket_name
            processed_bucket_name = 'test-processed-va-052023'
            dates_for_dim_date = set()

            s3_client = boto3.client('s3')

            ingestion_response = s3_client.head_bucket(Bucket=ingestion_bucket_name)
            ingestion_status_code = ingestion_response['ResponseMetadata']['HTTPStatusCode']
            if ingestion_status_code == 200:
                logger.info(f'Connection to {ingestion_bucket_name} confirmed')

            processed_response = s3_client.head_bucket(Bucket=processed_bucket_name)
            processed_status_code = processed_response['ResponseMetadata']['HTTPStatusCode']
            if processed_status_code == 200:
                logger.info(f'Connection to {processed_bucket_name} confirmed')

            transform_design('design', ingestion_bucket_name, processed_bucket_name)
            transform_payment_type('payment_type', ingestion_bucket_name, processed_bucket_name)
            transform_location('address', ingestion_bucket_name, processed_bucket_name)
            transform_transaction('transaction', ingestion_bucket_name, processed_bucket_name)
            transform_staff('staff', 'department', ingestion_bucket_name, processed_bucket_name)
            transform_currency('currency', ingestion_bucket_name, processed_bucket_name)
            transform_counterparty('counterparty', 'address', ingestion_bucket_name, processed_bucket_name)
            transform_sales_order('sales_order', ingestion_bucket_name, processed_bucket_name, dates_for_dim_date)
            transform_purchase_order('purchase_order', ingestion_bucket_name, processed_bucket_name, dates_for_dim_date)
            transform_payment('payment', ingestion_bucket_name, processed_bucket_name, dates_for_dim_date)
            create_date(dates_for_dim_date, processed_bucket_name)

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.info('Bucket does not exist')
            raise e
        if e.response['Error']['Code'] == '403':
            logger.info('Invalid permissions for bucket')
        else:
            logger.info('transform_lambda_handler ', e)
            raise e



'''

- function will trigger whenever a new file appears in ingestion s3 bucket (check sprint)

- connect to ingestion s3 bucket

- fetch data from ingestion s3 bucket

- transform data into fact & dimension tables (star schema)

- upload data to processer in parquet format



'''