import logging
import boto3
from src.utils.table_transformations import transform_design, transform_payment_type, transform_location, transform_transaction, transform_currency, transform_staff, transform_payment, transform_purchase_order, transform_counterparty, transform_sales_order, create_date
from pprint import pprint

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def transformation_lambda_handler(event=1, context=1):
    # run in terminal to view pq table --> parquet-tools show s3://{target_bucket}/{file}.parquet
    # pd.set_option('display.max_columns', None)
    ingestion_bucket_name = 'test-va-ingestion-atif'
    processing_bucket_name = 'test-processed-va-052023'
    dates_for_dim_date = set()
    try:
        s3_client = boto3.client('s3')
        response = s3_client.head_bucket(Bucket=ingestion_bucket_name)
        status_code = response['ResponseMetadata']['HTTPStatusCode']
        if status_code != 200:
            raise Exception('the bucket may not exist, or, you may not have the correct permissions')
        transform_design('design', ingestion_bucket_name, processing_bucket_name)
        transform_payment_type('payment_type', ingestion_bucket_name, processing_bucket_name)
        transform_location('address', ingestion_bucket_name, processing_bucket_name)
        transform_transaction('transaction', ingestion_bucket_name, processing_bucket_name)
        transform_staff('staff', 'department', ingestion_bucket_name, processing_bucket_name)
        transform_currency('currency', ingestion_bucket_name, processing_bucket_name)
        transform_counterparty('counterparty', 'address', ingestion_bucket_name, processing_bucket_name)
        transform_sales_order('sales_order', ingestion_bucket_name, processing_bucket_name, dates_for_dim_date)
        transform_purchase_order('purchase_order', ingestion_bucket_name, processing_bucket_name, dates_for_dim_date)
        transform_payment('payment', ingestion_bucket_name, processing_bucket_name, dates_for_dim_date)
        create_date(dates_for_dim_date, processing_bucket_name)
    except Exception as e:
        logger.info('transform_lambda_handler ', e)
        raise e

transformation_lambda_handler()

'''

- function will trigger whenever a new file appears in ingestion s3 bucket (check sprint)

- connect to ingestion s3 bucket

- fetch data from ingestion s3 bucket

- transform data into fact & dimension tables (star schema)

- upload data to processer in parquet format



'''