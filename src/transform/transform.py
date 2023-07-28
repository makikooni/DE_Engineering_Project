
import os
import logging
import boto3
from pprint import pprint
import csv
import pandas as pd
import numpy as np

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def transformation_lambda_handler():
    # pd.set_option('display.max_columns', None)
    ingestion_bucket_name = 'test-va-ingestion-atif'
    processing_bucket_name = 'processed-va-052023'
    try:
        s3_client = boto3.client('s3')
        s3_resource = boto3.resource('s3')
        response = s3_client.head_bucket(Bucket=ingestion_bucket_name)
        status_code = response['ResponseMetadata']['HTTPStatusCode']
        if status_code != 200:
            raise Exception('the bucket may not exist, or, you may not have the correct permissions')
        
        # dim_design table
        design_table = pd.read_csv(f's3://{ingestion_bucket_name}/design.csv')
        dim_design_table = design_table[['design_id', 'design_name', 'file_location', 'file_name']]
        dim_design_table.to_parquet(f's3://{processing_bucket_name}/test_dim_design.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/test_dim_design.parquet

        # dim_payment_type table
        payment_type_table = pd.read_csv(f's3://{ingestion_bucket_name}/payment_type.csv')
        dim_payment_type_table = payment_type_table[['payment_type_id', 'payment_type_name']]
        dim_payment_type_table.to_parquet(f's3://{processing_bucket_name}/test_dim_payment_type.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/test_dim_payment_type.parquet

        # dim_location table
        address_table = pd.read_csv(f's3://{ingestion_bucket_name}/address.csv')
        dim_address_table = address_table[['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]
        dim_address_table.rename(columns={'address_id': 'location_id'}, inplace=True)
        dim_address_table.to_parquet(f's3://{processing_bucket_name}/test_dim_location.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/test_dim_location.parquet

        # dim_transaction table
        transaction_table = pd.read_csv(f's3://{ingestion_bucket_name}/transaction.csv')
        dim_transaction_table = transaction_table[['transaction_id', 'transaction_type', 'sales_order_id', 'purchase_order_id']]
        dim_transaction_table.to_parquet(f's3://{processing_bucket_name}/test_dim_transaction.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/test_dim_transaction.parquet

        # dim_staff table
        staff_table = pd.read_csv(f's3://{ingestion_bucket_name}/staff.csv')
        department_table = pd.read_csv(f's3://{ingestion_bucket_name}/department.csv')
        joined_staff_department_table = staff_table.join(department_table.set_index('department_id'), on='department_id', lsuffix="staff", rsuffix='department')
        dim_staff_table = joined_staff_department_table[['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']]
        dim_staff_table.to_parquet(f's3://{processing_bucket_name}/test_dim_staff.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/test_dim_staff.parquet

        # dim_currency table
        currency_table = pd.read_csv(f's3://{ingestion_bucket_name}/currency.csv')
        dim_currency_table = currency_table[['currency_id', 'currency_code']]
        conditions = [(dim_currency_table['currency_code'] == 'EUR'),
                      (dim_currency_table['currency_code'] == 'GBP'),
                      (dim_currency_table['currency_code'] == 'USD')]
        values = ['Euro',
                'British Pound',
                'US Dollar']
        dim_currency_table['currency_name'] = np.select(conditions, values)
        dim_currency_table.to_parquet(f's3://{processing_bucket_name}/test_dim_currency.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/test_dim_currency.parquet

        # dim_counterparty table
        counterparty_table = pd.read_csv(f's3://{ingestion_bucket_name}/counterparty.csv')
        address_table_for_counterparty = pd.read_csv(f's3://{ingestion_bucket_name}/address.csv')
        joined_counterparty_address_table = counterparty_table.join(address_table_for_counterparty.set_index('address_id'), on='legal_address_id', lsuffix='counterparty', rsuffix='address')
        dim_counterparty = joined_counterparty_address_table[['counterparty_id', 'counterparty_legal_name', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]
        columns_to_rename = ['address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country']
        dim_counterparty.rename(columns={col: 'counterparty_legal_'+col for col in dim_counterparty.columns if col in columns_to_rename}, inplace=True)
        dim_counterparty.rename(columns={'phone': 'counterparty_legal_phone_number'}, inplace=True)
        dim_counterparty.to_parquet(f's3://{processing_bucket_name}/test_dim_counterparty.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/test_dim_counterparty.parquet

        # fact_sales_order table
        










    except Exception as e:
        print('except')
        print(e)
        pass


transformation_lambda_handler()



'''

- function will trigger whenever a new file appears in ingestion s3 bucket (check sprint)

- connect to ingestion s3 bucket

- fetch data from ingestion s3 bucket

- transform data into fact & dimension tables (star schema)

- upload data to processer in parquet format



'''