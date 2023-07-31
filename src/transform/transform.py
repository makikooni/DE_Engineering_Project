
import os
import logging
import boto3
from pprint import pprint
import csv
import pandas as pd
import numpy as np

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

ingestion_bucket_name = 'test-va-ingestion-atif'
processing_bucket_name = 'processed-va-052023'

dates_for_dim_date = set()

def transformation_lambda_handler():
    # pd.set_option('display.max_columns', None)
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
        dim_design_table.to_parquet(f's3://{processing_bucket_name}/dim_design.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/dim_design.parquet


        # dim_payment_type table
        payment_type_table = pd.read_csv(f's3://{ingestion_bucket_name}/payment_type.csv')
        dim_payment_type_table = payment_type_table[['payment_type_id', 'payment_type_name']]
        dim_payment_type_table.to_parquet(f's3://{processing_bucket_name}/dim_payment_type.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/dim_payment_type.parquet


        # dim_location table
        address_table = pd.read_csv(f's3://{ingestion_bucket_name}/address.csv')
        dim_address_table = address_table[['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]
        dim_address_table.rename(columns={'address_id': 'location_id'}, inplace=True)
        dim_address_table.to_parquet(f's3://{processing_bucket_name}/dim_location.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/dim_location.parquet


        # dim_transaction table
        transaction_table = pd.read_csv(f's3://{ingestion_bucket_name}/transaction.csv')
        dim_transaction_table = transaction_table[['transaction_id', 'transaction_type', 'sales_order_id', 'purchase_order_id']]
        dim_transaction_table.to_parquet(f's3://{processing_bucket_name}/dim_transaction.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/dim_transaction.parquet


        # dim_staff table
        staff_table = pd.read_csv(f's3://{ingestion_bucket_name}/staff.csv')
        department_table = pd.read_csv(f's3://{ingestion_bucket_name}/department.csv')
        joined_staff_department_table = staff_table.join(department_table.set_index('department_id'), on='department_id', lsuffix="staff", rsuffix='department')
        dim_staff_table = joined_staff_department_table[['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']]
        dim_staff_table.to_parquet(f's3://{processing_bucket_name}/dim_staff.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/dim_staff.parquet


        # dim_currency table
        currency_table = pd.read_csv(f's3://{ingestion_bucket_name}/currency.csv')
        dim_currency_table = currency_table[['currency_id', 'currency_code']]
        conditions = [(dim_currency_table['currency_code'] == 'EUR'), (dim_currency_table['currency_code'] == 'GBP'), (dim_currency_table['currency_code'] == 'USD')]
        values = ['Euro', 'British Pound', 'US Dollar']
        dim_currency_table['currency_name'] = np.select(conditions, values)
        dim_currency_table.to_parquet(f's3://{processing_bucket_name}/dim_currency.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/dim_currency.parquet


        # dim_counterparty table
        counterparty_table = pd.read_csv(f's3://{ingestion_bucket_name}/counterparty.csv')
        address_table_for_counterparty = pd.read_csv(f's3://{ingestion_bucket_name}/address.csv')
        joined_counterparty_address_table = counterparty_table.join(address_table_for_counterparty.set_index('address_id'), on='legal_address_id', lsuffix='counterparty', rsuffix='address')
        dim_counterparty = joined_counterparty_address_table[['counterparty_id', 'counterparty_legal_name', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]
        columns_to_rename = ['address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country']
        dim_counterparty.rename(columns={col: 'counterparty_legal_'+col for col in dim_counterparty.columns if col in columns_to_rename}, inplace=True)
        dim_counterparty.rename(columns={'phone': 'counterparty_legal_phone_number'}, inplace=True)
        dim_counterparty.to_parquet(f's3://{processing_bucket_name}/dim_counterparty.parquet')
        # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/dim_counterparty.parquet

        transform_sales_order()
        transform_purchase_order()
        transform_payment()
        create_date(dates_for_dim_date)


        # dim_date table





    except Exception as e:
        print('except')
        print(e)
        pass




def transform_sales_order():
    # fact_sales_order table
    sales_order_table = pd.read_csv(f's3://{ingestion_bucket_name}/sales_order.csv')
    sales_order_table.columns.values[0] = "sales_record_id"
    
    new_created_sales = sales_order_table['created_at'].str.split(" ", n = 1, expand = True)
    sales_order_table['created_date']= new_created_sales[0]
    sales_order_table['created_time']= new_created_sales[1]
    sales_order_table.drop(columns =['created_at'], inplace = True)

    new_updated_sales = sales_order_table['last_updated'].str.split(" ", n = 1, expand = True)
    sales_order_table['last_updated_date']= new_updated_sales[0]
    sales_order_table['last_updated_time']= new_updated_sales[1]
    sales_order_table.drop(columns =['last_updated'], inplace = True)

    sales_order_table.rename(columns={'staff_id': 'sales_staff_id'}, inplace=True)
    fact_sales_order = sales_order_table[['sales_record_id', 'sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']]
    fact_sales_order.to_parquet(f's3://{processing_bucket_name}/fact_sales_order.parquet')

    date_cols_to_add = [fact_sales_order['created_date'], fact_sales_order['last_updated_date'],fact_sales_order['agreed_payment_date'], fact_sales_order['agreed_delivery_date']]
    for col in date_cols_to_add:
        for row in col:
            dates_for_dim_date.add(row)
    # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/fact_sales_order.parquet

def transform_purchase_order():
    # fact_purchase_order table
    purchase_order_table = pd.read_csv(f's3://{ingestion_bucket_name}/purchase_order.csv')
    purchase_order_table.columns.values[0] = "purchase_record_id"

    new_created_purchase = purchase_order_table['created_at'].str.split(" ", n = 1, expand = True)
    purchase_order_table['created_date']= new_created_purchase[0]
    purchase_order_table['created_time']= new_created_purchase[1]
    purchase_order_table.drop(columns =['created_at'], inplace = True)

    new_updated_purchase = purchase_order_table['last_updated'].str.split(" ", n = 1, expand = True)
    purchase_order_table['last_updated_date']= new_updated_purchase[0]
    purchase_order_table['last_updated_time']= new_updated_purchase[1]
    purchase_order_table.drop(columns =['last_updated'], inplace = True)

    fact_purchase_order = purchase_order_table[['purchase_record_id', 'purchase_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'staff_id', 'counterparty_id', 'item_code', 'item_quantity', 'item_unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']]
    fact_purchase_order.to_parquet(f's3://{processing_bucket_name}/fact_purchase_order.parquet')

    date_cols_to_add = [fact_purchase_order['created_date'], fact_purchase_order['last_updated_date'], fact_purchase_order['agreed_delivery_date'], fact_purchase_order['agreed_payment_date']]
    for col in date_cols_to_add:
        for row in col:
            dates_for_dim_date.add(row)
    # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/fact_purchase_order.parquet

def transform_payment():
    # fact_payment table
    payment_table = pd.read_csv(f's3://{ingestion_bucket_name}/payment.csv')
    payment_table.columns.values[0] = 'payment_record_id'

    new_created_payment = payment_table['created_at'].str.split(" ", n = 1, expand = True)
    payment_table['created_date']= new_created_payment[0]
    payment_table['created_time']= new_created_payment[1]
    payment_table.drop(columns =['created_at'], inplace = True)

    new_updated_payment = payment_table['last_updated'].str.split(" ", n = 1, expand = True)
    payment_table['last_updated_date']= new_updated_payment[0]
    payment_table['last_updated_time']= new_updated_payment[1]
    payment_table.drop(columns =['last_updated'], inplace = True)

    # figma states last_updated, assuming this is a typo: find last_updated_time
    fact_payment_table = payment_table[['payment_record_id', 'payment_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'transaction_id', 'counterparty_id', 'payment_amount', 'currency_id', 'payment_type_id', 'paid', 'payment_date']]
    fact_payment_table.to_parquet(f's3://{processing_bucket_name}/fact_payment.parquet')

    date_cols_to_add = [fact_payment_table['created_date'], fact_payment_table['last_updated_date'], fact_payment_table['payment_date']]
    for col in date_cols_to_add:
        for row in col:
            dates_for_dim_date.add(row)
    # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/fact_payment.parquet

def create_date(dates_for_dim_date):
    dates = {'date_id': sorted(list(dates_for_dim_date))}
    dim_date = pd.DataFrame(data=dates)
    dim_date['year'] = pd.DatetimeIndex(dim_date['date_id']).year
    dim_date['month'] = pd.DatetimeIndex(dim_date['date_id']).month
    dim_date['day'] = pd.DatetimeIndex(dim_date['date_id']).day
    dim_date['day_of_week'] = pd.DatetimeIndex(dim_date['date_id']).dayofweek
    dim_date['day_name'] = pd.DatetimeIndex(dim_date['date_id']).day_name()
    dim_date['month_name'] = pd.DatetimeIndex(dim_date['date_id']).month_name()
    dim_date['quarter'] = pd.DatetimeIndex(dim_date['date_id']).quarter
    dim_date.to_parquet(f's3://{processing_bucket_name}/dim_date.parquet')
    # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/dim_date.parquet



transformation_lambda_handler()


'''

- function will trigger whenever a new file appears in ingestion s3 bucket (check sprint)

- connect to ingestion s3 bucket

- fetch data from ingestion s3 bucket

- transform data into fact & dimension tables (star schema)

- upload data to processer in parquet format



'''