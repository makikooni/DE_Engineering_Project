import json
import pg8000
import boto3
import awswrangler as wr
import pandas as pd
from botocore.exceptions import ClientError


def add_new_rows(s3_table_name, wh_table_name, secret_name='warehouse'):
    # get warehouse credentails from AWS secrets
    secretsmanager = boto3.client('secretsmanager')
    try:
        db_credentials = secretsmanager.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            raise Exception("The requested secret was not found")
        else:
            raise error
    try:
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
        # get the table from the s3 and put it in a pandas dataframe
        table = get_table_data(s3_table_name)
        
        insert_table_sql = build_insert_sql(wh_table_name, table)
        # Convert DataFrame to list tuples for executemany
        data_to_insert = dataframe_to_list(table)
        # Execute the query using executemany to insert all rows at once
        insert_table_data(connection,insert_table_sql, data_to_insert)
    except Exception as error:
        raise error

def get_table_data(s3_table_name):
    return wr.s3.read_parquet(f's3://test-processed-va-052023/{s3_table_name}')

def dataframe_to_list(table):
    return [tuple(row) for row in table.itertuples(index=False)]

def build_insert_sql(wh_table_name, table):
    columns = ', '.join(table.columns)
    placeholder = ',' .join(['%s'] * len(table.columns))
    return f"INSERT INTO {wh_table_name} ({columns}) VALUES ({placeholder})"

def insert_table_data(connection,insert_table_sql, data_to_insert):
    cursor = connection.cursor()
    cursor.executemany(insert_table_sql, data_to_insert)
    connection.commit()
    cursor.close()

def check_update_or_rows():
    '''
    1. look in the s3 bucket
    2. check if folder with data is new
    3. check  
    '''

def build_update_sql(wh_table_name, table):
    ph_SET = ''
    ph_WHERE = ''
    index = 0
    columns = table.columns
    for column in columns:
        if index < (len(table.columns) - 1) and index != 0:
            ph_SET += column + ' = %s, '
            index += 1
        elif index == (len(table.columns) - 1):
            ph_SET += column + ' = %s'
            index += 1
        elif index == 0:
            ph_WHERE += column + ' = %s'
            index += 1
    return f"UPDATE {wh_table_name} SET {ph_SET} WHERE {ph_WHERE}"

def update_data_format(table):
    data = []
    rows = table.values.tolist()
    columns = table.columns
    for row in rows:
        update_row = row
        index = 0
        for value in update_row:
            if index < len(row) and index != 0:
                data.append(columns[index])
                data.append(value)
                index += 1
            elif index == 0:
                index += 1
        data.append(columns[0])
        data.append(row[0])
    return data

def load_lambda_hander():
    add_new_rows('dim_counterparty.parquet', 'dim_counterparty')
    add_new_rows('dim_currency.parquet', 'dim_currency')
    add_new_rows('dim_date.parquet', 'dim_date')
    add_new_rows('dim_design.parquet', 'dim_design')
    add_new_rows('dim_location.parquet', 'dim_location')
    add_new_rows('dim_payment_type.parquet', 'dim_payment_type')
    add_new_rows('dim_staff.parquet', 'dim_staff')
    add_new_rows('fact_payment.parquet', 'fact_payment')
    add_new_rows('fact_purchase_order.parquet', 'fact_purchase_order')
    add_new_rows('fact_sales_order.parquet', 'fact_sales_order')
