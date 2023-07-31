import json
import pg8000
import boto3
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
        # conecting to the process bucket
        process_bucket = 'processed-va-052023'
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
        insert_table_sql = build_load_sql(wh_table_name, table)
        # Convert DataFrame to list tuples for executemany
        data_to_insert = datafarme_to_list(table)
        # Execute the query using executemany to insert all rows at once
        insert_table_data(connection,insert_table_sql, data_to_insert)
    except Exception as error:
        raise error
def get_table_data(s3_table_name):
    return pd.read_parquet(f's3://processed-va-052023/{s3_table_name}')

def datafarme_to_list(table):
    return [tuple(row) for row in table.itertuples(index=False)]

def build_load_sql(wh_table_name, table):
    columns = ', '.join(table.columns)
    placeholder = ',' .join(['%s'] * len(table.columns))
    return f"INSERT INTO {wh_table_name} ({columns}) VALUES ({placeholder})"

def insert_table_data(connection,insert_table_sql, data_to_insert):
    cursor = connection.cursor()
    cursor.executemany(insert_table_sql, data_to_insert)
    connection.commit()
    cursor.close()

#add_new_rows('test_dim_design.parquet', 'dim_design')
