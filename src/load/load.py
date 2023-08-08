import logging
import json
import pg8000
import boto3
import awswrangler as wr
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def update_wh(s3_table_name, wh_table_name, is_dim, secret_name='warehouse'):
    # get warehouse credentails from AWS secrets
    print(secret_name)
    secretsmanager = boto3.client('secretsmanager')
    try:
        db_credentials = secretsmanager.get_secret_value(
            SecretId=secret_name
        )
        print(db_credentials)
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

        # get the table from the s3 and put it in a pandas dataframe#
        # will need to work with a list
        new_jobs = get_job_list()
        for ts_dir in new_jobs:
            table = get_table_data(s3_table_name, ts_dir)
            if is_dim:
                lst_wh_id = get_id_col(connection, wh_table_name, table)
                for row in table.values.tolist():
                    if row[0] in lst_wh_id:
                        update_data = update_data_format(row)
                        query = build_update_sql(wh_table_name, table)
                        insert_table_data(connection, query, update_data)
                    else:
                        insert_table_sql = build_insert_sql(wh_table_name, table)
                        # # Convert DataFrame to list tuples for executemany
                        data_to_insert = insert_data_format(table)
                        # # Execute the query using executemany to insert all rows at once
                        insert_table_data(connection,insert_table_sql, data_to_insert)
            else:
                insert_table_sql = build_insert_sql(wh_table_name, table)
                # # Convert DataFrame to list tuples for executemany
                data_to_insert = insert_data_format(table)
                # # Execute the query using executemany to insert all rows at once
                insert_table_data(connection,insert_table_sql, data_to_insert)
    except Exception as error:
        logger.info["main function error", error]
        raise error
    
def get_id_col(connection, wh_table_name, table):
    query = f"SELECT {table.columns[0]} FROM {wh_table_name};"
    cursor = connection.cursor()
    cursor.execute(query)
    id_col = cursor.fetchall()
    cursor.close()
    return [lst[0] for lst in id_col]

def get_table_data(s3_table_name, timestamp):
    try:
        return wr.s3.read_parquet(f's3://processed-va-052023/{timestamp}/{s3_table_name}')
    except Exception as error:
        logger.info["get_table_data", error]
        raise error

def insert_data_format(table):
    try:
        return [tuple(row) for row in table.itertuples(index=False)]
    except Exception as error:
        logger.info["insert_data_format", error]
        raise error

def build_insert_sql(wh_table_name, table):
    try:
        columns = ', '.join(table.columns)
        placeholder = ',' .join(['%s'] * len(table.columns))
        return f"INSERT INTO {wh_table_name} ({columns}) VALUES ({placeholder}) "
    except Exception as error:
        logger.info["build_insert_sql", error]
        raise error

def insert_table_data(connection,insert_table_sql, data_to_insert):
    try:
        cursor = connection.cursor()
        cursor.executemany(insert_table_sql, data_to_insert)
        connection.commit()
        cursor.close()
    except Exception as error:
        logger.info["insert_table_data", error]
        raise error

def get_job_list():
    try:
        df = wr.s3.read_csv(path='s3://processed-va-052023/lastjob/lastjob.csv')
        ts_list = [str(ts[0]) for ts in df.values.tolist()]
        return ts_list
    except Exception as error:
        logger.info["no new jobs", error]
        raise error

def build_update_sql(wh_table_name, table):
    try:
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
    except Exception as error:
        logger.info["build_update_sql", error]
        raise error

def update_data_format(row):
    try:
        data = []
        index = 0
        for value in row:
            if index < len(row) and index != 0:
                data.append(value)
                index += 1
            elif index == 0:
                index += 1
        data.append(row[0])
        print(data)
        return data
    except Exception as error:
        logger.info["update_data_format", error]
        raise error

def rename_lastjob():
    timestamp_suffix = datetime.now().strftime('%Y%m%d%H%M%S')
    s3_client = boto3.client('s3')
    s3_client.copy_object(Bucket='processed-va-052023', CopySource='processed-va-052023/lastjob/lastjob.csv', Key=f'lastjob/lastjob{timestamp_suffix}.csv')
    s3_client.delete_object(Bucket='processed-va-052023', Key='lastjob/lastjob.csv')


def load_lambda_hander():
    update_wh('dim_counterparty.parquet', 'dim_counterparty', True)
    update_wh('dim_currency.parquet', 'dim_currency', True)
    update_wh('dim_date.parquet', 'dim_date', True)
    update_wh('dim_design.parquet', 'dim_design', True)
    update_wh('dim_location.parquet', 'dim_location', True)
    update_wh('dim_payment_type.parquet', 'dim_payment_type', True)
    update_wh('dim_staff.parquet', 'dim_staff', True)
    update_wh('fact_payment.parquet', 'fact_payment', False)
    update_wh('fact_purchase_order.parquet', 'fact_purchase_order', False)
    update_wh('fact_sales_order.parquet', 'fact_sales_order', False)
    rename_lastjob()
