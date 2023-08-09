import logging
import json
import pg8000
import boto3
import awswrangler as wr
import pandas as pd
from datetime import datetime
from utils.utils import get_secret, connect_db
from botocore.exceptions import ClientError

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

# def update_wh(s3_table_name, wh_table_name, is_dim, secret_name='warehouse'):
#     # get warehouse credentails from AWS secrets
#     db_creds = get_secret(secret_name)
#     try:
#         connection = connect_db(db_creds)

#         # get the table from the s3 and put it in a pandas dataframe#
#         # will need to work with a list
#         new_jobs = get_job_list()
#         for ts_dir in new_jobs:
#             table_df = get_table_data(s3_table_name, ts_dir)
#             if is_dim:
#                 lst_wh_id = get_id_col(connection, wh_table_name, table_df)
#                 for row in table.values.tolist():
#                     if row[0] in lst_wh_id:
#                         update_data = update_data_format(row)
#                         query = build_update_sql(wh_table_name, table)
#                         insert_table_data(connection, query, update_data)
#                     else:
#                         insert_table_sql = build_insert_sql(wh_table_name, table)
#                         # # Convert DataFrame to list tuples for executemany
#                         data_to_insert = insert_data_format(table)
#                         # # Execute the query using executemany to insert all rows at once
#                         insert_table_data(connection,insert_table_sql, data_to_insert)
#             else:
#                 insert_table_sql = build_insert_sql(wh_table_name, table)
#                 # # Convert DataFrame to list tuples for executemany
#                 data_to_insert = insert_data_format(table)
#                 # # Execute the query using executemany to insert all rows at once
#                 insert_table_data(connection,insert_table_sql, data_to_insert)
#         connection.close()
#     except Exception as error:
#         logger.error["main function error", error]
#         raise error
    
def get_id_col(connection, wh_table_name, table_df):
    query = f"SELECT {table_df.columns[0]} FROM {wh_table_name};"
    cursor = connection.cursor()
    cursor.execute(query)
    id_col = cursor.fetchall()
    cursor.close()
    return [lst[0] for lst in id_col]

def get_table_data(s3_table_name, bucket_name, timestamp):
    try:
        return wr.s3.read_parquet(f's3://{bucket_name}/{timestamp}/{s3_table_name}.parquet')
    except Exception as error:
        logger.error["get_table_data", error]
        raise error

def insert_data_format(table):
    try:
        return [tuple(row) for row in table.itertuples(index=False)]
    except Exception as error:
        logger.error["insert_data_format", error]
        raise error

def build_insert_sql(wh_table_name, table):
    try:
        columns = ', '.join(table.columns)
        placeholder = ',' .join(['%s'] * len(table.columns))
        return f"INSERT INTO {wh_table_name} ({columns}) VALUES ({placeholder}) "
    except Exception as error:
        logger.error["build_insert_sql", error]
        raise error

def insert_table_data(connection,insert_table_sql, data_to_insert):
    try:
        cursor = connection.cursor()
        cursor.executemany(insert_table_sql, data_to_insert)
        connection.commit()
        cursor.close()
    except Exception as error:
        logger.error["insert_table_data", error]
        raise error

def get_job_list():
    try:
        df = wr.s3.read_csv(path='s3://processed-va-052023/lastjob/lastjob.csv')
        ts_list = [str(ts[0]) for ts in df.values.tolist()]
        return ts_list
    except Exception as error:
        logger.error["no new jobs", error]
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
        logger.error["build_update_sql", error]
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
        return data
    except Exception as error:
        logger.error["update_data_format", error]
        raise error

def rename_lastjob(bucket_name):
    timestamp_suffix = datetime.now().strftime('%Y%m%d%H%M%S')
    s3_client = boto3.client('s3')
    s3_client.copy_object(Bucket=bucket_name, CopySource=f'{bucket_name}/lastjob/lastjob.csv', Key=f'lastjob/lastjob_{timestamp_suffix}.csv')
    s3_client.delete_object(Bucket=bucket_name, Key='lastjob/lastjob.csv')


def load_lambda_handler(event, context):
    PROCESSED_BUCKET_NAME = 'processed-va-052023'
    WAREHOUSE_DB_NAME = 'warehouse'
    WAREHOUSE_TABLE_NAMES = 'warehouse_table_names'

    db_creds = get_secret(WAREHOUSE_DB_NAME)
    new_jobs = get_job_list()
    table_names = list(get_secret(WAREHOUSE_TABLE_NAMES).keys())

    try:
        connection = connect_db(db_creds)
        for table_name in table_names:
            for ts_dir in new_jobs:
                table_df = get_table_data(table_name, PROCESSED_BUCKET_NAME, ts_dir)
                if table_df.startswith('dim'):
                    lst_wh_id = get_id_col(connection, table_name, table_df)
                    for row in table_df.values.tolist():
                        if row[0] in lst_wh_id:
                            data = update_data_format(row)
                            query = build_update_sql(table_name, table_df)
                        else:
                            query = build_insert_sql(table_name, table_df)
                            data = insert_data_format(table_df)
                        insert_table_data(connection, query, data)
                else:
                    query = build_insert_sql(table_name, table_df)
                    data = insert_data_format(table_df)
                    insert_table_data(connection, query, data)
        connection.close()
        rename_lastjob(PROCESSED_BUCKET_NAME)
    except Exception as error:
        logger.error["main function error", error]
        raise error
