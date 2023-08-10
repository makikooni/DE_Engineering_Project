import boto3
import awswrangler as wr
import pandas as pd
from datetime import datetime, date
import logging
from botocore.exceptions import ClientError
logger = logging.getLogger('LoadUtilsLogger')
logger.setLevel(logging.INFO)


def get_id_col(connection, wh_table_name, table_df):
    """
    Function queries database to retrieve list of column id tuples, formatting 
    data in the case of date ids, and extracting ids from tuples into list.

    Args:
        connection (pg8000 connection object): 
            containing data regarding warehouse connection.
        
        wh_table_name (str):
            name of current table.

        table_df (pandas.DataFrame):
            dataframe of current table.

    Returns:
        new_id_col (list): 
            correctly formatted list of column ids
    """
    query = f"SELECT {table_df.columns[0]} FROM {wh_table_name};"
    cursor = connection.cursor()
    cursor.execute(query)
    id_col = cursor.fetchall()
    cursor.close()
    if len(id_col) !=0 and isinstance(id_col[0][0], date):
        new_id_col = [lst[0].strftime('%Y-%m-%d') for lst in id_col]
    else:
        new_id_col = [lst[0] for lst in id_col]
    return new_id_col

def get_table_data(s3_table_name, bucket_name, timestamp):
    try:
        return wr.s3.read_parquet(f's3://{bucket_name}/{timestamp}/{s3_table_name}.parquet')
    except Exception as error:
        logger.error("get_table_data")
        raise error

def insert_data_format(table):
    try:
        return [tuple(row) for row in table.itertuples(index=False)]
    except Exception as error:
        logger.error("insert_data_format")
        raise error

def build_insert_sql(wh_table_name, table):
    try:
        columns = ', '.join(table.columns)
        placeholder = ',' .join(['%s'] * len(table.columns))
        return f"INSERT INTO {wh_table_name} ({columns}) VALUES ({placeholder}) "
    except Exception as error:
        logger.error("build_insert_sql")
        raise error

def insert_table_data(connection,insert_table_sql, data_to_insert):
    try:
        cursor = connection.cursor()
        cursor.executemany(insert_table_sql, data_to_insert)
        connection.commit()
        cursor.close()
    except Exception as error:
        logger.error("insert_table_data")
        raise error

def get_job_list(bucket_name):
    try:
        df = wr.s3.read_csv(path=f's3://{bucket_name}/lastjob/lastjob.csv')
        ts_list = [str(ts[0]) for ts in df.values.tolist()]
        logger.info("successfully retrieved job list")
        return ts_list
    except Exception as error:
        logger.error("no new jobs")
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
        logger.error("build_update_sql")
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
        logger.error("update_data_format")
        raise error

def rename_lastjob(bucket_name):
    timestamp_suffix = datetime.now().strftime('%Y%m%d%H%M%S')
    s3_client = boto3.client('s3')
    s3_client.copy_object(Bucket=bucket_name, CopySource=f'{bucket_name}/lastjob/lastjob.csv', Key=f'lastjob/lastjob_{timestamp_suffix}.csv')
    s3_client.delete_object(Bucket=bucket_name, Key='lastjob/lastjob.csv')