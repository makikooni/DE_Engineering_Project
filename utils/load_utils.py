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
            name of current warehouse table.

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
    """
    Reads data from parquet file stored in processed s3 bucket and 
    returns it as pandas.DataFrame.

    Args:
        s3_table_name (str): 
            name of current table.
        
        bucket_name (str): 
            name of processed s3 bucket.
        
        timstamp (str):
            specific timestamp to locate correct directory within processed 
            s3 bucket.
    
    Raises:
        Exception:
            Raised if data reading is unsuccessful.
    """
    try:
        return wr.s3.read_parquet(f's3://{bucket_name}/{timestamp}/{s3_table_name}.parquet')
    except Exception as error:
        logger.error("get_table_data")
        raise error

def insert_data_format(table):
    """
    Converts data in each row into a tuple, returning it as a list of tuples.

    Args:
        table (pandas.DataFrame):
            dataframe containing data to be processed.

    Returns:
        list:
            list of tuples.
    
    Raises:
        Exception:
            Raised if data conversion is unsuccessful.
    """
    try:
        return [tuple(row) for row in table.itertuples(index=False)]
    except Exception as error:
        logger.error("insert_data_format")
        raise error

def build_insert_sql(wh_table_name, table):
    """
    Builds SQL query for iserting data into the warehouse by joining table 
    column names into comma separated string, and joining placeholders into 
    comma separated string, before constructing the SQL query and returning it.

    Args:
        wh_table_name (str):
            name of current warehouse table.  

        table (pandas.DataFrame): 
            dataframe containing current incoming table data.

    Returns:
        str: 
            SQL query as string.

    Raises:
        Execption:
            Raised if execution unsuccessful.
    """
    try:
        columns = ', '.join(table.columns)
        placeholder = ',' .join(['%s'] * len(table.columns))
        return f"INSERT INTO {wh_table_name} ({columns}) VALUES ({placeholder}) "
    except Exception as error:
        logger.error("build_insert_sql")
        raise error

def insert_table_data(connection, query, data):
    """
    Function adds data to warehouse.

    Args:
        connection (pg8000 connection object): 
            containing data regarding warehouse connection.
        
        query (str): 
            SQL query string.

        data (list): 
            data to be sent to warehouse as list of tuples.
    
    Returns:
        None.

    Raises:
        Exception:
            Raised if data entry is unsuccessful.
    """
    try:
        cursor = connection.cursor()
        cursor.executemany(query, data)
        connection.commit()
        cursor.close()
    except Exception as error:
        logger.error("insert_table_data")
        raise error

def get_job_list(bucket_name):
    """
    Extracts list of timestamps from lastjob.csv, stored in processed s3 bucket,
    logs progress and returns list.

    Args:
        bucket_name (str):
            name of processed s3 bucket.
    
    Returns:
        list: list of timstamps.
    
    Raises:
        Exception:
            Raised if no new jobs, or, data conversion is unsuccessful.
    """
    try:
        df = wr.s3.read_csv(path=f's3://{bucket_name}/lastjob/lastjob.csv')
        ts_list = [str(ts[0]) for ts in df.values.tolist()]
        logger.info("successfully retrieved job list")
        return ts_list
    except Exception as error:
        logger.error("no new jobs")
        raise error

def build_update_sql(wh_table_name, table):
    """
    Builds an SQL query by iterating through list of table columns 
    to calculate neccessary query perameter values and returns it.

    Args:
        wh_table_name (str):
            name of current warehouse table.

        table (pandas.DataFrame):
            dataframe of current table.
        
    Returns:
        str: SQL query string.
    
    Raises:
        Exception:
            Raised if query construction is unsuccessful.
    """
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
    """
    Formats row data for updating rows in warehouse.

    Args:
        row (list):
            list of data from a specfic table row.
    
    Returns:
        list: list of correctly formatted data.

    Raises:
        Exception:
            Raised if data formatting is unsuccessful.
    """
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
    """
    Creates copy of lastjob.csv file from the processed s3 bucket, names it with the relevant timestamp 
    and also stores it in the processed s3 bucket. It then deletes the lastjob.csv file.

    Args:
        bucket_name (str): name of processed s3 bucket.
        
    Returns:
        None.
    """
    timestamp_suffix = datetime.now().strftime('%Y%m%d%H%M%S')
    s3_client = boto3.client('s3')
    s3_client.copy_object(Bucket=bucket_name, CopySource=f'{bucket_name}/lastjob/lastjob.csv', Key=f'lastjob/lastjob_{timestamp_suffix}.csv')
    s3_client.delete_object(Bucket=bucket_name, Key='lastjob/lastjob.csv')