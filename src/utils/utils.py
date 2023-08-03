import awswrangler as wr
import logging
from pprint import pprint


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def read_csv_to_pandas(file, source_bucket):
    try:
        return wr.s3.read_csv(path=f's3://{source_bucket}/{file}.csv')
    except Exception as e:
        logger.info('read_csv_to_pandas', e)
        raise e


def write_df_to_parquet(df, file, target_bucket):
    try:
       return wr.s3.to_parquet(df=df, path=f's3://{target_bucket}/{file}.parquet')
    except Exception as e:
        logger.info('write_df_to_parquet', e)
        raise e
        

def timestamp_to_date_and_time(dataframe):
    try:
        new_created = dataframe['created_at'].str.split(" ", n = 1, expand = True)
        dataframe['created_date']= new_created[0]
        dataframe['created_time']= new_created[1]
        dataframe.drop(columns =['created_at'], inplace = True)
        new_updated = dataframe['last_updated'].str.split(" ", n = 1, expand = True)
        dataframe['last_updated_date']= new_updated[0]
        dataframe['last_updated_time']= new_updated[1]
        dataframe.drop(columns =['last_updated'], inplace = True)
        return dataframe
    except Exception as e:
        logger.info('timestamp_to_date_and_time', e)
        raise e


def add_to_dates_set(set, cols_to_add):
    try:
        for col in cols_to_add:
            for row in col:
                set.add(row)
    except Exception as e:
        logger.info('add_to_dates_set', e)
        raise e