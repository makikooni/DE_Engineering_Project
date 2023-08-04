import awswrangler as wr
from datetime import datetime
import logging
import json
from pg8000.native import Connection, InterfaceError, DatabaseError, identifier, literal
import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger("UtilsLogger")
logger.setLevel(logging.INFO)

def get_secret(secret_name):
    if not isinstance(secret_name, str):
        raise TypeError(f"secret_name is {type(secret_name)}, {str} is required")

    secretsmanager = boto3.client("secretsmanager")
    try:
        response = secretsmanager.get_secret_value(SecretId=secret_name)
        secret_value = json.loads(response["SecretString"].replace("'", '"'))
        logger.info(f"secret: {secret_name} information successfully retrieved!")
        return secret_value

    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.error(
                f"ResourceNotFoundException: the secret named {secret_name} cannot be found in SecretsManager"
            )
            raise KeyError(
                f"ResourceNotFoundException: the secret named {secret_name} cannot be found in SecretsManager"
            )
        elif e.response["Error"]["Code"] == "AccessDeniedException":
            logger.error(
                f"AccessDeniedException: the lambda does not have an identity-based policy to access SecretsManager resource"
            )
            raise RuntimeError(
                f"AccessDeniedException: the lambda does not have an identity-based policy to access SecretsManager resource"
            )
        else:
            logger.error(e)


def get_table_db(connection, table_name):
    if not isinstance(table_name, str):
        raise TypeError(f"table name is {type(table_name)}, expected {str}")
    if not isinstance(connection, Connection):
        raise TypeError(
            f"connection object is {type(connection)}, expected {Connection}"
        )

    try:
        query = f"SELECT * FROM {identifier(table_name)};"

        table_data = connection.run(query)
        column_names = [col["name"] for col in connection.columns]

        table_df = pd.DataFrame(data=table_data, columns=column_names)

        return table_df, query
    except InterfaceError:
        logger.error(f"InterfaceError: the query: \n {query} \n cannot be executed.")
        raise InterfaceError


def upload_table_s3(table_df, table_name, bucket_name):
    if not isinstance(table_df, pd.DataFrame):
        raise TypeError(f"table dataframe {type(table_df)}, expected {pd.DataFrame}")
    if not isinstance(table_name, str):
        raise TypeError(f"table name is {type(table_name)}, expected {str}")
    if not isinstance(bucket_name, str):
        raise TypeError(f"ingestion bucket name is {type(bucket_name)}, expected {str}")

    try:
        wr.s3.to_csv(table_df, f"s3://{bucket_name}/{table_name}.csv", index=False)

    except Exception as e:
        error = e.response["Error"]
        if e.response["Error"]["Code"] == "NoSuchBucket":
            logger.error(f"NoSuchBucket: {error['BucketName']} does not exist")

            raise KeyError(f"{error['BucketName']} does not exist")
        else:
            raise e


def connect_db(db_credentials):
    if not isinstance(db_credentials, dict):
        raise TypeError(f"db_credentials is {type(db_credentials)}, {dict} is required")

    db_credentials_keys = list(db_credentials.keys())
    required_keys = ["host", "port", "dbname", "username", "password"]

    for req_key in required_keys:
        if req_key not in db_credentials_keys:
            raise KeyError(f"db_credentials contains does not contain {req_key}")

    for key in db_credentials:
        if not isinstance(db_credentials[key], str):
            raise ValueError(
                f"value for {key} is {type(db_credentials[key])}, {str} is required"
            )

    try:
        connection = Connection(
            host=db_credentials["host"],
            port=db_credentials["port"],
            database=db_credentials["dbname"],
            user=db_credentials["username"],
            password=db_credentials["password"],
        )
        return connection
    except InterfaceError:
        logger.error(f"InterfaceError: please check your database credentials")
        raise InterfaceError

    except DatabaseError:
        logger.error(f"DatabaseError: please contact your database administrator")
        raise DatabaseError

        
def extract_history_s3(bucket_name, prefix):
    if not isinstance(bucket_name, str):
        raise TypeError(f"bucket name {type(bucket_name)}, expected {str}")
    if not isinstance(prefix, str):
        raise TypeError(f"prefix {type(prefix)}, expected {str}")

    try:
        log_string = "SUCCESS"
        file_name = f"{datetime.now().strftime('%d%m%Y%H%M')}" #ddmmyyhhmmss
        s3 = boto3.client('s3')
        
        s3.put_object(
            Body=log_string, 
            Bucket=bucket_name, 
            Key=f'{prefix}/{file_name}.txt'
        )
    except ClientError as e:
        print(e)
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.error(
                f"ResourceNotFoundException: the bucket {bucket_name} cannot be found in S3"
            )
            raise KeyError(
                f"ResourceNotFoundException: the bucket {bucket_name} cannot be found in S3"
            )
        elif e.response["Error"]["Code"] == "AccessDeniedException":
            logger.error(
                f"AccessDeniedException: the lambda does not have an identity-based policy to access S3 resource"
            )
            raise RuntimeError(
                f"AccessDeniedException: the lambda does not have an identity-based policy to access S3 resource"
            )
        else:
            logger.error(e)
            raise e
            
            
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