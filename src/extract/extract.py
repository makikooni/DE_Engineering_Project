import json
import logging
from pg8000.native import Connection, InterfaceError, DatabaseError
import boto3
import pandas as pd
from botocore.exceptions import ClientError

"""
Defines lambda function responsible for extracting the data
from the database and depositing it in the ingestion bucket
"""

logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)


def extraction_lambda_handler(event, context):
    AWS_SECRET_TABLES_NAMES = "ingestion/db/table-names"
    AWS_SECRET_DB_CREDENTIALS_NAME = "ingestion/db/credentials"

    INGESTION_BUCKET_NAME = "test-va-ingestion-atif"

    db_credentials_response = get_secret(AWS_SECRET_DB_CREDENTIALS_NAME)
    table_names_response = get_secret(AWS_SECRET_TABLES_NAMES)

    db_credentials = json.loads(db_credentials_response["SecretString"])
    table_names = json.loads(table_names_response["SecretString"]).keys()

    try:
        connection = connect_db(db_credentials, db_name = "totesys")

        for table_name in table_names:
            table_df = get_table(connection, table_name)

            upload_table_s3(table_df, table_name, INGESTION_BUCKET_NAME)

        return {"statusCode": 200}

    except pg8000.Error as _e:
        raise NameError(f"Error connecting to the database: {_e}")

    except ClientError as _e:
        logging.error(_e.response)
        if _e.response["Error"]["Code"] == "NoSuchBucket":
            raise Exception("Not a valid bucket")
        else:
            logging.error(_e)


def get_secret(secret_name):
    secretsmanager = boto3.client("secretsmanager")
    try:
        logging.info(f"Retrieving {secret_name} information from SecretsManager...")

        response = secretsmanager.get_secret_value(
            SecretId=secret_name
        )

        logging.info(f"{secret_name} information successfully retrieved!")
        
        return response
    
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logging.error(f"ResourceNotFoundException: the secret named {secret_name} cannot be found in SecretsManager")
            raise KeyError(f"ResourceNotFoundException: the secret named {secret_name} cannot be found in SecretsManager")
        else:
            logging.error(e)
    except Exception as e:
        logging.error(e)


def get_table(connection, table_name):
    logging.info(f"Extracting {table_name} table from database...")

    query = f"SELECT * FROM {table_name};"

    table_data = connection.run(query)
    column_names = [col["name"] for col in connection.columns]

    table_df = pd.DataFrame(data=table_data, columns=column_names)

    logging.info(f"{table_name} table successfully extracted as dataframe!")

    return table_df

def upload_table_s3(table_df, table_name, ingestion_bucket_name):
    logging.info(
        f"uploading {table_name} table to {ingestion_bucket_name} S3 bucket..."
    )
    table_df.to_csv(f"s3://{ingestion_bucket_name}/{table_name}.csv")
    logging.info(
        f"{table_name} table successfully uploaded to {ingestion_bucket_name} S3 bucket!"
    )


def connect_db(db_credentials, db_name = ""):
    logging.info(f"Performing checks before connecting to database...")
    if not isinstance(db_credentials, dict):
        raise TypeError(f"db_credentials is {type(db_credentials)}, {dict} is required")
    elif not isinstance(db_name,str):
        raise TypeError(f"db_name is {type(db_name)}, {str} is required")
    
    db_credentials_keys = list(db_credentials.keys())
    required_keys = ["host", "port", "dbname", "username", "password"]

    for req_key in required_keys:
        if req_key not in db_credentials_keys:
            raise KeyError(f'db_credentials contains does not contain {req_key}')
    
    for key in db_credentials:
        if not isinstance(db_credentials[key], str):
            raise ValueError(f"value for {key} is {type(db_credentials[key])}, {str} is required")

    try:
        logging.info(f"Starting connection to {db_name} database...")
        connection = Connection(
            host=db_credentials["host"],
            port=db_credentials["port"],
            database=db_credentials["dbname"],
            user=db_credentials["username"],
            password=db_credentials["password"],
        )
        logging.info(f"Successfully connected to {db_name} database!")
        return connection
    except InterfaceError:
        logging.error(f"InterfaceError: please check your database credentials")
        raise InterfaceError
    
    except DatabaseError:
        logging.error(f"DatabaseError: please contact your database administrator")
        raise DatabaseError