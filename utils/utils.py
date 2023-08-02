import awswrangler as wr
import logging
import json
from pg8000.native import Connection, InterfaceError, DatabaseError, identifier, literal
import boto3
import pandas as pd
from botocore.exceptions import ClientError


def get_secret(secret_name):
    if not isinstance(secret_name, str):
        raise TypeError(f"secret_name is {type(secret_name)}, {str} is required")

    secretsmanager = boto3.client("secretsmanager")
    try:
        logging.info(f"Retrieving {secret_name} information from SecretsManager...")

        response = secretsmanager.get_secret_value(SecretId=secret_name)
        secret_value = json.loads(response["SecretString"].replace("'", '"'))
        logging.info(f"{secret_name} information successfully retrieved!")
        return secret_value

    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logging.error(
                f"ResourceNotFoundException: the secret named {secret_name} cannot be found in SecretsManager"
            )
        elif e.response["Error"]["Code"] == "AccessDeniedException":
            logging.error(
                f"AccessDeniedException: the lambda does not have an identity-based policy to access SecretsManager resource "
            )
            raise KeyError(
                f"ResourceNotFoundException: the secret named {secret_name} cannot be found in SecretsManager"
            )
        else:
            logging.error(e)


def get_table_db(connection, table_name):
    if not isinstance(table_name, str):
        raise TypeError(f"table name is {type(table_name)}, expected {str}")
    if not isinstance(connection, Connection):
        raise TypeError(
            f"connection object is {type(connection)}, expected {Connection}"
        )

    logging.info(f"Extracting {table_name} table from database...")
    try:
        query = f"SELECT * FROM {identifier(table_name)};"

        table_data = connection.run(query)
        column_names = [col["name"] for col in connection.columns]

        table_df = pd.DataFrame(data=table_data, columns=column_names)

        logging.info(f"{table_name} table successfully extracted as dataframe!")

        return table_df, query
    except InterfaceError:
        logging.error(f"InterfaceError: the query: \n {query} \n cannot be executed.")
        raise InterfaceError


def upload_table_s3(table_df, table_name, bucket_name):
    if not isinstance(table_df, pd.DataFrame):
        raise TypeError(f"table dataframe {type(table_df)}, expected {pd.DataFrame}")
    if not isinstance(table_name, str):
        raise TypeError(f"table name is {type(table_name)}, expected {str}")
    if not isinstance(bucket_name, str):
        raise TypeError(f"ingestion bucket name is {type(bucket_name)}, expected {str}")

    try:
        boto3.client("s3").head_bucket(Bucket=bucket_name)

        logging.info(f"uploading {table_name} table to {bucket_name} S3 bucket...")

        wr.s3.to_csv(table_df, f"s3://{bucket_name}/{table_name}.csv", index=False)

        logging.info(
            f"{table_name} table successfully uploaded to {bucket_name} S3 bucket!"
        )
    except Exception as e:
        error = e.response["Error"]
        if e.response["Error"]["Code"] == "NoSuchBucket":
            logging.error(f"NoSuchBucket: {error['BucketName']} does not exist")

            raise KeyError(f"{error['BucketName']} does not exist")
        else:
            raise e


def connect_db(db_credentials, db_name=""):
    logging.info(f"Performing checks before connecting to database...")
    if not isinstance(db_credentials, dict):
        raise TypeError(f"db_credentials is {type(db_credentials)}, {dict} is required")
    elif not isinstance(db_name, str):
        raise TypeError(f"db_name is {type(db_name)}, {str} is required")

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
