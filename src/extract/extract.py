import json
import logging
from pprint import pprint
import pg8000
import boto3
import pandas as pd
from botocore.exceptions import ClientError
'''
Defines lambda function responsible for extracting the data
from the database and depositing it in the ingestion bucket
'''

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def extraction_lambda_handler(event, context):

    AWS_SECRET_TABLES_NAMES = 'ingestion/db/table-names'
    AWS_SECRET_DB_CREDENTIALS_NAME = 'ingestion/db/credentials'
    
    INGESTION_BUCKET_NAME = 'test-va-ingestion-atif'

    db_credentials, table_names = get_secrets(
        db_secret_name=AWS_SECRET_DB_CREDENTIALS_NAME,
        tables_secret_name=AWS_SECRET_TABLES_NAMES
    )

    try:
        connection = connect_totesys_db(db_credentials)

        for table_name in table_names:

            table_df = get_table(connection, table_name)

            upload_table_s3(table_df, table_name, INGESTION_BUCKET_NAME)

        return {
            'statusCode': 200
            }

    except pg8000.Error as _e:
        raise NameError(f"Error connecting to the database: {_e}")

    except ClientError as _e:
        pprint(_e.response)
        if _e.response['Error']['Code'] == 'NoSuchBucket':
            raise Exception('Not a valid bucket')
        else:
            print(_e)


def get_secrets(db_secret_name, tables_secret_name):

    secretsmanager = boto3.client('secretsmanager')
    try:
        logging.info(
            "Retrieving confidential information from SecretsManager...")

        database_credentials_res = secretsmanager.get_secret_value(
            SecretId=db_secret_name
        )

        table_names_res = secretsmanager.get_secret_value(
            SecretId=tables_secret_name
        )

        db_credentials = json.loads(database_credentials_res['SecretString'])
        table_names = json.loads(table_names_res['SecretString']).keys()

        logging.info("Confidential information successfully retrieved!")
        return db_credentials, table_names

    except ClientError as _e:
        if _e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error("ResourceNotFoundException")
            raise Exception("The requested secret was not found")
        else:
            print(_e)
    except Exception as _e:
        logger.error(e)


def get_table(connection, table_name):
    logging.info(f"Extracting {table_name} table from database...")

    column_names = get_column_names(table_name, connection)

    table_data = get_table_data(table_name, connection)

    table_df = pd.DataFrame(data=table_data, columns=column_names)

    logging.info(f"{table_name} table successfully extracted as dataframe!")

    return table_df


def get_column_names(table, connection):
    cursor = connection.cursor()

    column_names = []
    table_cols_query = "select * from "\
        "INFORMATION_SCHEMA.COLUMNS" \
        " where TABLE_NAME='" + table + "';"

    cursor.execute(table_cols_query)
    columns_data = cursor.fetchall()

    for column in columns_data:
        column_names.append(column[3])

    cursor.close()
    return column_names


def get_table_data(table, connection):
    cursor = connection.cursor()
    rows = []

    table_query = "SELECT * FROM " + table + ";"
    cursor.execute(table_query)
    table_data = cursor.fetchall()

    cursor.close()
    return table_data


def upload_table_s3(table_df, table_name, ingestion_bucket_name):

    logging.info(
        f"uploading {table_name} table to {ingestion_bucket_name} S3 bucket..."
    )
    table_df.to_csv(f"s3://{ingestion_bucket_name}/{table_name}.csv")
    logging.info(
        f"{table_name} table successfully uploaded to {ingestion_bucket_name} S3 bucket!"
    )


def connect_totesys_db(db_credentials):

    logging.info(f"Starting connection to totsys database...")

    connection = pg8000.connect(
        host=db_credentials['host'],
        port=db_credentials['port'],
        database=db_credentials['dbname'],
        user=db_credentials['username'],
        password=db_credentials['password']
    )

    logging.info(f"Successfully connected!")
    return connection
