import json
import logging
from botocore.exceptions import ClientError
from src.utils.utils import get_secret, connect_db, get_table_db, upload_table_s3

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

    db_credentials = get_secret(AWS_SECRET_DB_CREDENTIALS_NAME)
    table_names = get_secret(AWS_SECRET_TABLES_NAMES).keys()

    try:
        connection = connect_db(db_credentials, db_name="totesys")

        for table_name in table_names:
            table_df, query = get_table_db(connection, table_name)

            upload_table_s3(table_df, table_name, INGESTION_BUCKET_NAME)

        return {"statusCode": 200}

    except Exception as e:
        logging.error("some unidentified error has occured!")
        raise e
