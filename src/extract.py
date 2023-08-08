import json
import logging
from utils.utils import (
    get_secret,
    connect_db,
    get_table_db,
    upload_table_s3,
    log_latest_job,
    trigger_transform_lambda,
)

"""
Defines lambda function responsible for extracting the data
from the database and depositing it in the ingestion bucket
"""

logger = logging.getLogger("ExtractionLogger")
logger.setLevel(logging.INFO)


def extraction_lambda_handler(event, context):
    """Handles scheduled event and transfers data from database to s3.

      On receipt of a Cloudwatch scheduled event:
        - connects to totesys database
        - extracts each table
        - uploads each table as a csv to ingestion zone

    Args:
        event:
            a valid Cloudwatch scheudle event
        context:
            a valid AWS lambda Python context object

    Raises:
        ValueError:
            Event resources arn does not match the expected arn.
        RuntimeError:
            An unexpected error occurred in execution. Other errors
            result in an informative log message.
    """
    DBNAME = "totesys"
    AWS_SECRET_TABLES_NAMES = "ingestion/db/table-names"
    AWS_SECRET_DB_CREDENTIALS_NAME = "ingestion/db/credentials"

    INGESTION_BUCKET_NAME = "ingestion-va-052023"
    CLOUDWATCH_TRIGGER_ARN = (
        "arn:aws:events:eu-west-2:454963742860:rule/extraction_schedule"
    )

    req_event_keys = [
        "id",
        "detail-type",
        "source",
        "account",
        "time",
        "region",
        "resources",
        "detail",
    ]

    if event["resources"][0] != CLOUDWATCH_TRIGGER_ARN:
        logger.error(
            f"cloudwatch trigger arn is {event['resources'][0]}, \n \
                       expected {CLOUDWATCH_TRIGGER_ARN} "
        )
        raise ValueError("Event schedule is incorrect")
    for key in req_event_keys:
        if key not in list(event.keys()):
            raise KeyError(
                f"This event is not a valid cloudwatch event.\
                           event object does not contain the key {key}"
            )

    logger.info(f'Lambda triggered on {event["time"]}')

    db_credentials = get_secret(AWS_SECRET_DB_CREDENTIALS_NAME)
    table_names = get_secret(AWS_SECRET_TABLES_NAMES).keys()

    try:
        connection = connect_db(db_credentials)
        logger.info(f"Successfully connected to {DBNAME} database!")

        JOB_TIMESTAMP = connection.run("SELECT NOW()")[0][0]

        for table_name in table_names:
            table_df, query = get_table_db(
                connection, table_name, INGESTION_BUCKET_NAME
            )

            upload_table_s3(table_df, table_name, INGESTION_BUCKET_NAME, JOB_TIMESTAMP)

            logger.info(f"{table_name} table successfully extracted and uploaded!")
        connection.close()
        logger.info("#=#=#=#=#= Extract Lambda Job Complete! =#=#=#=#=#")

        log_latest_job(bucket_name=INGESTION_BUCKET_NAME, timestamp=JOB_TIMESTAMP)

        trigger_transform_lambda(
            bucket_name=INGESTION_BUCKET_NAME, prefix="ExtractHistory"
        )

    except Exception as e:
        logger.error(e)
        raise RuntimeError


with open("tests/extract/valid_event.json") as v:
    event = json.loads(v.read())

    extraction_lambda_handler(event, {})
