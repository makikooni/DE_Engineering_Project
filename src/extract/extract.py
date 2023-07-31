import logging
from src.utils.utils import get_secret, connect_db, get_table_db, upload_table_s3

"""
Defines lambda function responsible for extracting the data
from the database and depositing it in the ingestion bucket
"""

logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)


def extraction_lambda_handler(event, context):
    """Handles scheduled invocation event and transfers data from database to s3.

      One receipt of a Cloudwatch scheduled invocation event, connects to totesys database and
      extracts each table and uploads as a csv to ingestion zone (S3 bucket).

    Args:
        event:
            a valid Cloudwatch scheudle event -
            see https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
        context:
            a valid AWS lambda Python context object - see
            https://docs.aws.amazon.com/lambda/latest/dg/python-context.html

    Raises:
        ValueError: Event resources arn does not match the expected arn.
        RuntimeError: An unexpected error occurred in execution. Other errors
        result in an informative log message.
    """
    AWS_SECRET_TABLES_NAMES = "ingestion/db/table-names"
    AWS_SECRET_DB_CREDENTIALS_NAME = "ingestion/db/credentials"

    INGESTION_BUCKET_NAME = "test-va-ingestion-atif"
    CLOUDWATCH_TRIGGER_ARN = (
        "arn:aws:events:eu-west-2:454963742860:rule/extraction_schedule"
    )

    if event["resources"] != CLOUDWATCH_TRIGGER_ARN:
        raise ValueError("Event schedule is incorrect")

    logger.info(
        f'The lambda has been triggered at {event["time"]} by a {event["detail-type"]} '
    )
    logger.info(f'event["detail"] is: \n {event["detail"]} ')

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

    except Exception as e:
        logger.error(e)
        raise RuntimeError
