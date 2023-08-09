import logging
from utils.utils import (
    get_secret,
    connect_db,
    get_table_db,
    upload_table_s3,
    log_latest_job_extract,
    trigger_transform_lambda,
)


logger = logging.getLogger("ExtractionLogger")
logger.setLevel(logging.INFO)


def extraction_lambda_handler(event, context):
    """
    Entry point for Extraction Lambda.
    Verifies CloudWatch scheduled event trigger, and required event keys,
    before connecting to the Terriffic Totes RDBMS using credentials retrieved
    from AWS Secrets Manager. Then extracts tables from RDBMS, storing them as
    CSV files in our ingestion s3 bucket, and logs the extraction progress.

    Args:
        event (dict): event data containing info about CloudWatch event which
        triggered the lambda function. The expected data structure follows:
            {
                "id": "12345678",
                "detail-type": "DatabaseExtraction",
                "source": "aws.source-database",
                "account": "123456789012",
                "time": "2001-01-01T12:34:56Z",
                "region": "eu-west-2",
                "resources": [
                    "arn:aws:events:eu-west-2:123456789012:rule/extraction_schedule"
                ],
                "detail": {
                    "table_name": "table-name",
                    "database": "database-name"
                }
            }

        context (LambdaContext):
            Runtime information about the lambda function.

    Returns:
        None.

    Raises:
        ValueError:
            Raised when the event's trigger does not match the anticipated
            CloudWatch trigger ARN.

        KeyError:
            Raised if any of the required event keys are not present.

        RuntimeError:
            Raised if an error occurs during either the extraction or
            upload processes.
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

            upload_table_s3(
                table_df,
                table_name,
                INGESTION_BUCKET_NAME,
                JOB_TIMESTAMP)

            logger.info(
                f"{table_name} table successfully extracted and uploaded!")
        connection.close()
        logger.info("#=#=#=#=#= Extract Lambda Job Complete! =#=#=#=#=#")

        log_latest_job_extract(
            bucket_name=INGESTION_BUCKET_NAME, timestamp=JOB_TIMESTAMP
        )

        trigger_transform_lambda(
            bucket_name=INGESTION_BUCKET_NAME, prefix="ExtractHistory"
        )

    except Exception as e:
        logger.error(e)
        raise RuntimeError
