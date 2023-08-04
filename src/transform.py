import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def transform_lambda_handler(event, context):
    logger.info("#=#=#=#=#=#=#=# TRANSFORM LAMBDA =#=#=#=#=#=#=#=#")
    logger.info("Hello from the Transform Lambda :)")
