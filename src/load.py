import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_lambda_handler(event, context):
    logger.info("#=#=#=#=#=#=#=# LOAD LAMBDA =#=#=#=#=#=#=#=#")
    logger.info("Hello from the Load Lambda :)")
