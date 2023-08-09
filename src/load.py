import logging
from utils.utils import get_secret, connect_db
from utils.load_utils import (
    get_id_col, get_table_data,
    insert_data_format, build_insert_sql,
    insert_table_data, get_job_list,
    build_update_sql, update_data_format,
    rename_lastjob)

logger = logging.getLogger('LoadLogger')
logger.setLevel(logging.INFO)


def load_lambda_handler(event, context):
    PROCESSED_BUCKET_NAME = 'processed-va-052023'
    WAREHOUSE_DB_NAME = 'warehouse'
    WAREHOUSE_TABLE_NAMES = 'warehouse_table_names'

    db_creds = get_secret(WAREHOUSE_DB_NAME)
    new_jobs = get_job_list(PROCESSED_BUCKET_NAME)
    table_names = list(get_secret(WAREHOUSE_TABLE_NAMES).keys())

    try:
        connection = connect_db(db_creds)

        for table_name in table_names:

            for ts_dir in new_jobs:

                table_df = get_table_data(
                    table_name, PROCESSED_BUCKET_NAME, ts_dir)

                if len(table_df) > 0:

                    if table_df.startswith('dim'):

                        lst_wh_id = get_id_col(
                            connection, table_name, table_df)

                        for row in table_df.values.tolist():

                            if row[0] in lst_wh_id:
                                data = update_data_format(row)
                                query = build_update_sql(table_name, table_df)
                            else:
                                query = build_insert_sql(table_name, table_df)
                                data = insert_data_format(table_df)
                            insert_table_data(connection, query, data)

                    else:
                        query = build_insert_sql(table_name, table_df)
                        data = insert_data_format(table_df)
                        insert_table_data(connection, query, data)

        connection.close()

        rename_lastjob(PROCESSED_BUCKET_NAME)
    except Exception as error:
        logger.error["main function error", error]
        raise error
