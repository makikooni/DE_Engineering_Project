import logging
import pg8000
import numpy as np
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
    # table_names = list(get_secret(WAREHOUSE_TABLE_NAMES).keys())
    table_names = ['fact_payment', 'fact_purchase_order', 'fact_sales_order' ]
    # table_names = [ 'dim_date', 'dim_design', 'dim_staff', 'dim_counterparty',
    #                'dim_currency', 'dim_location', 'dim_payment_type', 'dim_transaction']
    new_jobs = ["20230809105942"]
    try:
        # connection = connect_db(db_creds)
        connection = pg8000.connect(
            host=db_creds['host'],
            port=db_creds['port'],
            database=db_creds['dbname'],
            user=db_creds['username'],
            password=db_creds['password']
        )
        logger.info(f'successfully connected to database')
        print(f'successfully connected to database')

        for table_name in table_names:

            for ts_dir in new_jobs:
                # if ts_dir != "20230809105942":

                logger.info(f'looping over {table_name} in directory {ts_dir}')
                print(f'looping over {table_name} in directory {ts_dir}')

                table_df = get_table_data(
                    table_name, PROCESSED_BUCKET_NAME, ts_dir)
                logger.info(f'successfully retrieved {table_name} dataframe')
                print(f'successfully retrieved {table_name} dataframe')
                
                if table_name == 'dim_transaction':
                    table_df.replace({np.nan: -1}, inplace=True)
                    table_df = table_df.astype({"sales_order_id":'int', "purchase_order_id":'int'}) 
                    table_df.replace({-1: None}, inplace=True)

                if len(table_df) > 0:
                    logger.info(f'trying to load {table_name}...')
                    print(f'trying to load {table_name}...')

                    if table_name.startswith('dim'):
                        logger.info("we are in the dim section")
                        print("we are in the dim section")
                        lst_wh_id = get_id_col(
                            connection, table_name, table_df)
                        for row in table_df.values.tolist():
                            if row[0] in lst_wh_id:
                                query = build_update_sql(table_name, table_df)
                                data = [tuple(update_data_format(row))]
                            else:
                                query = build_insert_sql(table_name, table_df)
                                data = [tuple(row)] # insert_data_format(table_df)
                                print(data)
                            insert_table_data(connection, query, data)
                    else:
                        query = build_insert_sql(table_name, table_df)
                        data = insert_data_format(table_df)
                        insert_table_data(connection, query, data)
                        print(f'successfully loaded {table_name}...')
                else:
                    logger.info(f'SKIPPING: {table_name} - no data to add')
                    print(f'SKIPPING: {table_name} - no data to add')

                logger.info(f'successfully loaded {table_name} to the warehouse')
                print(f'successfully loaded {table_name} to the warehouse')

        connection.close()

        # rename_lastjob(PROCESSED_BUCKET_NAME)
    except Exception as error:
        logger.error("main function error")
        raise error


load_lambda_handler({}, {})
