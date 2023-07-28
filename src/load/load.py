import json
import pg8000
import logging
import boto3
import pandas as pd
from botocore.exceptions import ClientError
def update_table(s3_table_name, wh_table_name, secret_name = 'warehouse'):
    # get warehouse credentails from AWS secrets
    secretsmanager = boto3.client('secretsmanager')
    try:
        db_credentials = secretsmanager.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as error:
        # if _e.response['Error']['Code'] == 'ResourceNotFoundException':
            # raise Exception("The requested secret was not found")
        raise error
   
    try:
        
        # conecting to the process bucket
        process_bucket = 'processed-va-052023'
        # s3_resource = boto3.resource('s3')   
        # s3_client = boto3.client('s3')
        # response = s3_client.head_bucket(Bucket=process_bucket)
        # credentails for the warehouse
        db_creds = json.loads(db_credentials['SecretString'])
        host = db_creds['host']
        port = db_creds['port']
        database = db_creds['dbname']
        user = db_creds['username']
        password = db_creds['password']
        
        
        # connecting to warehouse
        connection = pg8000.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password
                )
        # get the table from the s3 and put it in a pandas dataframe
        table = pd.read_parquet(f's3://{process_bucket}/{s3_table_name}')
        table_data_frame = pd.DataFrame(table)
        # build the sql string
        cursor = connection.cursor()
        ## build the columns for the sql string
        list_columns = ''
        index_columns = 0
        for column in list(table_data_frame.columns):
            if index_columns != len(list(table_data_frame.columns))- 1:
                list_columns += str(column) + ', '
                index_columns += 1
            else:
                list_columns += column
        print(list_columns)
        ## build the new row for the sql string
        new_row = ''
        print(list(table_data_frame.loc[0]))
        index_rows = 0
        for row in list(list(table_data_frame.loc[0])):
            if index_rows != len(list(table_data_frame.loc[0]))- 1:
                new_row += str(row) + ', '
                index_rows += 1
            else:
                new_row += row
        print(new_row)
        ## put the sql string together
        update_table_sql = 'INSERT INTO ' + wh_table_name + ' (' +list_columns+') VALUES (' + new_row + ')' 
        cursor.execute(update_table_sql)
        cursor.close()
    except Exception as error:
        raise error
update_table('test_dim_design.parquet', 'dim_design')
