import json
from pprint import pprint
import pg8000
import boto3
import pandas as pd
from botocore.exceptions import ClientError
'''
Defines lambda function responsible for extracting the data 
from the database and depositing it in the ingestion bucket
'''
def extraction_lambda_handler(tableName, bucketName='test-va-0423', secretName='ingestion/db'):
    secretsmanager = boto3.client('secretsmanager')

    try: 
        db_credentials = secretsmanager.get_secret_value(
        SecretId = secretName
        )
    except ClientError as _e:
        if _e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("in error")
            raise Exception(f"The requested secret was not found")
        else:
            print(_e)
    db_creds = json.loads(db_credentials['SecretString']) # gets database credentials from secrets manager
    s3 = boto3.resource('s3')
    host = db_creds['host']
    port = db_creds['port'] 
    # port = '0808'     
    
    database = db_creds['dbname']
    user = db_creds['username']
    password = db_creds['password']


    try:
        connection = pg8000.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = connection.cursor()
        sqlColumns = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='"+tableName+"';"
        cursor.execute(sqlColumns) # gets the column names
        columnsRaw = cursor.fetchall()# gets the column names
        columns = []# gets the column names
        for columnName in columnsRaw:
            columns.append(columnName[3])# gets the column names
        sqlTable = "SELECT * FROM "+tableName+";"
        cursor.execute(sqlTable) # gets the data from a table (in this case design)
        rows = []
        rows.append(columns)
        rows.append(cursor.fetchall())# adds the column names to the start of the data
        df = pd.DataFrame(data=rows[1], columns=rows[0]) # uses pandas to convert to a dataframe
        output = df.to_csv()
        fileName = tableName
        obj = s3.Object(bucketName, fileName)# adds the data to the s3 (this case test-va-0423), as a csv file (test_design.csv)
        obj.put(Body=str(output))# adds the data to the s3 (this case test-va-0423), as a csv file (test_design.csv)
        cursor.close()
        connection.close()
        print("Connection closed.")
        
    except pg8000.Error as _e:
        raise NameError(f"Error connecting to the database: {_e}")
    
    except ClientError as _e:
        pprint(_e.response)
        if _e.response['Error']['Code'] == 'NoSuchBucket':
            raise Exception('Not a valid bucket')
        else:
            print(_e)
