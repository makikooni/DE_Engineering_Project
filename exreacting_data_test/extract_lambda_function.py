import pg8000
import boto3
import pandas as pd
from pprint import pprint
from botocore.exceptions import ClientError

def put_table(tableName, bucketName='test-va-0423'):
    session = boto3.Session(
    aws_access_key_id='AKIAWT3PTZCGJVV2RIGV',
    aws_secret_access_key='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'
    )
    s3 = session.resource('s3')
    host = 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
    port = '5432'
    database = 'totesys'
    user = 'project_user_3'
    password = 'I4NX4jLv8i9VdeeM43uWBKPV'
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
        output = df.to_string()
        fileName = tableName + ".csv"
        obj = s3.Object(bucketName, fileName)# adds the data to the s3 (this case test-va-0423), as a csv file (test_design.csv)
        obj.put(Body=output)# adds the data to the s3 (this case test-va-0423), as a csv file (test_design.csv)
        cursor.close()
        connection.close()
        print("Connection closed.")
        
    except pg8000.Error as e:
        raise NameError(f"Error connecting to the database: {e}")
        # print(f"Error connecting to the database: {e}")
    
    except ClientError as e:
       if e.response['Error']['Code'] == 'NoSuchBucket':
           raise Exception('Not a valid bucket')
        
        # print(e.value())
        # pass

        # print(dir(e))

put_table(tableName='design', bucketName='test-va-0424')

# def get_table():
#     session = boto3.Session(
#     aws_access_key_id='AKIAWT3PTZCGJVV2RIGV',
#     aws_secret_access_key='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'
#     )

#     client = boto3.client('s3')
#     response = client.get_object(
#         Bucket='test-va-0423',
#         Key='test_design.csv'
#     )
#     print(response)

#     bucket = client._bucket('test-va-0423')
#     for obj in bucket.get_all_keys():
#         print(obj.key)

# def put_table(tableName):
#     session = boto3.Session(
#     aws_access_key_id='AKIAWT3PTZCGJVV2RIGV',
#     aws_secret_access_key='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'
#     )
#     s3 = session.resource('s3')
#     host = 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
#     port = '5432'
#     database = 'totesys'
#     user = 'project_user_3'
#     password = 'I4NX4jLv8i9VdeeM43uWBKPV'
#     try:
#         connection = pg8000.connect(
#             host=host,
#             port=port,
#             database=database,
#             user=user,
#             password=password
#         )
#         cursor = connection.cursor()
#         sqlColumns = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='"+tableName+"';"
#         cursor.execute(sqlColumns) # gets the column names
#         columnsRaw = cursor.fetchall()# gets the column names
#         columns = []# gets the column names
#         for columnName in columnsRaw:
#             columns.append(columnName[3])# gets the column names
#         sqlTable = "SELECT * FROM "+tableName+";"
#         cursor.execute(sqlTable) # gets the data from a table (in this case design)
#         rows = []
#         rows.append(columns)
#         rows.append(cursor.fetchall())# adds the column names to the start of the data
#         fileName = "test_" + tableName + ".csv"
#         obj = s3.Object('test-va-0423', fileName)# adds the data to the s3 (this case test-va-0423), as a csv file (test_design.csv)
#         result = obj.put(Body=str(rows))# adds the data to the s3 (this case test-va-0423), as a csv file (test_design.csv)
#         cursor.close()
#         connection.close()
#         print("Connection closed.")
#         print(rows)
#         df = pd.DataFrame(rows)
#         print(df)
        
#     except pg8000.Error as e:
#         print(f"Error connecting to the database: {e}")





    # s3 = session.resource('s3')
    # host = 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
    # port = '5432'
    # database = 'totesys'
    # user = 'project_user_3'
    # password = 'I4NX4jLv8i9VdeeM43uWBKPV'
    # try:
        # connection = pg8000.connect(
        #     host=host,
        #     port=port,
        #     database=database,
        #     user=user,
        #     password=password
        # )
        # cursor = connection.cursor()
    #     sqlColumns = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='"+tableName+"';"
    #     cursor.execute(sqlColumns) # gets the column names
    #     columnsRaw = cursor.fetchall()# gets the column names
    #     columns = []# gets the column names
    #     for columnName in columnsRaw:
    #         columns.append(columnName[3])# gets the column names
    #     sqlTable = "SELECT * FROM "+tableName+";"
    #     cursor.execute(sqlTable) # gets the data from a table (in this case design)
    #     rows = []
    #     rows.append(columns)
    #     rows.append(cursor.fetchall())# adds the column names to the start of the data
    #     fileName = "test_" + tableName + ".csv"
    #     obj = s3.Object('test-va-0423', fileName)# adds the data to the s3 (this case test-va-0423), as a csv file (test_design.csv)
    #     result = obj.put(Body=str(rows))# adds the data to the s3 (this case test-va-0423), as a csv file (test_design.csv)
    #     cursor.close()
    #     connection.close()
    #     print("Connection closed.")
        
    # except pg8000.Error as e:
    #     print(f"Error connecting to the database: {e}")

# put_table('counterparty')
# put_table('sales_order')


# put_table('address')

# get_table()
