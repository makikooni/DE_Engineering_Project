import pg8000
import boto3


def get_table(tableName):
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
        fileName = "test_" + tableName + ".csv"
        obj = s3.Object('test-va-0423', fileName)# adds the data to the s3 (this case test-va-0423), as a csv file (test_design.csv)
        result = obj.put(Body=str(rows))# adds the data to the s3 (this case test-va-0423), as a csv file (test_design.csv)
        cursor.close()
        connection.close()
        print("Connection closed.")
        
    except pg8000.Error as e:
        print(f"Error connecting to the database: {e}")

get_table('counterparty')
get_table('sales_order')
get_table('design')
get_table('address')
