from unittest import TestCase
import json
import boto3
import pg8000
# from mock import patch

class MockDB(TestCase):
    @classmethod
    def set_up_database(self):
        # secretsmanager = boto3.client('secretsmanager')
        # try:
        #     db_credentials = secretsmanager.get_secret_value(
        #         SecretId='test-Database-credentials'
        #     )
        # except Exception as error:
        #     raise error        
        #     # credentails for the warehouse
        # db_creds = json.loads(db_credentials['SecretString'])
        # host = db_creds['host']
        # port = db_creds['port']
        # database = db_creds['dbname']
        # user = db_creds['username']
        # password = db_creds['password']
        # connecting to warehouse
        connection = pg8000.connect(
            host='localhost',
            user='lucy',
            port=5432,
            database='postgres',
            password='QASW"1qa'
        )
        cursor = connection.cursor()
        connection.autocommit = True
        try:
            cursor.execute("DROP DATABASE Test_DB_load")
            print("DB dropped")
        except Exception as err:
            print("Test_DB_load tried to be deteted error:{}".format(err))
            cursor = connection.cursor()
    # create sql database
        try:
            cursor.execute("CREATE DATABASE Test_DB_load")
        except Exception as err:
            print("Failed creating database: {}".format(err))
    @classmethod
    def set_up_tables(self):
    # connection to database
        connection = pg8000.connect(
            host='localhost',
            user='lucy',
            port=5432,
            database='test_db_load',
            password='QASW"1qa'
        )
        cursor = connection.cursor()
        connection.autocommit = True
    # drop table
        try:
            cursor.execute("DROP TABLE if exists dim_design_t1")
        except Exception as err:
            print("test_table tried to be deteted error ={}".format(err))
            cursor = connection.cursor()
    # create table
        query = "CREATE TABLE dim_design_t1 (design_id int PRIMARY KEY, design_name VARCHAR(30), file_location VARCHAR(30), file_name VARCHAR(30))"
        try:
            cursor.execute(query)
            connection.commit()
            print("table created")
            connection.close()
        except Exception as err:
            print("Error with adding table", err)


New_database = MockDB()
# New_database.set_up_database()
New_database.set_up_tables()
