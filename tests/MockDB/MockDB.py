from unittest import TestCase
from mock import patch

class MockDB(TestCase):
# delete database if there
    try:
       cursor.execute("DROP DATABASE Test_DB")
       cursor.close()
       print("DB dropped")
    except Exception as err:
       print("Test_DB{}".format(err))
    cursor = cnx.cursor(dictionary=True)
 # create sql database
    try:
       cursor.execute(
           "CREATE DATABASE Test_DB DEFAULT CHARACTER SET 'utf8'")
    except mysql.connector.Error as err:
       print("Failed creating database: {}".format(err))
       exit(1)
    cnx.database = Test_DB
   
 # create table

    query = """CREATE TABLE `dim_desgin` (
             `text` text NOT NULL
           )"""
    try:
       cursor.execute(query)
       cnx.commit()
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
           print("test_table already exists.")
      else:
           print(err.msg)
    else:
       print("OK")