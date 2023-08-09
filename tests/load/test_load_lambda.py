import pytest
from moto import mock_s3
import boto3
import pg8000
import awswrangler as wr
import pandas as pd
from datetime import datetime
from pandas.testing import assert_frame_equal
from utils.load_utils import get_table_data, insert_data_format, build_insert_sql, insert_table_data, build_update_sql, update_data_format, get_id_col, get_job_list
from tests.MockDB.MockDB import MockDB

@pytest.fixture
def create_s3_client():
    with mock_s3():
        yield boto3.client('s3', region_name='eu-west-2')

@pytest.fixture
def create_s3_resource():
    with mock_s3():
        yield boto3.resource('s3', region_name='eu-west-2')

@pytest.fixture
def mock_client(create_s3_client, create_s3_resource):
    mock_client = create_s3_client
    conn = create_s3_resource
    processed_bucket_name = 'processed-va-052023'
    file_name = 'test_dim_design'
    mock_client.create_bucket(
        Bucket=processed_bucket_name,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
        )
    test_data_1 = [[
        'design_id', 'design_name', 'file_location', 'file_name'
    ],
    [{
        'design_id':'8',
        'design_name': 'Wooden',
        'file_location': '/usr',
        'file_name': 'wooden-20220717-npgz.json'
    }]]
    test_data_2 = [[
        'design_id', 'design_name', 'file_location', 'file_name'
    ],
    [{
        'design_id':'7',
        'design_name': 'Wooden',
        'file_location': '/usr',
        'file_name': 'wooden-20220717-npgz.json'
    }]]
    test_data_3 = [[
        'design_id', 'design_name', 'file_location', 'file_name'
    ],
    [{
        'design_id':'8',
        'design_name': 'Wood',
        'file_location': '/usr',
        'file_name': 'wooden-20220717-npgz.json'
    }]]
    df_data_1 = pd.DataFrame(data = test_data_1[1], columns = test_data_1[0])
    df_data_2 = pd.DataFrame(data = test_data_2[1], columns = test_data_2[0])
    df_data_3 = pd.DataFrame(data = test_data_3[1], columns = test_data_3[0])
    wr.s3.to_parquet(df=df_data_1, path=f's3://{processed_bucket_name}/20230808110721/{file_name}.parquet')
    wr.s3.to_parquet(df=df_data_2, path=f's3://{processed_bucket_name}/20230808110752/{file_name}.parquet')
    wr.s3.to_parquet(df=df_data_3, path=f's3://{processed_bucket_name}/20230808110813/{file_name}.parquet')
    wr.s3.upload(local_file = './tests/load/lastjob.csv', path=f's3://{processed_bucket_name}/lastjob/lastjob.csv')
    yield mock_client

def test_get_table_data(mock_client):
    test_data = [[
        'design_id', 'design_name', 'file_location', 'file_name'
    ],
    [{
        'design_id':'8',
        'design_name': 'Wooden',
        'file_location': '/usr',
        'file_name': 'wooden-20220717-npgz.json'
    }]]
    df_data = pd.DataFrame(data = test_data[1], columns = test_data[0])
    # expect = wr.s3.to_parquet(df=df_data)
    output = get_table_data('test_dim_design', 'processed-va-052023', '20230808110721')
    assert assert_frame_equal(output, df_data, check_dtype = False) == None
    
def test_dataframe_to_list_returns_a_list():
    test_data = [[
        'design_id', 'design_name', 'file_location', 'file_name'
    ],
    [{
        'design_id':'8',
        'design_name': 'Wooden',
        'file_location': '/usr',
        'file_name': 'wooden-20220717-npgz.json'
    }]]
    df_data = pd.DataFrame(data = test_data[1], columns = test_data[0])
    output = insert_data_format(df_data)
    assert isinstance(output, list)

def test_dataframe_to_list_returns_only_data():
    test_data = [[
        'design_id', 'design_name', 'file_location', 'file_name'
    ],
    [{
        'design_id':'8',
        'design_name': 'Wooden',
        'file_location': '/usr',
        'file_name': 'wooden-20220717-npgz.json'
    }]]
    df_data = pd.DataFrame(data = test_data[1], columns = test_data[0])
    output = insert_data_format(df_data)
    expect = [('8', 'Wooden', '/usr', 'wooden-20220717-npgz.json')]
    assert output == expect

def test_dataframe_to_list_returns_only_data_with_multiple_rows():
    test_data = [[
        'design_id', 'design_name', 'file_location', 'file_name'
    ],
    [{
        'design_id':'8',
        'design_name': 'Wooden',
        'file_location': '/usr',
        'file_name': 'wooden-20220717-npgz.json'
    },
    {
        'design_id':'7',
        'design_name': 'Woden',
        'file_location': '/us',
        'file_name': 'wooden.json'
    }]]
    df_data = pd.DataFrame(data = test_data[1], columns = test_data[0])
    output = insert_data_format(df_data)
    expect = [('8', 'Wooden', '/usr', 'wooden-20220717-npgz.json'), ('7', 'Woden', '/us', 'wooden.json')]
    assert output == expect

def test_build_insert_sql():
    test_data = [[
        'design_id', 'design_name', 'file_location', 'file_name', 'example'
    ],
    [{
        'design_id':'8',
        'design_name': 'Wooden',
        'file_location': '/usr',
        'file_name': 'wooden-20220717-npgz.json',
        'example': 'hello'
    }]]
    df_data = pd.DataFrame(data = test_data[1], columns = test_data[0])
    output = build_insert_sql('dim_design', df_data)
    expect = "INSERT INTO dim_design (design_id, design_name, file_location, file_name, example) "\
            "VALUES (%s,%s,%s,%s,%s) "
    
    assert  output == expect

def test_build_insert_sql_with_different_amount_of_columns():
    test_data = [[
        'design_id', 'design_name', 'file_location', 'file_name'
    ],
    [{
        'design_id':'8',
        'design_name': 'Wooden',
        'file_location': '/usr',
        'file_name': 'wooden-20220717-npgz.json'
    }]]
    df_data = pd.DataFrame(data = test_data[1], columns = test_data[0])
    output = build_insert_sql('dim_design', df_data)
    expect = "INSERT INTO dim_design (design_id, design_name, file_location, file_name) "\
            "VALUES (%s,%s,%s,%s) "
    assert  output == expect

    test_data = [[
        'design_id', 'design_name', 'file_location'
    ],
    [{
        'design_id':'8',
        'design_name': 'Wooden',
        'file_location': '/usr'
    }]]
    df_data = pd.DataFrame(data = test_data[1], columns = test_data[0])
    output = build_insert_sql('dim_design', df_data)
    expect = "INSERT INTO dim_design (design_id, design_name, file_location) "\
            "VALUES (%s,%s,%s) "
    assert  output == expect

# def test_insert_table_data_works_with_insert_sql():
#     test_db = MockDB
#     test_db.set_up_database()
#     test_db.set_up_tables()
#     data_to_insert = [('8', 'Wooden', '/usr', 'wooden-20220717-npgz.json')]
#     insert_table_sql =  "INSERT INTO dim_design_t1 (design_id, design_name, file_location, file_name) "\
#                         "VALUES (%s,%s,%s,%s) "
#     connection = pg8000.connect(
#             host='localhost',
#             user='david',
#             port=5432,
#             database='test_db_load',
#             password='Paprika5'
#         )
#     insert_table_data(connection,insert_table_sql, data_to_insert)
#     cursor = connection.cursor()
#     cursor.execute("SELECT * FROM dim_design_t1")
#     output = cursor.fetchall()
#     connection.close()
#     expect = [8, 'Wooden', '/usr', 'wooden-20220717-npgz.json']
#     assert output[0] == expect
#     test_db.insert_data_to_update()

# def test_insert_table_data_works_with_update_sql():
#     test_db = MockDB
#     test_db.set_up_database()
#     test_db.set_up_tables()
#     test_db.insert_data_to_update()
#     data_to_insert = [( 'Wooden',  '/usr', 'wooden-20220717-npgz.json', '8')]
#     insert_table_sql =  "UPDATE dim_design_t1 SET design_name = %s, file_location = %s, file_name = %s WHERE design_id = %s"
#     connection = pg8000.connect(
#             host='localhost',
#             user='david',
#             port=5432,
#             database='test_db_load',
#             password='Paprika5'
#         )
#     insert_table_data(connection,insert_table_sql, data_to_insert)
#     cursor = connection.cursor()
#     cursor.execute("SELECT * FROM dim_design_t1")
#     output = cursor.fetchall()
#     connection.close()
#     expect = [8, 'Wooden', '/usr', 'wooden-20220717-npgz.json']
#     assert output[0] == expect

# def test_insert_table_data_works_with_update_sql_with_multiple_data():
#     test_db = MockDB
#     test_db.set_up_database()
#     test_db.set_up_tables()
#     test_db.insert_data_to_update()
#     test_db.insert_data_to_update_2()

#     data_to_insert = [( 'Wooden',  '/usr', 'wooden-20220717-npgz.json', '8'), ( 'Wooden',  '/usr', 'wooden-20220717-npgz.json', '7')]
#     insert_table_sql =  "UPDATE dim_design_t1 SET design_name = %s, file_location = %s, file_name = %s WHERE design_id = %s"
#     connection = pg8000.connect(
#             host='localhost',
#             user='david',
#             port=5432,
#             database='test_db_load',
#             password='Paprika5'
#         )
#     insert_table_data(connection,insert_table_sql, data_to_insert)
#     cursor = connection.cursor()
#     cursor.execute("SELECT * FROM dim_design_t1")
#     output = cursor.fetchall()
#     expect = ([8, 'Wooden', '/usr', 'wooden-20220717-npgz.json'], [7, 'Wooden', '/usr', 'wooden-20220717-npgz.json'])
#     assert output == expect

def test_build_update_sql_return_string():
    test_data = [[
        'design_id', 'design_name', 'file_location', 'file_name'
    ],
    [{
        'design_id':'8',
        'design_name': 'Wooden',
        'file_location': '/usr',
        'file_name': 'wooden-20220717-npgz.json'
    }]]
    df_data = pd.DataFrame(data = test_data[1], columns = test_data[0])
    output = build_update_sql('dim_design', df_data)
    assert isinstance(output, str)

def test_build_update_sql_returns_string_desired_string():
    test_data = [[
        'design_id', 'design_name', 'file_location', 'file_name'
    ],
    [{
        'design_id':'8',
        'design_name': 'Wooden',
        'file_location': '/usr',
        'file_name': 'wooden-20220717-npgz.json'
    }]]
    df_data = pd.DataFrame(data = test_data[1], columns = test_data[0])
    output = build_update_sql('dim_design', df_data)
    expect = "UPDATE dim_design SET design_name = %s, file_location = %s, file_name = %s WHERE design_id = %s"
    print(expect, "= expect")
    print(output, "= output")
    assert output == expect

def test_update_data_format_returns_a_list():
    test_data =['8','Wooden','/usr','wooden-20220717-npgz.json']
    output = update_data_format(test_data)
    assert isinstance(output, list)

def test_update_data_format_returns_a_list_in_right_format():
    test_data =['8','Wooden','/usr','wooden-20220717-npgz.json']
    expect = ['Wooden', '/usr', 'wooden-20220717-npgz.json', '8']
    output = update_data_format(test_data)
    assert expect == output

def test_get_job_list_returns_list(mock_client):
    output = get_job_list('processed-va-052023')
    print(output)
    assert isinstance(output, list)

def test_get_job_list_returns_list_that_is_wanted(mock_client):
    output = get_job_list('processed-va-052023')
    expect = ['20230808110721', '20230808110752', '20230808110813']
    assert output == expect

# def test_get_col_id_returns_list():
#     test_db = MockDB
#     test_db.set_up_database()
#     test_db.set_up_tables()
#     test_db.insert_data_to_update()
#     test_db.insert_data_to_update_2()

#     connection = pg8000.connect(
#             host='localhost',
#             user='david',
#             port=5432,
#             database='test_db_load',
#             password='Paprika5'
#         )
#     test_data = [[
#         'design_id', 'design_name', 'file_location', 'file_name'
#     ],
#     [{
#         'design_id':'8',
#         'design_name': 'Wooden',
#         'file_location': '/usr',
#         'file_name': 'wooden-20220717-npgz.json'
#     },
#     {
#         'design_id':'7',
#         'design_name': 'Woden',
#         'file_location': '/us',
#         'file_name': 'wooden.json'
#     }]]
#     df_data = pd.DataFrame(data = test_data[1], columns = test_data[0]) 
#     output = get_id_col(connection, 'dim_design_t1', df_data)
#     print(output)
#     assert isinstance(output, list)
