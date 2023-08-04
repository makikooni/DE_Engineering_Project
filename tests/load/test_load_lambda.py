import pytest
from moto import mock_s3
import boto3
import pg8000
import awswrangler as wr
import pandas as pd
from pandas.testing import assert_frame_equal
from src.load.load import get_table_data, dataframe_to_list, build_insert_sql, insert_table_data, build_update_sql, update_data_format
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
    processed_bucket_name = 'test-processed-va-052023'
    file_name = 'test_dim_design'
    mock_client.create_bucket(
        Bucket=processed_bucket_name,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
        )
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
    wr.s3.to_parquet(df=df_data, path=f's3://{processed_bucket_name}/{file_name}.parquet')
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
    output = get_table_data('test_dim_design.parquet')
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
    output = dataframe_to_list(df_data)
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
    output = dataframe_to_list(df_data)
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
    output = dataframe_to_list(df_data)
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
    expect = "INSERT INTO dim_design (design_id, design_name, file_location, file_name, example) VALUES (%s,%s,%s,%s,%s)"
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
    expect = "INSERT INTO dim_design (design_id, design_name, file_location, file_name) " \
        "VALUES (%s,%s,%s,%s)"
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
    expect = "INSERT INTO dim_design (design_id, design_name, file_location) VALUES (%s,%s,%s)"
    assert  output == expect

def test_insert_table_data():
    test_db = MockDB
    test_db.set_up_database()
    test_db.set_up_tables()
    data_to_insert = [('8', 'Wooden', '/usr', 'wooden-20220717-npgz.json')]
    insert_table_sql = "INSERT INTO dim_design_t1 (design_id, design_name, file_location, file_name) VALUES (%s,%s,%s,%s)"
    connection = pg8000.connect(
            host='localhost',
            user='lucy',
            port=5432,
            database='test_db_load',
            password='QASW"1qa'
        )
    insert_table_data(connection,insert_table_sql, data_to_insert)
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM dim_design_t1")
    output = cursor.fetchall()
    expect = [8, 'Wooden', '/usr', 'wooden-20220717-npgz.json']
    assert output[0] == expect

def skip_test_check_update_or_rows():
    check_update_or_rows()

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

def test_build_update_sql_returnds_string_desired_string():
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
    expect = 'UPDATE dim_design SET %s = %s, %s = %s, %s = %s WHERE %s = %s'
    assert output == expect

def test_update_data_format_returns_a_list():
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
    output = update_data_format(df_data)
    assert isinstance(output, list)