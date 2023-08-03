import pytest
from moto import mock_s3
import boto3
import awswrangler as wr
import pandas as pd
from pandas.testing import assert_frame_equal
from src.load.load import add_new_rows, get_table_data, dataframe_to_list, build_load_sql, insert_table_data
from pprint import pprint

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
    print(df_data, "= data in bucket")
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
    print(df_data,  "= comparison data")
    output = get_table_data('test_dim_design.parquet')
    print(output, "= data extracted from bucket")
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

def test_build_load_sql():
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
    output = build_load_sql('dim_design', df_data)
    expect = "INSERT INTO dim_design (design_id, design_name, file_location, file_name, example) VALUES (%s,%s,%s,%s,%s)"
    assert  output == expect

def test_build_load_sql_with_different_amount_of_columns():
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
    output = build_load_sql('dim_design', df_data)
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
    output = build_load_sql('dim_design', df_data)
    expect = "INSERT INTO dim_design (design_id, design_name, file_location) VALUES (%s,%s,%s)"
    assert  output == expect

# def test_insert_table_data():
#     assert 0