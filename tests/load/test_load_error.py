import pytest
from moto import mock_s3
import boto3
import awswrangler as wr
import pandas as pd
from pandas.testing import assert_frame_equal
from utils.load_utils import get_table_data, insert_data_format, build_insert_sql, insert_table_data, build_update_sql, update_data_format

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

def test_get_table_data_error(mock_client):
    with pytest.raises(Exception):
        get_table_data('test_dim_desig.parquet')
    
def test_dataframe_to_list():
    not_data = []
    with pytest.raises(Exception):
        insert_data_format(not_data)

def test_build_insert_sql():
    not_data = []
    not_table = ''
    with pytest.raises(Exception):
        build_insert_sql(not_table, not_data)

def test_insert_table_data():
    not_connection = []
    not_data = []
    not_table = ''
    with pytest.raises(Exception):
        insert_table_data(not_connection,not_data, not_table)

def test_build_update_sql():
    wh_table_name = []
    table = []
    with pytest.raises(Exception):
        build_update_sql(wh_table_name, table)

def test_update_data_format():
    table = []
    with pytest.raises(Exception):
        update_data_format(table)