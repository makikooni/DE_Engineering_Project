import pytest
from moto import mock_s3
import boto3
import pg8000
import awswrangler as wr
import pandas as pd
from pandas.testing import assert_frame_equal
from src.load.load import update_wh
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
    wr.s3.upload(local_file = './tests/load/lastjob.txt', path=f's3://{processed_bucket_name}/lastjobdir/lastjob.txt')
    yield mock_client

def test_add_new_rows_puts_data_in_wh_with_0_rows(mock_client):
    test_db = MockDB
    test_db.set_up_database()
    test_db.set_up_tables()
    connection = pg8000.connect(
            host='localhost',
            user='lucy',
            port=5432,
            database='test_db_load',
            password='QASW"1qa'
        )
    update_wh('test_dim_design.parquet', 'dim_design_t1', True)
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM dim_design_t1")
    output = cursor.fetchall()
    expect =  ( ['7','Wooden', '/usr', 'wooden-20220717-npgz.json'], \
    ['8', 'Wood', '/usr','wooden-20220717-npgz.json'])
    assert output == expect
