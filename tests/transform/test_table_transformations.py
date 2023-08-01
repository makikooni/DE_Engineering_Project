import logging
import boto3
from src.utils.table_transformations import transform_counterparty, transform_currency, transform_design, transform_location, transform_payment, transform_payment_type, transform_purchase_order, transform_sales_order, transform_staff, transform_transaction, create_date
from moto import mock_s3
from pprint import pprint
from src.transform import transformation_lambda_handler
import pytest
from src.utils.utils import read_csv_to_pandas
import pandas as pd


@pytest.fixture
def create_s3_client():
    with mock_s3():
        yield boto3.client('s3')

@pytest.fixture()
def mock_client(create_s3_client):
        '''
        fixture creates 'test-ingestion-va-052023' bucket in 
        mock aws account and uploads 'test.txt' file to it
        '''
        mock_client = create_s3_client
        ingestion_bucket_name = 'mock-test-ingestion-va-052023'
        processed_bucket_name = 'mock-test-processed-va-052023'
        mock_client.create_bucket(
             Bucket=ingestion_bucket_name, 
             CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
             )
        mock_client.create_bucket(
             Bucket=processed_bucket_name, 
             CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
             )
        table_name = "test_table"
        test_data = [
            ["r1c1", "r1c2", "r1c3"],
            ["r2c1", "r2c2", "r2c3"],
            ["r3c1", "r3c2", "r3c3"],
        ]
        pprint(mock_client.list_buckets())
        test_table_df = pd.DataFrame(data=test_data, columns=["col1", "col2", "col3"])
        test_table_df.to_csv(f"s3://{ingestion_bucket_name}/{table_name}.csv")
        pprint(mock_client.list_objects_v2(Bucket=ingestion_bucket_name))
        # with open('/Users/angushirst/Northcoders/week-11-project/teststuff/test.txt', 'rb') as data:
        #     mock_client.upload_fileobj(data, ingestion_bucket_name, 'test.csv')
        yield mock_client

def test_test(mock_client):
    # pprint(mock_client.list_buckets())
    assert True


# def test_transform_design_retrieves_csv_file_from_ingestion_s3_bucket_and_puts_parquet_file_in_processed_s3_bucket(mock_client):

#     ingestion_bucket_name = 'mock-test-ingestion-va-052023'
#     processed_bucket_name = 'mock-test-processed-va-052023'
    


#     pprint(mock_client.list_buckets())
#     pprint('$-break1------------------------------------------------------------------------------$')
#     pprint(mock_client.list_objects_v2(Bucket=ingestion_bucket_name))
#     pprint('$-break2------------------------------------------------------------------------------$')
#     pprint(mock_client.list_objects_v2(Bucket=processed_bucket_name))
#     key = 'test.csv'
#     dataframe = pd.read_csv(f"s3://mock-test-ingestion-va-052023/{key}")
#     pprint(dataframe)

#     # df = read_csv_to_pandas('test', ingestion_bucket_name)
#     # transform_design('test', ingestion_bucket_name, processed_bucket_name)
#     # pprint(df)

#     pprint('$-break3------------------------------------------------------------------------------$')
#     pprint(mock_client.list_objects_v2(Bucket=processed_bucket_name))

#     assert False


# def test_transform_design_transforms_tables_into_correct_parquet_shchema():

#     pass


# def test_transform_design_puts_parquet_file_into_processed_s3_bucket():

#     pass


# def test_transform_design_raises_exception_when_agruments_invalid():
    
#     pass