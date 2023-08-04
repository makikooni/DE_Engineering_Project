from moto import mock_s3
from moto.core import patch_client
import pytest
import boto3
import json
from unittest.mock import patch
from botocore.exceptions import ClientError
from pprint import pprint
from src.transform import transformation_lambda_handler


@pytest.fixture
def invalid_event():
    with open("tests/transform/test_invalid_event.json") as v:
        event = json.loads(v.read())
    return event


@pytest.fixture
def valid_event():
    with open("tests/transform/test_valid_event.json") as v:
        event = json.loads(v.read())
    return event


# @pytest.fixture
# def create_s3_client():
#     with mock_s3():
#         yield boto3.client('s3', region_name='eu-west-2')


# @pytest.fixture
# def mock_client(create_s3_client):
#     '''
#     fixture creates 'test-ingestion-va-052023' bucket in 
#     mock aws account and uploads 'test.txt' file to it
#     '''
#     mock_client = create_s3_client
#     ingestion_bucket_name = 'mock-test-ingestion-va-052023'
#     processed_bucket_name = 'mock-test-processed-va-052023'
#     mock_client.create_bucket(
#         Bucket=ingestion_bucket_name, 
#         CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
#         )
#     mock_client.create_bucket(
#         Bucket=processed_bucket_name, 
#         CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
#         )
#     with open('tests/transform/test_data_csv_files/test_sales_order.csv', 'rb') as data:
#         mock_client.upload_fileobj(data, ingestion_bucket_name, 'test.csv')
#     yield mock_client


def test_function_connects_to_ingestion_s3_bucket(caplog, valid_event):
    with patch('src.transform.transform_design') as mock_transform_design,\
        patch('src.transform.transform_payment_type') as mock_transform_payment_type,\
        patch('src.transform.transform_location') as mock_transform_location,\
        patch('src.transform.transform_transaction') as mock_transform_transaction,\
        patch('src.transform.transform_staff') as mock_transform_staff,\
        patch('src.transform.transform_currency') as mock_transform_currency,\
        patch('src.transform.transform_counterparty') as mock_transform_counterparty,\
        patch('src.transform.transform_sales_order') as mock_transform_sales_order,\
        patch('src.transform.transform_purchase_order') as mock_transform_purchase_order,\
        patch('src.transform.transform_payment') as mock_transform_payment,\
        patch('src.transform.create_date') as mock_create_date:
        transformation_lambda_handler(valid_event)
        assert "Connection to test-va-ingestion-atif confirmed" in caplog.text


def test_ensures_internal_methods_are_each_called_once(valid_event):
    with patch('src.transform.transform_design') as mock_transform_design,\
        patch('src.transform.transform_payment_type') as mock_transform_payment_type,\
        patch('src.transform.transform_location') as mock_transform_location,\
        patch('src.transform.transform_transaction') as mock_transform_transaction,\
        patch('src.transform.transform_staff') as mock_transform_staff,\
        patch('src.transform.transform_currency') as mock_transform_currency,\
        patch('src.transform.transform_counterparty') as mock_transform_counterparty,\
        patch('src.transform.transform_sales_order') as mock_transform_sales_order,\
        patch('src.transform.transform_purchase_order') as mock_transform_purchase_order,\
        patch('src.transform.transform_payment') as mock_transform_payment,\
        patch('src.transform.create_date') as mock_create_date:
        transformation_lambda_handler(valid_event)
        assert mock_transform_design.call_count == 1
        assert mock_transform_payment_type.call_count == 1
        assert mock_transform_location.call_count == 1
        assert mock_transform_transaction.call_count == 1
        assert mock_transform_staff.call_count == 1
        assert mock_transform_currency.call_count == 1
        assert mock_transform_counterparty.call_count == 1
        assert mock_transform_sales_order.call_count == 1
        assert mock_transform_purchase_order.call_count == 1
        assert mock_transform_payment.call_count == 1
        assert mock_create_date.call_count == 1
         

def test_function_does_not_execute_if_event_key_is_not_extraction_complete_file(invalid_event):
    '''
    This test demonstrates the transformation_lambda_handler function only
    runs when the event key given is the extraction complete file
    '''
    with patch('src.transform.transform_design') as mock_transform_design,\
        patch('src.transform.transform_payment_type') as mock_transform_payment_type,\
        patch('src.transform.transform_location') as mock_transform_location,\
        patch('src.transform.transform_transaction') as mock_transform_transaction,\
        patch('src.transform.transform_staff') as mock_transform_staff,\
        patch('src.transform.transform_currency') as mock_transform_currency,\
        patch('src.transform.transform_counterparty') as mock_transform_counterparty,\
        patch('src.transform.transform_sales_order') as mock_transform_sales_order,\
        patch('src.transform.transform_purchase_order') as mock_transform_purchase_order,\
        patch('src.transform.transform_payment') as mock_transform_payment,\
        patch('src.transform.create_date') as mock_create_date:
        transformation_lambda_handler(invalid_event)
        assert mock_transform_design.call_count == 0
        assert mock_transform_payment_type.call_count == 0
        assert mock_transform_location.call_count == 0
        assert mock_transform_transaction.call_count == 0
        assert mock_transform_staff.call_count == 0
        assert mock_transform_currency.call_count == 0
        assert mock_transform_counterparty.call_count == 0
        assert mock_transform_sales_order.call_count == 0
        assert mock_transform_purchase_order.call_count == 0
        assert mock_transform_payment.call_count == 0
        assert mock_create_date.call_count == 0


def test_raises_excpetion_when_bucket_does_not_exist(invalid_event):
    '''
    Demonstrates the function raises an exception when passed an invalid bucket name
    '''
    invalid_event['Records'][0]['s3']['object']['key'] = 'extraction_complete.txt'
    with pytest.raises(ClientError):
        transformation_lambda_handler(invalid_event)