from moto import mock_s3
from moto.core import patch_client
import pytest
import boto3
import json
from unittest.mock import patch
from botocore.exceptions import ClientError
from src.transform import transform_lambda_handler


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


def test_ensures_internal_methods_are_each_called_once(valid_event):
    with patch("src.transform.transform_design") as mock_transform_design, patch(
        "src.transform.transform_payment_type"
    ) as mock_transform_payment_type, patch(
        "src.transform.transform_location"
    ) as mock_transform_location, patch(
        "src.transform.transform_transaction"
    ) as mock_transform_transaction, patch(
        "src.transform.transform_staff"
    ) as mock_transform_staff, patch(
        "src.transform.transform_currency"
    ) as mock_transform_currency, patch(
        "src.transform.transform_counterparty"
    ) as mock_transform_counterparty, patch(
        "src.transform.transform_sales_order"
    ) as mock_transform_sales_order, patch(
        "src.transform.transform_purchase_order"
    ) as mock_transform_purchase_order, patch(
        "src.transform.transform_payment"
    ) as mock_transform_payment, patch(
        "src.transform.create_date"
    ) as mock_create_date:
        transform_lambda_handler(valid_event, {})
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


def test_function_does_not_execute_if_event_key_is_not_extraction_complete_file(
    invalid_event,
):
    """
    This test demonstrates the transformation_lambda_handler function only
    runs when the event key given is the extraction complete file
    """
    with patch("src.transform.transform_design") as mock_transform_design, patch(
        "src.transform.transform_payment_type"
    ) as mock_transform_payment_type, patch(
        "src.transform.transform_location"
    ) as mock_transform_location, patch(
        "src.transform.transform_transaction"
    ) as mock_transform_transaction, patch(
        "src.transform.transform_staff"
    ) as mock_transform_staff, patch(
        "src.transform.transform_currency"
    ) as mock_transform_currency, patch(
        "src.transform.transform_counterparty"
    ) as mock_transform_counterparty, patch(
        "src.transform.transform_sales_order"
    ) as mock_transform_sales_order, patch(
        "src.transform.transform_purchase_order"
    ) as mock_transform_purchase_order, patch(
        "src.transform.transform_payment"
    ) as mock_transform_payment, patch(
        "src.transform.create_date"
    ) as mock_create_date:
        with pytest.raises(ValueError):
            transform_lambda_handler(invalid_event, {})


def test_raises_excpetion_when_bucket_does_not_exist(invalid_event):
    """
    Demonstrates the function raises an exception when passed an invalid bucket name
    """
    invalid_event["Records"][0]["s3"]["object"]["key"] = "ExtractHistory/1.txt"
    with pytest.raises(ValueError):
        transform_lambda_handler(invalid_event, {})
