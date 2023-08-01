from src.extract import extraction_lambda_handler
import pytest
import json


@pytest.fixture
def invalid_schedule_event():
    with open("tests/extract/invalid_test_schedule_event.json") as v:
        event = json.loads(v.read())
    return event


@pytest.fixture
def invalid_test_event():
    with open("tests/extract/invalid_test_event.json") as v:
        event = json.loads(v.read())
    return event


def test_should_raise_error_on_invalid_cloudwatch_event(invalid_schedule_event):
    with pytest.raises(ValueError):
        extraction_lambda_handler(invalid_schedule_event, {})


def test_should_raise_error_on_invalid_event(invalid_test_event):
    with pytest.raises(KeyError):
        extraction_lambda_handler(invalid_test_event, {})
