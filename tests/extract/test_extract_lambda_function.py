from src.extract.extract import extraction_lambda_handler
import pytest
import json

@pytest.fixture
def invalid_event():
    with open('tests/invalid_test_event.json') as v:
        event = json.loads(v.read())
    return event

def test_should_not_raise_error_on_valid_event(invalid_event):
    
    with pytest.raises(ValueError):
        extraction_lambda_handler(invalid_event, {})