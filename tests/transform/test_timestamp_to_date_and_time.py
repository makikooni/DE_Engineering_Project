from moto import mock_s3
from pandas.testing import assert_frame_equal
import pandas as pd
import pytest
from pprint import pprint
from src.utils.utils import timestamp_to_date_and_time


def test_function_correctly_splits_columns_and_data():
     
    df = pd.read_csv('tests/transform/test_data_csv_files/test_timestamp_to_date_and_time.csv')
    result_df = timestamp_to_date_and_time(df)
    expected_df = pd.DataFrame({'created_date': ['1a', '3a'], 'created_time': ['1b', '3b'], 'last_updated_date': ['2a', '4a'], 'last_updated_time': ['2b', '4b']})
    
    assert_frame_equal(result_df, expected_df)


def test_function_raises_exception_when_agruments_invalid():

    with pytest.raises(Exception):
        timestamp_to_date_and_time('wrong')
