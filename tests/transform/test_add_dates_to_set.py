from moto import mock_s3
from pandas.testing import assert_frame_equal
import pandas as pd
import pytest
from pprint import pprint
from src.utils.utils import add_to_dates_set


def test_function_correctly_appends_unique_dates_to_set():
     
    test_set = set()
    df = pd.read_csv('tests/transform/test_data_csv_files/test_add_dates_to_set.csv')
    add_to_dates_set(test_set, df)
    pprint(test_set)
    
    assert False

