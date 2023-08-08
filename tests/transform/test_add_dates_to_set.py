import pandas as pd
import pytest
from utils.utils import add_to_dates_set


def test_function_correctly_adds_unique_dates_to_set():
    """
    This test ensures the function collects only unique dates within the supplied set
    """
     
    test_set = set()
    df1 = pd.DataFrame({'created_date': ['1', '2', '3']})
    df2 = pd.DataFrame({'created_at': ['2', '3', '4', '5']})

    col_list = [df1['created_date'], df2['created_at']]
    add_to_dates_set(test_set, col_list)

    assert test_set == {'1', '2', '3', '4', '5'}


def test_function_raises_exception_when_given_invalid_arguments():

    """
    This test ensures the function raises an exception when passed invalid arguments
    """
    
    col_list = []

    with pytest.raises(Exception):
        add_to_dates_set(a, col_list)

    with pytest.raises(Exception):
        add_to_dates_set({1, 2, 3}, False)