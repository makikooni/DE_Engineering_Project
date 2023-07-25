from exreacting_data_test.extract_lambda_function import put_table
# import moto3

def test_testing_function_imported_correctly():
    assert callable(put_table)

def test_returns_error_when_passed_invalid_table():
    try:
        put_table('design')
        # fix this test
        assert False
    except:
        assert True

def test_adds_object_to_s3_bucket():
    pass

def test_():
    pass