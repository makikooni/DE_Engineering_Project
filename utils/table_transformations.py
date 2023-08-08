from utils.utils import read_csv_to_pandas, write_df_to_parquet, timestamp_to_date_and_time, add_to_dates_set
import pandas as pd
import numpy as np
import logging


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def transform_design(file, source_bucket, target_bucket):
    """
    This function transforms data read from a CSV file, located in the source S3 bucket, by extracting specific columns
    before writing the resulting data to a Parquet file named 'dim_design.parquet' stored in the target S3 bucket.
    Once the parquet file has been stored in the target s3 bucket, this progress is logged.

    Parameters:
        file (str): The name of the CSV file containing data to be transformed (without the '.csv' extension).

        source_bucket (str): The name of the source S3 bucket where the CSV file is located.

        target_bucket (str): The name of the target S3 bucket to store the resulting Parquet file.

    Returns:
        None.

    Raises:
        Exception: If any error occurs during the transformation process, an exception is raised, and an error message is logged.

    Example:
        transform_design('example-file-name', 'source-bucket-name', 'target-bucket-name')

    Note:
        This function relies on, and utilises, the read_csv_to_pandas() utility function which returns a pandas dataframe read from a csv file.

        This function also relies on, and utilises, the write_df_to_parquet() utility function which writes a parquet file, read from a pandas
        dataframe, and stores it in the s3 target bucket.
    """
    try:
        design_table = read_csv_to_pandas(file, source_bucket)
        dim_design_table = design_table.loc[:, ['design_id', 'design_name', 'file_location', 'file_name']]
        write_df_to_parquet(dim_design_table, 'dim_design', target_bucket)
        logger.info(f'dim_design.parquet successfully created in {target_bucket}')
    except Exception as e:
        logger.error('ERROR: transform_design')
        raise e


def transform_payment_type(file, source_bucket, target_bucket):
    """
    This function transforms data read from a CSV file, located in the source S3 bucket, by extracting specific columns
    before writing the resulting data to a Parquet file named 'dim_payment_type.parquet' stored in the target S3 bucket.
    Once the parquet file has been stored in the target s3 bucket, this progress is logged.

    Parameters:
        file (str): The name of the CSV file containing data to be transformed (without the '.csv' extension).

        source_bucket (str): The name of the source S3 bucket where the CSV file is located.

        target_bucket (str): The name of the target S3 bucket to store the resulting Parquet file.

    Returns:
        None.

    Raises:
        Exception: If any error occurs during the transformation process, an exception is raised, and an error message is logged.

    Example:
        transform_payment_type('example-file-name', 'source-bucket-name', 'target-bucket-name')

    Note:
        This function relies on, and utilises, the read_csv_to_pandas() utility function which returns a pandas dataframe read from a csv file.

        This function also relies on, and utilises, the write_df_to_parquet() utility function which writes a parquet file, read from a pandas
        dataframe, and stores it in the s3 target bucket.
    """  
    try:
        payment_type_table = read_csv_to_pandas(file, source_bucket)
        dim_payment_type_table = payment_type_table.loc[:, ['payment_type_id', 'payment_type_name']]
        write_df_to_parquet(dim_payment_type_table, 'dim_payment_type', target_bucket)
        logger.info(f'dim_payment_type.parquet successfully created in {target_bucket}')
    except Exception as e:
        logger.error('ERROR: transform_payment_type')
        raise e


def transform_location(file, source_bucket, target_bucket):
    """
    This function transforms data read from a CSV file, located in the source S3 bucket, by extracting 
    specific columns and renaming the 'address_id' column to 'location_id' before writing the 
    resulting data to a Parquet file named 'dim_location.parquet' stored in the target S3 bucket. Once 
    the parquet file has been stored in the target s3 bucket, this progress is logged.

    Parameters:
        file (str): The name of the CSV file containing data to be transformed (without the '.csv' extension).

        source_bucket (str): The name of the source S3 bucket where the CSV file is located.

        target_bucket (str): The name of the target S3 bucket to store the resulting Parquet file.

    Returns:
        None.

    Raises:
        Exception: If any error occurs during the transformation process, an exception is raised, and an error message is logged.

    Example:
        transform_location('example-file-name', 'source-bucket-name', 'target-bucket-name')

    Note:
        This function relies on, and utilises, the read_csv_to_pandas() utility function which returns a pandas dataframe read from a csv file.

        This function also relies on, and utilises, the write_df_to_parquet() utility function which writes a parquet file, read from a pandas
        dataframe, and stores it in the s3 target bucket.
    """ 
    try:
        address_table = read_csv_to_pandas(file, source_bucket)
        dim_address_table = address_table.loc[:, ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]
        dim_address_table.rename(columns={'address_id': 'location_id'}, inplace=True)
        write_df_to_parquet(dim_address_table, 'dim_location', target_bucket)
        logger.info(f'dim_location.parquet successfully created in {target_bucket}')
    except Exception as e:
        logger.error('ERROR: transform_location')
        raise e


def transform_transaction(file, source_bucket, target_bucket):
    """
    This function transforms data read from a CSV file, located in the source S3 bucket, by extracting specific columns
    before writing the resulting data to a Parquet file named 'dim_transaction.parquet' stored in the target S3 bucket.
    Once the parquet file has been stored in the target s3 bucket, this progress is logged.

    Parameters:
        file (str): The name of the CSV file containing data to be transformed (without the '.csv' extension).

        source_bucket (str): The name of the source S3 bucket where the CSV file is located.

        target_bucket (str): The name of the target S3 bucket to store the resulting Parquet file.

    Returns:
        None.

    Raises:
        Exception: If any error occurs during the transformation process, an exception is raised, and an error message is logged.

    Example:
        transform_transaction('example-file-name', 'source-bucket-name', 'target-bucket-name')

    Note:
        This function relies on, and utilises, the read_csv_to_pandas() utility function which returns a pandas dataframe read from a csv file.

        This function also relies on, and utilises, the write_df_to_parquet() utility function which writes a parquet file, read from a pandas
        dataframe, and stores it in the s3 target bucket.
    """ 
    try:
        transaction_table = read_csv_to_pandas(file, source_bucket)
        dim_transaction_table = transaction_table.loc[:, ['transaction_id', 'transaction_type', 'sales_order_id', 'purchase_order_id']]
        write_df_to_parquet(dim_transaction_table, 'dim_transaction', target_bucket)
        logger.info(f'dim_transaction.parquet successfully created in {target_bucket}')
    except Exception as e:
        logger.error('ERROR: transform_transaction')
        raise e


def transform_staff(file1, file2, source_bucket, target_bucket):
    """
    This function reads data from two CSV files, located in the source s3 bucket, before joining the two tables on the 
    'department_id' column. Specific columns are extracted, from the joined tables, and the resulting data is written 
    to a Parquet file named 'dim_staff.parquet' stored in the target s3 bucket. Once the parquet file has been stored 
    in the target s3 bucket, this progress is logged.

    Parameters:
        file1 (str): The name of one CSV file containing data to be transformed (without the '.csv' extension).

        file2 (str): The name of the other CSV file containing data to be transformed (without the '.csv' extension).

        source_bucket (str): The name of the source S3 bucket where the CSV file is located.

        target_bucket (str): The name of the target S3 bucket to store the resulting Parquet file.

    Returns:
        None.

    Raises:
        Exception: If any error occurs during the transformation process, an exception is raised, and an error message is logged.

    Example:
        transform_staff('example-file-1-name', 'example-file-2-name', 'source-bucket-name', 'target-bucket-name')

    Note:
        This function relies on, and utilises, the read_csv_to_pandas() utility function which returns a pandas dataframe read from a csv file.

        This function also relies on, and utilises, the write_df_to_parquet() utility function which writes a parquet file, read from a pandas
        dataframe, and stores it in the s3 target bucket.
    """ 
    try:
        staff_table = read_csv_to_pandas(file1, source_bucket)
        department_table = read_csv_to_pandas(file2, source_bucket)
        joined_staff_department_table = staff_table.join(department_table.set_index('department_id'), on='department_id', lsuffix="staff", rsuffix='department')
        dim_staff_table = joined_staff_department_table.loc[:, ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']]
        write_df_to_parquet(dim_staff_table, 'dim_staff', target_bucket)
        logger.info(f'dim_staff.parquet successfully created in {target_bucket}')
    except Exception as e:
        logger.error('ERROR: transform_staff')
        raise e


def transform_currency(file, source_bucket, target_bucket):
    """
    This function reads a currency table from a CSV data file, located in the source s3 bucket. Specific columns 
    are then extracted before the data is enriched with currency names based on their currency codes, the 
    resulting data is then written to a Parquet file named 'dim_currency.parquet' stored in the target s3 bucket.
    Once the parquet file has been stored in the target s3 bucket, this progress is logged.

    Parameters:
        file (str): The name of the CSV file containing data to be transformed (without the '.csv' extension).

        source_bucket (str): The name of the source S3 bucket where the CSV file is located.

        target_bucket (str): The name of the target S3 bucket to store the resulting Parquet file.

    Returns:
        None.

    Raises:
        Exception: If any error occurs during the transformation process, an exception is raised, and an error message is logged.

    Example:
        transform_currency('example-file-name', 'source-bucket-name', 'target-bucket-name')

    Note:
        This function relies on, and utilises, the read_csv_to_pandas() utility function which returns a pandas dataframe read from a csv file.

        This function also relies on, and utilises, the write_df_to_parquet() utility function which writes a parquet file, read from a pandas
        dataframe, and stores it in the s3 target bucket.
    """
    try:
        currency_table = read_csv_to_pandas(file, source_bucket)
        dim_currency_table = currency_table.loc[:, ['currency_id', 'currency_code']]
        conditions = [(dim_currency_table['currency_code'] == 'EUR'), (dim_currency_table['currency_code'] == 'GBP'), (dim_currency_table['currency_code'] == 'USD')]
        values = ['Euro', 'British Pound', 'US Dollar']
        dim_currency_table['currency_name'] = np.select(conditions, values)
        write_df_to_parquet(dim_currency_table, 'dim_currency', target_bucket)
        logger.info(f'dim_currency.parquet successfully created in {target_bucket}')
    except Exception as e:
        logger.error('ERROR: transform_currency')
        raise e


def transform_counterparty(file1, file2, source_bucket, target_bucket):
    """
    This function reads data from two CSV files, located in the source s3 bucket, before joining the two tables on 
    the 'legal_address_id' column, derived from file1, and the 'address_id column, derived from file2. Specific 
    columns are extracted, from the joined tables, and the resulting data is written to a Parquet file named 
    'dim_counterparty.parquet' stored in the target s3 bucket. Once the parquet file has been stored in the 
    target s3 bucket, this progress is logged.

    Parameters:
        file1 (str): The name of one CSV file containing data to be transformed (without the '.csv' extension).

        file2 (str): The name of the other CSV file containing data to be transformed (without the '.csv' extension).

        source_bucket (str): The name of the source S3 bucket where the CSV file is located.

        target_bucket (str): The name of the target S3 bucket to store the resulting Parquet file.

    Returns:
        None.

    Raises:
        Exception: If any error occurs during the transformation process, an exception is raised, and an error message is logged.

    Example:
        transform_counterparty('example-file-1-name', 'example-file-2-name', 'source-bucket-name', 'target-bucket-name')

    Note:
        This function relies on, and utilises, the read_csv_to_pandas() utility function which returns a pandas dataframe read from a csv file.

        This function also relies on, and utilises, the write_df_to_parquet() utility function which writes a parquet file, read from a pandas
        dataframe, and stores it in the s3 target bucket.
    """
    try:
        counterparty_table = read_csv_to_pandas(file1, source_bucket)
        address_table_for_counterparty = read_csv_to_pandas(file2, source_bucket)
        joined_counterparty_address_table = counterparty_table.join(address_table_for_counterparty.set_index('address_id'), on='legal_address_id', lsuffix='counterparty', rsuffix='address')
        dim_counterparty = joined_counterparty_address_table.loc[:, ['counterparty_id', 'counterparty_legal_name', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]
        columns_to_rename = ['address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country']
        dim_counterparty.rename(columns={col: 'counterparty_legal_'+col for col in dim_counterparty.columns if col in columns_to_rename}, inplace=True)
        dim_counterparty.rename(columns={'phone': 'counterparty_legal_phone_number'}, inplace=True)
        write_df_to_parquet(dim_counterparty, 'dim_counterparty', target_bucket)
        logger.info(f'dim_counterparty.parquet successfully created in {target_bucket}')
    except Exception as e:
        logger.error('ERROR: transform_counterparty')
        raise e


def transform_sales_order(file, source_bucket, target_bucket, dates_for_dim_date):
    """
    This function transforms data read from a CSV file, located in the source S3 bucket, by splitting and extracting specific 
    columns before writing the resulting data to a Parquet file named 'fact_sales_order.parquet' stored in the target S3 bucket.
    Once the parquet file has been stored in the target s3 bucket, this progress is logged, after which the dates data are added 
    to the dim_for_dim_dates set.

    Parameters:
        file (str): The name of the CSV file containing data to be transformed (without the '.csv' extension).

        source_bucket (str): The name of the source S3 bucket where the CSV file is located.

        target_bucket (str): The name of the target S3 bucket to store the resulting Parquet file.

        dates_for_dim_date (set): A set of dates to be used in the create_date() function.

    Returns:
        None.

    Raises:
        Exception: If any error occurs during the transformation process, an exception is raised, and an error message is logged.

    Example:
        transform_sales_order('example-file-name', 'source-bucket-name', 'target-bucket-name', 'dates-set-name')

    Note:
        This function relies on, and utilises, the read_csv_to_pandas() utility function which returns a pandas dataframe read from a csv file.

        This function also relies on, and utilises, the write_df_to_parquet() utility function which writes a parquet file, read from a pandas
        dataframe, and stores it in the s3 target bucket.

        This function also relies on, and utilises, the timestamp_to_date_and_time() utility function which splits the data from the 'created_at' 
        column into new columns of 'created_date' and 'created_time' whilst also splitting the data from the 'last_updated' column into new 
        columns of 'last_updated_date' and 'last_updated_time'.
    """
    try:
        sales_order_table = read_csv_to_pandas(file, source_bucket)
        
        sales_order_table = timestamp_to_date_and_time(sales_order_table)

        sales_order_table.rename(columns={'staff_id': 'sales_staff_id'}, inplace=True)
        fact_sales_order = sales_order_table.loc[:, ['sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']]
        write_df_to_parquet(fact_sales_order, 'fact_sales_order', target_bucket)
        logger.info(f'fact_sales_order.parquet successfully created in {target_bucket}')

        date_cols_to_add = [fact_sales_order['created_date'], fact_sales_order['last_updated_date'],fact_sales_order['agreed_payment_date'], fact_sales_order['agreed_delivery_date']]
        add_to_dates_set(dates_for_dim_date, date_cols_to_add)
    except Exception as e:
        logger.error('ERROR: transform_sales_order')
        raise e


def transform_purchase_order(file, source_bucket, target_bucket, dates_for_dim_date):
    """
    This function transforms data read from a CSV file, located in the source S3 bucket, by splitting and extracting specific 
    columns before writing the resulting data to a Parquet file named 'fact_purchase_order.parquet' stored in the target S3 bucket.
    Once the parquet file has been stored in the target s3 bucket, this progress is logged, after which the dates data are added 
    to the dim_for_dim_dates set.

    Parameters:
        file (str): The name of the CSV file containing data to be transformed (without the '.csv' extension).

        source_bucket (str): The name of the source S3 bucket where the CSV file is located.

        target_bucket (str): The name of the target S3 bucket to store the resulting Parquet file.

        dates_for_dim_date (set): A set of dates to be used in the create_date() function.

    Returns:
        None.

    Raises:
        Exception: If any error occurs during the transformation process, an exception is raised, and an error message is logged.

    Example:
        transform_purchase_order('example-file-name', 'source-bucket-name', 'target-bucket-name', 'dates-set-name')

    Note:
        This function relies on, and utilises, the read_csv_to_pandas() utility function which returns a pandas dataframe read from a csv file.

        This function also relies on, and utilises, the write_df_to_parquet() utility function which writes a parquet file, read from a pandas
        dataframe, and stores it in the s3 target bucket.

        This function also relies on, and utilises, the timestamp_to_date_and_time() utility function which splits the data from the 'created_at' 
        column into new columns of 'created_date' and 'created_time' whilst also splitting the data from the 'last_updated' column into new 
        columns of 'last_updated_date' and 'last_updated_time'.
    """
    try:
        purchase_order_table = read_csv_to_pandas(file, source_bucket)

        purchase_order_table = timestamp_to_date_and_time(purchase_order_table)

        fact_purchase_order = purchase_order_table.loc[:, ['purchase_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'staff_id', 'counterparty_id', 'item_code', 'item_quantity', 'item_unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']]
        write_df_to_parquet(fact_purchase_order, 'fact_purchase_order', target_bucket)
        logger.info(f'fact_purchase_order.parquet successfully created in {target_bucket}')

        date_cols_to_add = [fact_purchase_order['created_date'], fact_purchase_order['last_updated_date'], fact_purchase_order['agreed_delivery_date'], fact_purchase_order['agreed_payment_date']]
        add_to_dates_set(dates_for_dim_date, date_cols_to_add)
    except Exception as e:
        logger.error('ERROR: transform_purchase_order')
        raise e


def transform_payment(file, source_bucket, target_bucket, dates_for_dim_date):
    """
    This function transforms data read from a CSV file, located in the source S3 bucket, by splitting and extracting specific 
    columns before writing the resulting data to a Parquet file named 'fact_payment.parquet' stored in the target S3 bucket.
    Once the parquet file has been stored in the target s3 bucket, this progress is logged, after which the dates data are added 
    to the dim_for_dim_dates set.

    Parameters:
        file (str): The name of the CSV file containing data to be transformed (without the '.csv' extension).

        source_bucket (str): The name of the source S3 bucket where the CSV file is located.

        target_bucket (str): The name of the target S3 bucket to store the resulting Parquet file.

        dates_for_dim_date (set): A set of dates to be used in the create_date() function.

    Returns:
        None.

    Raises:
        Exception: If any error occurs during the transformation process, an exception is raised, and an error message is logged.

    Example:
        transform_payment('example-file-name', 'source-bucket-name', 'target-bucket-name', 'dates-set-name')

    Note:
        This function relies on, and utilises, the read_csv_to_pandas() utility function which returns a pandas dataframe read from a csv file.

        This function also relies on, and utilises, the write_df_to_parquet() utility function which writes a parquet file, read from a pandas
        dataframe, and stores it in the s3 target bucket.

        This function also relies on, and utilises, the timestamp_to_date_and_time() utility function which splits the data from the 'created_at' 
        column into new columns of 'created_date' and 'created_time' whilst also splitting the data from the 'last_updated' column into new 
        columns of 'last_updated_date' and 'last_updated_time'.
    """
    try:
        payment_table = read_csv_to_pandas(file, source_bucket)

        payment_table = timestamp_to_date_and_time(payment_table)

        fact_payment_table = payment_table.loc[:, ['payment_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'transaction_id', 'counterparty_id', 'payment_amount', 'currency_id', 'payment_type_id', 'paid', 'payment_date']]
        write_df_to_parquet(fact_payment_table, 'fact_payment', target_bucket)
        logger.info(f'fact_payment.parquet successfully created in {target_bucket}')

        date_cols_to_add = [fact_payment_table['created_date'], fact_payment_table['last_updated_date'], fact_payment_table['payment_date']]
        add_to_dates_set(dates_for_dim_date, date_cols_to_add)
    except Exception as e:
        logger.error('ERROR: transform_payment')
        raise e


def create_date(dates_for_dim_date, target_bucket):
    """
    This function creates a dimension date table, from the dates_for_dim_date set of date values, by converting the data into a pandas
    dataframe before extracting various date-related attributes into new columns. The resulting data is then written to a parquet file
    named 'dim_date.parquet' stored in the target s3 bucket, this progress is then logged.

    Args:
        dates_for_dim_date (set): A set containing date values to populate the dimension date table.
        
        target_bucket (str): The target bucket or directory where the output data will be stored.

    Returns:
        None.

    Raises:
        Exception: If any error occurs during the creation process, an exception is raised, and an error message is logged.
    
    Example:
        create_date('dates-set-name', 'target-bucket-name')

    Note:
        This function relies on, and utilises, the write_df_to_parquet() utility function which writes a parquet file, read from a pandas
        dataframe, and stores it in the s3 target bucket.
    """
    try:
        dates = {'date_id': sorted(list(dates_for_dim_date))}
        dim_date = pd.DataFrame(data=dates)
        dim_date['year'] = pd.DatetimeIndex(dim_date['date_id']).year
        dim_date['month'] = pd.DatetimeIndex(dim_date['date_id']).month
        dim_date['day'] = pd.DatetimeIndex(dim_date['date_id']).day
        dim_date['day_of_week'] = pd.DatetimeIndex(dim_date['date_id']).dayofweek
        dim_date['day_name'] = pd.DatetimeIndex(dim_date['date_id']).day_name()
        dim_date['month_name'] = pd.DatetimeIndex(dim_date['date_id']).month_name()
        dim_date['quarter'] = pd.DatetimeIndex(dim_date['date_id']).quarter
        write_df_to_parquet(dim_date, 'dim_date', target_bucket)
        logger.info(f'dim_date.parquet successfully created in {target_bucket}')
    except Exception as e:
        logger.error('ERROR: create_date')
        raise e