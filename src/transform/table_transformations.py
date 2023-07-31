from src.utils.utils import read_csv_to_pandas, write_df_to_parquet, timestamp_to_date_and_time, add_to_dates_set
import pandas as pd
import numpy as np


def transform_design(file, source_bucket, target_bucket):
    design_table = read_csv_to_pandas(file, source_bucket)
    dim_design_table = design_table[['design_id', 'design_name', 'file_location', 'file_name']]
    write_df_to_parquet(dim_design_table, 'dim_design', target_bucket)


def transform_payment_type(file, source_bucket, target_bucket):
    payment_type_table = read_csv_to_pandas(file, source_bucket)
    dim_payment_type_table = payment_type_table[['payment_type_id', 'payment_type_name']]
    write_df_to_parquet(dim_payment_type_table, 'dim_payment_type', target_bucket)


def transform_location(file, source_bucket, target_bucket):
    address_table = read_csv_to_pandas(file, source_bucket)
    dim_address_table = address_table[['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]
    dim_address_table.rename(columns={'address_id': 'location_id'}, inplace=True)
    write_df_to_parquet(dim_address_table, 'dim_location', target_bucket)


def transform_transaction(file, source_bucket, target_bucket):
    transaction_table = read_csv_to_pandas(file, source_bucket)
    dim_transaction_table = transaction_table[['transaction_id', 'transaction_type', 'sales_order_id', 'purchase_order_id']]
    write_df_to_parquet(dim_transaction_table, 'dim_transaction', target_bucket)


def transform_staff(file1, file2, source_bucket, target_bucket):
    staff_table = read_csv_to_pandas(file1, source_bucket)
    department_table = read_csv_to_pandas(file2, source_bucket)
    joined_staff_department_table = staff_table.join(department_table.set_index('department_id'), on='department_id', lsuffix="staff", rsuffix='department')
    dim_staff_table = joined_staff_department_table[['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']]
    write_df_to_parquet(dim_staff_table, 'dim_staff', target_bucket)


def transform_currency(file, source_bucket, target_bucket):
    currency_table = read_csv_to_pandas(file, source_bucket)
    dim_currency_table = currency_table[['currency_id', 'currency_code']]
    conditions = [(dim_currency_table['currency_code'] == 'EUR'), (dim_currency_table['currency_code'] == 'GBP'), (dim_currency_table['currency_code'] == 'USD')]
    values = ['Euro', 'British Pound', 'US Dollar']
    dim_currency_table['currency_name'] = np.select(conditions, values)
    write_df_to_parquet(dim_currency_table, 'dim_currency', target_bucket)


def transform_counterparty(file1, file2, source_bucket, target_bucket):
    counterparty_table = read_csv_to_pandas(file1, source_bucket)
    address_table_for_counterparty = read_csv_to_pandas(file2, source_bucket)
    joined_counterparty_address_table = counterparty_table.join(address_table_for_counterparty.set_index('address_id'), on='legal_address_id', lsuffix='counterparty', rsuffix='address')
    dim_counterparty = joined_counterparty_address_table[['counterparty_id', 'counterparty_legal_name', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]
    columns_to_rename = ['address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country']
    dim_counterparty.rename(columns={col: 'counterparty_legal_'+col for col in dim_counterparty.columns if col in columns_to_rename}, inplace=True)
    dim_counterparty.rename(columns={'phone': 'counterparty_legal_phone_number'}, inplace=True)
    write_df_to_parquet(dim_counterparty, 'dim_counterparty', target_bucket)


def transform_sales_order(file, source_bucket, target_bucket, dates_for_dim_date):
    sales_order_table = read_csv_to_pandas(file, source_bucket)
    sales_order_table.columns.values[0] = "sales_record_id"
    
    sales_order_table = timestamp_to_date_and_time(sales_order_table)

    sales_order_table.rename(columns={'staff_id': 'sales_staff_id'}, inplace=True)
    fact_sales_order = sales_order_table[['sales_record_id', 'sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']]
    write_df_to_parquet(fact_sales_order, 'fact_sales_order', target_bucket)

    date_cols_to_add = [fact_sales_order['created_date'], fact_sales_order['last_updated_date'],fact_sales_order['agreed_payment_date'], fact_sales_order['agreed_delivery_date']]
    add_to_dates_set(dates_for_dim_date, date_cols_to_add)


def transform_purchase_order(file, source_bucket, target_bucket, dates_for_dim_date):
    purchase_order_table = read_csv_to_pandas(file, source_bucket)
    purchase_order_table.columns.values[0] = "purchase_record_id"

    purchase_order_table = timestamp_to_date_and_time(purchase_order_table)

    fact_purchase_order = purchase_order_table[['purchase_record_id', 'purchase_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'staff_id', 'counterparty_id', 'item_code', 'item_quantity', 'item_unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']]
    write_df_to_parquet(fact_purchase_order, 'fact_purchase_order', target_bucket)

    date_cols_to_add = [fact_purchase_order['created_date'], fact_purchase_order['last_updated_date'], fact_purchase_order['agreed_delivery_date'], fact_purchase_order['agreed_payment_date']]
    add_to_dates_set(dates_for_dim_date, date_cols_to_add)


def transform_payment(file, source_bucket, target_bucket, dates_for_dim_date):
    payment_table = read_csv_to_pandas(file, source_bucket)
    payment_table.columns.values[0] = 'payment_record_id'

    payment_table = timestamp_to_date_and_time(payment_table)

    # figma states last_updated, assuming this is a typo: find last_updated_time
    fact_payment_table = payment_table[['payment_record_id', 'payment_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'transaction_id', 'counterparty_id', 'payment_amount', 'currency_id', 'payment_type_id', 'paid', 'payment_date']]
    write_df_to_parquet(fact_payment_table, 'fact_payment', target_bucket)

    date_cols_to_add = [fact_payment_table['created_date'], fact_payment_table['last_updated_date'], fact_payment_table['payment_date']]
    add_to_dates_set(dates_for_dim_date, date_cols_to_add)


def create_date(dates_for_dim_date, target_bucket):
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
    # run in terminal to view pq table --> parquet-tools show s3://processed-va-052023/dim_date.parquet