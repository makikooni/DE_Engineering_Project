import pandas as pd

def read_csv_to_pandas(file, source_bucket):
    return pd.read_csv(f's3://{source_bucket}/{file}.csv')

def write_df_to_parquet(df, file, target_bucket):
    return df.to_parquet(f's3://{target_bucket}/{file}.parquet')

def timestamp_to_date_and_time(dataframe):
    new_created = dataframe['created_at'].str.split(" ", n = 1, expand = True)
    dataframe['created_date']= new_created[0]
    dataframe['created_time']= new_created[1]
    dataframe.drop(columns =['created_at'], inplace = True)
    new_updated = dataframe['last_updated'].str.split(" ", n = 1, expand = True)
    dataframe['last_updated_date']= new_updated[0]
    dataframe['last_updated_time']= new_updated[1]
    dataframe.drop(columns =['last_updated'], inplace = True)
    return dataframe

def add_to_dates_set(set, cols_to_add):
    for col in cols_to_add:
        for row in col:
            set.add(row)

