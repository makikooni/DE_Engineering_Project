import pandas as pd

def read_csv_to_pandas(file, source_bucket):
    return pd.read_csv(f's3://{source_bucket}/{file}.csv')

def write_df_to_parquet(df, file, target_bucket):
    return df.to_parquet(f's3://{target_bucket}/{file}.parquet')