import awswrangler as wr
from datetime import datetime
import logging
import json
from pg8000.native import Connection, InterfaceError, DatabaseError, identifier, literal
import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger("UtilsLogger")
logger.setLevel(logging.INFO)


def get_secret(secret_name):
    """
    This function retrieves a secret value from AWS Secrets Manager.

    Args:
        secret_name (str): The name of the secret to retrieve.

    Returns:
        dict: A dictionary containing the secret value. The expected data structure follows:
            {'dbname': 'example_dbname',
            'engine': 'example_engine',
            'host': 'example_host',
            'password': 'example_password',
            'port': 'example_port',
            'username': 'example_user'}

    Raises:
        TypeError: If secret_name argument entered is not a string.

        KeyError: If the secret_name secret cannot be found.

        RuntimeError: If access to the SecretsManager resource is denied.
    """
    if not isinstance(secret_name, str):
        raise TypeError(f"secret_name is {type(secret_name)}, {str} is required")

    secretsmanager = boto3.client("secretsmanager")

    try:
        response = secretsmanager.get_secret_value(SecretId=secret_name)
        secret_value = json.loads(response["SecretString"].replace("'", '"'))
        logger.info(f"secret: {secret_name} information successfully retrieved!")
        return secret_value

    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.error(
                f"ResourceNotFoundException: the secret named {secret_name} cannot be found in SecretsManager"
            )
            raise KeyError(
                f"ResourceNotFoundException: the secret named {secret_name} cannot be found in SecretsManager"
            )
        elif e.response["Error"]["Code"] == "AccessDeniedException":
            logger.error(
                f"AccessDeniedException: the lambda does not have an identity-based policy to access SecretsManager resource"
            )
            raise RuntimeError(
                f"AccessDeniedException: the lambda does not have an identity-based policy to access SecretsManager resource"
            )
        else:
            logger.error(e)


def get_table_db(connection, table_name, bucket_name):
    """
    This function retrieves data from a database table through a database connection object.

    Args:
        connection (Connection): A database connection object.

        table_name (str): The name of the table to retrieve data from.

    Returns:
        tuple: A tuple consisting of the data in pandas.DataFrame format and the SQL query string.

    Raises:
        TypeError: If table_name is not a string, or, if connection is not an instance of pg8000.native Connection.

        InterfaceError: If the query cannot be executed.
    """

    if not isinstance(table_name, str):
        raise TypeError(f"table name is {type(table_name)}, expected {str}")
    if not isinstance(connection, Connection):
        raise TypeError(
            f"connection object is {type(connection)}, expected {Connection}"
        )
    if not isinstance(bucket_name, str):
        raise TypeError(f"ingestion bucket name is {type(bucket_name)}, expected {str}")

    try:
        query = query_controller(table_name, bucket_name)

        table_data = connection.run(query)
        column_names = [col["name"] for col in connection.columns]

        table_df = pd.DataFrame(data=table_data, columns=column_names)

        return table_df, query
    except InterfaceError:
        logger.error(f"InterfaceError: the query: \n {query} \n cannot be executed.")
        raise InterfaceError


def upload_table_s3(table_df, table_name, bucket_name, time_stamp):
    """
    This fucntion uploads a pandas.DataFrame to a target S3 bucket as a CSV file.

    Args:
        table_df (pandas.DataFrame): The DataFrame containing the data to be uploaded.

        table_name (str): The defined name of the CSV file to be stored in the target s3 bucket (without .CSV extension).

        bucket_name (str): The name of the target S3 bucket.

    Returns:
        None.

    Raises:
        TypeError: If table_df is not a pandas.DataFrame, or, if either table_name or bucket_name are not strings.

        KeyError: If the specified bucket does not exist.
        
        Exception: If there's an error during the write and upload process.t
    """

    if not isinstance(table_df, pd.DataFrame):
        raise TypeError(f"table dataframe {type(table_df)}, expected {pd.DataFrame}")
    if not isinstance(table_name, str):
        raise TypeError(f"table name is {type(table_name)}, expected {str}")
    if not isinstance(bucket_name, str):
        raise TypeError(f"ingestion bucket name is {type(bucket_name)}, expected {str}")
    if not isinstance(time_stamp, datetime):
        raise TypeError(f"time stamp is {type(time_stamp)}, expected {datetime}")

    folder_name = time_stamp.strftime("%Y%m%d%H%M%S")

    try:
        wr.s3.to_csv(
            table_df, f"s3://{bucket_name}/{folder_name}/{table_name}.csv", index=False
        )

    except Exception as e:
        error = e.response["Error"]
        if e.response["Error"]["Code"] == "NoSuchBucket":
            logger.error(f"NoSuchBucket: {error['BucketName']} does not exist")

            raise KeyError(f"{error['BucketName']} does not exist")
        else:
            raise e


def connect_db(db_credentials):
    """
    This function uses the supplied credentials to establish a database connection.

    Args:
        db_credentials (dict): A dictionary containing database connection credentials.

    Returns:
        Connection: A database connection object.

    Raises:
        TypeError: If db_credentials is not supplied as a dictionary.

        KeyError: If any neccessary credentials are missing from db_credentials.

        ValueError: If any neccessary credential value is not a string.

        InterfaceError: If the attempt to establish a connection fails.

        DatabaseError: If there's an error from the database itself.
    """
    if not isinstance(db_credentials, dict):
        raise TypeError(f"db_credentials is {type(db_credentials)}, {dict} is required")

    db_credentials_keys = list(db_credentials.keys())
    required_keys = ["host", "port", "dbname", "username", "password"]

    for req_key in required_keys:
        if req_key not in db_credentials_keys:
            raise KeyError(f"db_credentials contains does not contain {req_key}")

    for key in db_credentials:
        if not isinstance(db_credentials[key], str):
            raise ValueError(
                f"value for {key} is {type(db_credentials[key])}, {str} is required"
            )

    try:
        connection = Connection(
            host=db_credentials["host"],
            port=db_credentials["port"],
            database=db_credentials["dbname"],
            user=db_credentials["username"],
            password=db_credentials["password"],
        )
        return connection
    except InterfaceError:
        logger.error(f"InterfaceError: please check your database credentials")
        raise InterfaceError

    except DatabaseError:
        logger.error(f"DatabaseError: please contact your database administrator")
        raise DatabaseError

def query_controller(table_name, bucket_name):
    """
    This function constructs an SQL query, based on the last extraction 
    timestamp of the given table, to fetch date from that table. If 
    no record of the last extraction timestamp is present; 
    last_job_timestamp equates to False, and the query retrieves
    all rows from that table. Otherwise, the function retrieves only
    rows with a timestamp greater than the last_job_timestamp.

    Args:
        table_name (str): Name of the database table to query.

        bucket_name (str): Name of the ingestion s3 bucket.

    Returns:
        str: SQL query string to fetch specific data from the table.

    Note:
        This function relies on, and utilises, the get_last_job_timestamp()
        utility function which returns a correctly formatted timestamp in
        regards to the last data stored in the ingestion s3 bucket.
    """
    last_job_timestamp = get_last_job_timestamp(bucket_name)

    if last_job_timestamp == False:
        return f"SELECT * FROM {identifier(table_name)};"
    else:
        return f"SELECT * FROM {identifier(table_name)} WHERE last_updated > {literal(last_job_timestamp)};"


def get_last_job_timestamp(bucket_name):
    try:
        key = "lastjob.txt"
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket_name, Key=key)
        body = response.get("Body")
        timestamp_str = body.read().decode()
        return datetime.fromtimestamp(float(timestamp_str))

    except s3.exceptions.NoSuchKey as e:
        return False
    except s3.exceptions.NoSuchBucket as e:
        logger.error(f"The S3 bucket {bucket_name} does not exist.")
        raise e


def log_latest_job_extract(bucket_name, timestamp):
    """
    This function writes the supplied timestamp, of the most recent extraction 
    job, to an S3 object named 'lastjob.txt' in the specified S3 bucket.

    Args:
        bucket_name (str): Name of the ingestion s3 bucket to store the timestamp.

        timestamp (datetime): Timestamp of most recent extraction job.

    Returns:
        None.

    Raises:
        TypeError: If the supplied bucket name is not a string, 
        or, if the timestamp is not a datetime object.

        KeyError: If the specified s3 bucket does not exist.

        RuntimeError: If the Lambda function lacks the neccessary S3 access policy, 
        and also; in the case of other unknown errors.
    """
    if not isinstance(bucket_name, str):
        raise TypeError(f"bucket name {type(bucket_name)}, expected {str}")
    if not isinstance(timestamp, datetime):
        raise TypeError(f"timestamp {type(timestamp)}, expected {datetime}")

    try:
        s3 = boto3.client("s3")

        s3.put_object(
            Body=str(timestamp.timestamp()), Bucket=bucket_name, Key=f"lastjob.txt"
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.error(
                f"ResourceNotFoundException: the bucket {bucket_name} cannot be found in S3"
            )
            raise KeyError(
                f"ResourceNotFoundException: the bucket {bucket_name} cannot be found in S3"
            )
        elif e.response["Error"]["Code"] == "AccessDeniedException":
            logger.error(
                f"AccessDeniedException: the lambda does not have an identity-based policy to access S3 resource"
            )
            raise RuntimeError(
                f"AccessDeniedException: the lambda does not have an identity-based policy to access S3 resource"
            )
        else:
            logger.error("ERROR: Unknown error whilst logging extract history to S3")
            raise e

def log_latest_job_transform(bucket_name, timestamp):
    if not isinstance(bucket_name, str):
        raise TypeError(f"bucket name {type(bucket_name)}, expected {str}")
    if not isinstance(timestamp, str):
        raise TypeError(f"timestamp {type(timestamp)}, expected {str}")

    try:
        timestamp_df = wr.s3.read_csv(path=f"s3://{bucket_name}/lastjob/lastjob.csv")
        timestamp_df.loc[len(timestamp_df)] = [timestamp]
        wr.s3.to_csv(
            timestamp_df, f"s3://{bucket_name}/lastjob/lastjob.csv", index=False
        )
    except wr.exceptions.NoFilesFound as e:
        # create df with timestamp
        timestamp_df = pd.DataFrame(data=[[timestamp]])
        # upload as csv
        wr.s3.to_csv(
            timestamp_df, f"s3://{bucket_name}/lastjob/lastjob.csv", index=False
        )
    except Exception as e:
        logger.error("ERROR: Unknown error whilst logging extract history to S3")
        raise e

def trigger_transform_lambda(bucket_name, prefix):
    """
    This function puts an file, with a prefixed and timestamped filename, in a specified s3 bucket.

    Args:
        bucket_name (str): The name of the target S3 bucket.

        prefix (str): The prefix to be added to the beginning of the filename.

    Returns:
        None.

    Raises:
        TypeError: If either bucket_name or prefix are not strings.

        KeyError: If the specified bucket does not exist.

        Exception: If an error arises whilst logging to S3.
    """
    if not isinstance(bucket_name, str):
        raise TypeError(f"bucket name {type(bucket_name)}, expected {str}")
    if not isinstance(prefix, str):
        raise TypeError(f"prefix {type(prefix)}, expected {str}")

    try:
        log_string = "SUCCESS"
        file_name = f"{datetime.now().strftime('%d%m%Y%H%M')}"  # ddmmyyhhmmss
        s3 = boto3.client("s3")

        s3.put_object(
            Body=log_string, Bucket=bucket_name, Key=f"{prefix}/{file_name}.txt"
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.error(
                f"ResourceNotFoundException: the bucket {bucket_name} cannot be found in S3"
            )
            raise KeyError(
                f"ResourceNotFoundException: the bucket {bucket_name} cannot be found in S3"
            )
        elif e.response["Error"]["Code"] == "AccessDeniedException":
            logger.error(
                f"AccessDeniedException: the lambda does not have an identity-based policy to access S3 resource"
            )
            raise RuntimeError(
                f"AccessDeniedException: the lambda does not have an identity-based policy to access S3 resource"
            )
        else:
            logger.error("ERROR: Unknown error whilst logging extract history to S3")
            raise e


def read_csv_to_pandas(file, source_bucket):
    """
    This function reads a CSV file from an S3 bucket and returns its content as a pandas DataFrame.

    Args:
        file (str): The name of the CSV file to be read (without the '.csv' extension).
        
        source_bucket (str): The name of the source S3 bucket containing the CSV file.

    Returns:
        pandas.DataFrame: The contents of the CSV file as a pandas DataFrame.

    Raises:
        KeyError: If the specified bucket cannot be found in S3.

        RuntimeError: If the Lambda function lacks the necessary policy to access the S3 resource.
        
        Exception: If an unknown error occurs during the reading process.
    """
    try:
        return wr.s3.read_csv(path=f"s3://{source_bucket}/{file}.csv")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.error(
                f"ResourceNotFoundException: the bucket {source_bucket} cannot be found in S3"
            )
            raise KeyError(
                f"ResourceNotFoundException: the bucket {source_bucket} cannot be found in S3"
            )
        elif e.response["Error"]["Code"] == "AccessDeniedException":
            logger.error(
                f"AccessDeniedException: the lambda does not have an identity-based policy \
                    to access {source_bucket} S3 resource"
            )
            raise RuntimeError(
                f"AccessDeniedException: the lambda does not have an identity-based policy \
                    to access {source_bucket} S3 resource"
            )
        else:
            logger.error("ERROR: Unknown error whilst logging extract history to S3")
            raise e


def write_df_to_parquet(df, file, target_bucket, folder_name):
    """
    This function writes a pandas DataFrame to Parquet format and saves it to an S3 bucket.

    Args:
        df (pandas.DataFrame): The DataFrame to be written to Parquet.

        file (str): The desired name for the Parquet file (without the '.parquet' extension).

        target_bucket (str): The name of the target S3 bucket for storing the Parquet file.

    Returns:
        None: This function doesn't return a value, but it writes the DataFrame to Parquet.

    Raises:
        Exception: If an error occurs during the writing process.
    """

    try:
        return wr.s3.to_parquet(
            df=df, path=f"s3://{target_bucket}/{folder_name}/{file}.parquet"
        )
    except Exception as e:
        logger.error("ERROR: write_df_to_parquet")
        raise e


def timestamp_to_date_and_time(dataframe):
    """
    This function extracts and separates date and time data, from timestamp data columns labelled 
    'created_at' and 'last_updated', into new date and time columns each before dropping the 
    original timestamp column.

    Args:
        dataframe (pandas.DataFrame): The DataFrame containing timestamp columns.

    Returns:
        pandas.DataFrame: The input DataFrame with additional date and time columns.

    Raises:
        Exception: If an error occurs during the transformation process.t
    """
    try:
        if len(dataframe) > 0:
            new_created = dataframe["created_at"].str.split(" ", n=1, expand=True)
            dataframe["created_date"] = new_created[0]
            dataframe["created_time"] = new_created[1]
            dataframe.drop(columns=["created_at"], inplace=True)
            new_updated = dataframe["last_updated"].str.split(" ", n=1, expand=True)
            dataframe["last_updated_date"] = new_updated[0]
            dataframe["last_updated_time"] = new_updated[1]
            dataframe.drop(columns=["last_updated"], inplace=True)
        else:
            dataframe.drop(columns=["created_at"], inplace=True)
            dataframe.drop(columns=["last_updated"], inplace=True)
            dataframe["created_date"] = ""
            dataframe["created_time"] = ""
            dataframe["last_updated_date"] = ""
            dataframe["last_updated_time"] = ""
        return dataframe
    except Exception as e:
        logger.error("ERROR: timestamp_to_date_and_time")
        raise e


def add_to_dates_set(set, cols_to_add):
    """
    This function adds date values, from cols_to_add, to a set for collecting 
    dimension dates to be used later in the creation of the dim_dates table.

    Args:
        date_set (set): The set containing date values to be updated.

        cols_to_add (list): A list of pandas.Series containing date values.

    Returns:
        None

    Raises:
        Exception: If an error occurs during the addition process.
    """ 
    try:
        for col in cols_to_add:
            for row in col:
                set.add(row)
    except Exception as e:
        logger.error("ERROR: add_to_dates_set")
        raise e
