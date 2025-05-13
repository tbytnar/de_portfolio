import boto3
import botocore
import json
import time
import os
from botocore.config import Config
import subprocess
import logging
import pandas as pd
import common_library.common_config as config

log = logging.getLogger(__name__)

aws_config = Config(region_name="us-east-1", signature_version="v4", retries={"max_attempts": 10, "mode": "standard"})

try:
    boto3.setup_default_session(profile_name="greengrassprod")
    boto3.client("s3")
except botocore.exceptions.ProfileNotFound:
    log.warning("greengrassprod profile not found, assuming local congfiguration for AWS access.")
except botocore.exceptions.TokenRetrievalError:
    log.error("greengrassprod profile found, but Session Expired")
    return_code = subprocess.call("aws --profile greengrassprod sso login", shell=True)


# secrets management
aws_access_key_id = os.getenv("AWS_ACCESS_KEY")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")


def GetAWSClient(service):
    """
    Creates and returns an AWS service client using provided credentials.

    Parameters:
    ----------
    service : str
        The AWS service name (e.g., 's3', 'secretsmanager', etc.)

    Functionality:
    --------------
    - Initializes a boto3 client for the specified AWS service.
    - Uses environment variables for authentication.
    - Ensures connections are made to the `us-east-1` region.

    Returns:
    -------
    boto3.client
        A boto3 client for interacting with the specified AWS service.
    """
    return boto3.client(service, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name="us-east-1")


def GetAWSResource(service):
    """
    Creates and returns an AWS service resource object.

    Parameters:
    ----------
    service : str
        The AWS service name (e.g., 's3', 'dynamodb', etc.)

    Functionality:
    --------------
    - Uses the AWS SDK (boto3) to establish a resource-level connection.
    - Provides access to AWS resources with higher-level object-oriented interactions.
    - Uses environment variables or AWS credentials to authenticate.
    - Ensures connections are made to the `us-east-1` region.

    Returns:
    -------
    boto3.resource
        A boto3 resource for interacting with the specified AWS service.
    """
    return boto3.resource(service, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name="us-east-1")


def GetSecretJson(secret_name):
    """
    Retrieves a secret from AWS Secrets Manager and returns it as a JSON object.

    Parameters:
    ----------
    secret_name : str
        The name of the secret stored in AWS Secrets Manager.

    Functionality:
    --------------
    - Establishes a connection to AWS Secrets Manager.
    - Retrieves the specified secret.
    - Parses and returns the secret as a JSON object.
    - Useful for storing and retrieving structured credentials or configurations.

    Returns:
    -------
    dict
        The secret as a JSON object.

    Raises:
    -------
    botocore.exceptions.ClientError
        If the secret retrieval fails (e.g., due to permission issues or missing secret).
    """

    asm_client = boto3.client("secretsmanager", config=aws_config)
    secret = asm_client.get_secret_value(SecretId=secret_name)
    return json.loads(secret["SecretString"])


def GetSecretString(secret_name):
    """
    Retrieves a secret from AWS Secrets Manager and returns it as a string.

    Parameters:
    ----------
    secret_name : str
        The name of the secret stored in AWS Secrets Manager.

    Functionality:
    --------------
    - Establishes a connection to AWS Secrets Manager.
    - Retrieves the specified secret as a string.
    - Unlike `GetSecretJson`, this function returns raw string values.
    - Useful for retrieving credentials, tokens, or configuration values.

    Returns:
    -------
    str
        The secret value as a string.

    Raises:
    -------
    botocore.exceptions.ClientError
        If the secret retrieval fails (e.g., due to permission issues or missing secret).
    """

    asm_client = boto3.client("secretsmanager", config=aws_config)
    secret = asm_client.get_secret_value(SecretId=secret_name)
    return secret["SecretString"]


def GetS3Object(sourceBucket, sourceKey):
    """
    Retrieves an S3 object from the specified bucket and key.

    Parameters:
    ----------
    sourceBucket : str
        The name of the S3 bucket.

    sourceKey : str
        The key (path) of the object in the bucket.

    Functionality:
    --------------
    - Uses AWS S3 resource interface to fetch an object.
    - Returns an S3 object reference, allowing interaction (e.g., reading content, metadata).
    - Can be used to check object existence before downloading or processing.

    Returns:
    -------
    boto3.resource.Object
        The S3 object resource.

    Raises:
    -------
    botocore.exceptions.ClientError
        If the object retrieval fails (e.g., incorrect bucket/key or insufficient permissions).
    """

    # Now we retrieve the s3 object(s)
    s3_resource = boto3.resource("s3", config=aws_config)
    return s3_resource.Object(sourceBucket, sourceKey)


def DownloadFileFromS3(bucket, object_name, local_file):
    """
    Downloads a file from an S3 bucket and saves it locally.

    Parameters:
    ----------
    bucket : str
        The name of the S3 bucket.

    object_name : str
        The key (path) of the object in the S3 bucket.

    local_file : str
        The local file path where the downloaded file will be saved.

    Functionality:
    --------------
    - Attempts to download the file using AWS S3 SDK.
    - Handles potential errors such as file not found (404) or unauthorized access (403).
    - Raises an exception if the file cannot be retrieved.

    Raises:
    -------
    BaseException
        If the file does not exist (404), access is unauthorized (403), or other errors occur.
    """
    try:
        s3_client = boto3.client("s3", config=aws_config)
        s3_client.download_file(bucket, object_name, local_file)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            # The key does not exist.
            raise BaseException("Source file not found")
        elif e.response["Error"]["Code"] == "403":
            # Unauthorized, including invalid bucket
            raise BaseException("Unauthorized or Invalid Bucket")
        else:
            # Something else has gone wrong.
            raise BaseException(f"Something has gone wrong fetching the source data file: {object_name}")


def UploadFileToS3(local_file, bucket, object_key):
    """
    Uploads a local file to an S3 bucket and deletes it locally after upload.

    Parameters:
    ----------
    local_file : str
        The path of the local file to be uploaded.

    bucket : str
        The name of the destination S3 bucket.

    object_key : str
        The key (path) for the file in the S3 bucket.

    Functionality:
    --------------
    - Opens the specified file in binary mode and uploads it to the S3 bucket.
    - Uses the boto3 client to upload the file.
    - Deletes the local file after successful upload to prevent redundant storage.
    - Logs success or failure of the upload operation.

    Returns:
    -------
    None

    Raises:
    -------
    botocore.exceptions.ClientError
        If the upload fails due to invalid bucket permissions or other errors.
    """

    s3_client = boto3.client("s3", config=aws_config)
    with open(local_file, "rb") as f:
        object_data = f.read()
        s3_client.put_object(Body=object_data, Bucket=bucket, Key=object_key)
    os.remove(local_file)
    log.info(f"{local_file} has successfully been uploaded to S3 as: {object_key}")


def StartGlueJob(job_name, arguments_dict):
    """
    Starts an AWS Glue job and monitors its execution status.

    Parameters:
    ----------
    job_name : str
        The name of the AWS Glue job to start.

    arguments_dict : dict
        A dictionary of arguments to pass to the Glue job.

    Functionality:
    --------------
    - Initiates an AWS Glue job with the provided job name and parameters.
    - Enables AWS Glue job metrics and logs for monitoring.
    - Periodically checks the job status until completion or failure.
    - Logs the job's progress and final status.
    - If the job fails or is stopped, raises an exception with a CloudWatch error logs URL.

    Returns:
    -------
    None

    Raises:
    -------
    BaseException
        If the Glue job fails or is stopped before completion.
    botocore.exceptions.ClientError
        If AWS credentials are invalid or the job does not exist.
    """

    glue_client = boto3.client("glue", config=aws_config)

    def GetJobStatus(job_name, jobRunID):
        try:
            response = glue_client.get_job_run(JobName=job_name, RunId=jobRunID, PredecessorsIncluded=True | False)
        except Exception as error:
            log.error(error)
        return response

    arguments_dict["--enable-metrics"] = "true"
    arguments_dict["--enable-continuous-cloudwatch-log"] = "true"
    try:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=arguments_dict,
        )
        log.info(response)
    except botocore.exceptions.TokenRetrievalError:
        log.error("ERROR: You must authenticate before running this script.  Example:  aws configure sso --profile greengrassprod")

    jobRunID = response["JobRunId"]
    jobStatus = "RUNNING"
    while jobStatus == "RUNNING" or jobStatus == "STOPPING":
        result = GetJobStatus(job_name, jobRunID)
        jobStatus = result["JobRun"]["JobRunState"]
        log.info(f"Polling for AWS Glue Job {job_name}. Current status: {jobStatus}.")
        time.sleep(5)

    if jobStatus == "FAILED":
        log.ERROR(json.dumps(result, indent=4, default=str))
        error_logs_url = f"https://us-east-1.console.aws.amazon.com/cloudwatch/home#logStream:group=/aws-glue/jobs/error;prefix={jobRunID};streamFilter=typeLogStreamPrefix"
        raise BaseException(f"\n{job_name} failed. JobID: {jobRunID}. Error Logs: {error_logs_url}")
    elif jobStatus == "STOPPED":
        raise BaseException(f"{job_name} was stopped. JobID: {jobRunID}.")
    else:
        log.info(f"Job run has finished.  Result: {jobStatus}")


def DataframeFromS3(s3_url, file_type, datatype_columns=None, delimiter=None, skip_rows=None, header=None):
    """
    Loads a file from the given S3 URL based on its type (CSV, XLSX, or Parquet) and returns a pandas DataFrame.

    Parameters:
    ----------
    s3_url : str
        The S3 URL of the file.

    file_type : str
        The file type ('csv', 'xlsx', or 'parquet').

    datatype_columns : list, optional
        A List of (tuple) column names and their data types. Default is None.

    delimiter : str, optional
        The delimiter to use when reading CSV files. Not applicable for 'xlsx' and 'parquet' files.

    skip_rows : int, optional
        The number of rows to skip when reading the file.

    Returns:
    -------
    pd.DataFrame
        The loaded data as a pandas DataFrame.

    Notes:
    ------
    - For 'parquet' files, the 'delimiter' parameter is ignored.
    - The 'skip_rows' parameter is not directly supported by 'read_parquet' and is applied after reading the data.
    - The 'datatype_columns' parameter is ignored for 'parquet' files since data types are preserved.
    """
    # If datatype_columns is provided, split it into column names and data types
    if datatype_columns:
        column_names = [col.name for col in datatype_columns]
        datatypes = [(col.name, config.pd_converters_map[col.data_type]) for col in datatype_columns]
    else:
        column_names = None
        datatypes = None

    log.info(datatypes)

    if file_type == "xlsx":
        return pd.read_excel(
            s3_url,
            converters={col.name: config.pd_converters_map[col.data_type] for col in datatype_columns} if datatype_columns else None,
            header=None,
            names=column_names,
            skiprows=skip_rows,
        )
    elif file_type == "csv":
        return pd.read_csv(
            s3_url,
            converters={col.name: config.pd_converters_map[col.data_type] for col in datatype_columns} if datatype_columns else None,
            delimiter=delimiter,
            header=header,
            names=column_names,
            skiprows=skip_rows,
        )
    elif file_type == "parquet":
        # Read the parquet file
        df = pd.read_parquet(s3_url)
        return df
    else:
        log.error(f"Unknown file_type: {file_type} in function DataframeFromS3")
        raise ValueError(f"Unsupported file_type '{file_type}'. Supported types are 'csv', 'xlsx', and 'parquet'.")
