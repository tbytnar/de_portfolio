import boto3
from common_library.common_aws import GetSecretJson, DownloadFileFromS3
from common_library.common_sharepoint import upload_file_to_sharepoint
from common_library.common_sftp import ConnectToSFTPSource
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def submit_aws_transfer(s3_file_key, **kwargs):
    client = boto3.client("transfer", region_name="us-east-1")

    submitter_args = kwargs["submitter_args"]

    connector_id = submitter_args.get("connector_id", None)
    remote_path = submitter_args.get("remote_path", None)

    if connector_id is not None and remote_path is not None and s3_file_key is not None:
        response = client.start_file_transfer(
            ConnectorId=connector_id,
            SendFilePaths=[
                s3_file_key,
            ],
            RemoteDirectoryPath=remote_path,
        )

        return response
    else:
        raise BaseException("Missing or invalid connector_id and/or remote_path")


def submit_sftp(s3_file_key, **kwargs):
    submitter_args = kwargs["submitter_args"]

    logger.info(f"File S3 Key: {s3_file_key}")

    credentials = GetSecretJson(submitter_args["credentials_secret"])

    s3_bucket = s3_file_key.split("/")[1]
    filename = s3_file_key.split("/")[-1]
    s3_key = s3_file_key.replace(f"/{s3_bucket}/", "")

    DownloadFileFromS3(bucket=s3_bucket, object_name=s3_key, local_file=f"/tmp/{filename}")

    target_directory = submitter_args["target_directory"]
    if target_directory[-1] != "/":
        target_directory += "/"

    sftp = ConnectToSFTPSource(credentials, optimize_transport=False)
    sftp.put(f"/tmp/{filename}", target_directory + filename)


def submit_sharepoint(s3_file_key, **kwargs):
    submitter_args = kwargs["submitter_args"]

    logger.info(submitter_args)

    s3_bucket = s3_file_key.split("/")[1]
    filename = s3_file_key.split("/")[-1]
    s3_key = s3_file_key.replace(f"/{s3_bucket}/", "")

    site_credentials_secret = GetSecretJson(submitter_args["credentials_secret"])

    site_url = submitter_args["site_url"]
    folder_url = submitter_args["folder_url"]
    logger.info(f"File S3 Key: {s3_file_key}")
    DownloadFileFromS3(bucket=s3_bucket, object_name=s3_key, local_file=f"/tmp/{filename}")
    result = upload_file_to_sharepoint(sharepoint_secrets=site_credentials_secret, file_path=f"/tmp/{filename}", site_url=site_url, folder_url=folder_url)

    if result.serverRelativeUrl:
        logger.info("File has been uploaded into: {0}".format(result.serverRelativeUrl))
    else:
        logger.error("Error transfering file to Sharepoint")
        raise BaseException(f"Error transfering file to Sharepoint: {result}")


def submit_mock(filename, **kwargs):
    submitter_args = kwargs["submitter_args"]
    return submitter_args["return"]
