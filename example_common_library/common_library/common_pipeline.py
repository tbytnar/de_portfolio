from datetime import datetime
import logging
import pandas as pd
import numpy as np
import common_library.common_config as config  # noqa: F401
from common_library.common_datasets import BaseDataset
from common_library.common_aws import UploadFileToS3, DataframeFromS3, GetAWSResource, GetSecretJson, DownloadFileFromS3
from common_library.common_snowflake import (
    ExecuteQuery,
    DataframeToSnowflake,
    ConvertTimestampToSQLDateTime,
)
from common_library.common_smc import (
    ParseSMCTriggerFile,
    ParseSMCFileName,
)
from common_library.common_sftp import ListSFTPFilesInPath, SFTPSourceFile, DownloadSFTPFile
import os
import uuid
import pendulum

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
local_cache = "/tmp/"


def SMCSourceToRaw(dataset_object: BaseDataset, start_time: datetime, end_time: datetime) -> bool:
    """
    Processes files from an SFTP source, uploads them to an S3 raw bucket, and updates metadata in the Snowflake
    management table for all new files.

    Parameters:
    ----------
    dataset_object : BaseDataset
        The dataset object containing metadata such as provider, schema, table, and SFTP credentials.

    Functionality:
    --------------
    1. Fetches existing data file records from the Snowflake management database.
    2. Retrieves and filters files from the SFTP source path.
    3. Identifies matching files (data and trigger files) and processes new ones.
    4. Uploads files to the S3 raw bucket and parses trigger files for metadata.
    5. Upserts metadata about the processed files into the Snowflake management database.

    Returns:
    --------
    None

    Raises:
    -------
    RuntimeError : If an error occurs during file processing or metadata upsertion.
    """
    if dataset_object.config.enabled:
        # Retrieve existing data file records from the management database
        query = f"""
        SELECT * FROM {config.datafile_management_db_name}.{config.datafile_management_schema_name}.{config.datafile_management_table_name} 
        WHERE ds_source_provider = '{dataset_object.config.provider}'
        AND ds_schema = '{dataset_object.config.target_schema}'
        AND ds_table = '{dataset_object.config.target_table}'  
        """
        try:
            SMC_SFTP_DataFiles_dataframe = ExecuteQuery(sql_query=query, database=config.datafile_management_db_name, schema=config.datafile_management_schema_name, as_pandas=True)
        except Exception as e:
            log.error(f"Failed to fetch existing data file records: {e}")
            raise RuntimeError(f"Error fetching data file records: {e}")

        # Retrieve SFTP credentials
        try:
            sftp_credentials = GetSecretJson(dataset_object.config.credentials_secret)
        except Exception as e:
            log.error(f"Failed to retrieve SFTP credentials: {e}")
            raise RuntimeError(f"Error retrieving SFTP credentials: {e}")

        if sftp_credentials:
            try:
                # Retrieve list of files from the SFTP path
                source_files = ListSFTPFilesInPath(sftp_credentials, dataset_object=dataset_object, start_time=start_time, end_time=end_time)
                log.info(f"Found {len(source_files)} files with the prefix: {dataset_object.config.file_prefix}")

                smc_source_files = [SFTPSourceFile(x["file_name"], x["modified_time"]) for x in source_files]

                if len(smc_source_files) > 0:
                    # Filter out data and trigger files
                    file_types = {"DATA", "RECON", "ADHOC"}
                    trg_file_types = {f"{file_type}_TRG" for file_type in file_types}

                    dat_files = [file for file in smc_source_files if file.smc_file_type in file_types]
                    trg_files = [file for file in smc_source_files if file.smc_file_type in trg_file_types]

                    log.info(f"Data Files Found: {len(dat_files)}")
                    log.info(f"Trigger Files Found: {len(trg_files)}")

                    new_files = False

                    # Process files and send metadata to Snowflake
                    trg_file: SFTPSourceFile
                    for trg_file in trg_files:
                        dat_file: SFTPSourceFile
                        for dat_file in dat_files:
                            if trg_file.file_name_no_ext == dat_file.file_name_no_ext:
                                if not (SMC_SFTP_DataFiles_dataframe["DAT_FILENAME"].str.contains(dat_file.file_name)).any():
                                    log.info(f"Processing new files: {dat_file.file_name} and {trg_file.file_name}")
                                    dat_file_key = f"{dataset_object.config.provider}/SFTP/{dataset_object.config.target_schema}/{dataset_object.config.target_table}/{dat_file.last_modified.year}/{dat_file.last_modified.month}/{dat_file.last_modified.day}/{dat_file.file_name}"
                                    trg_file_key = f"{dataset_object.config.provider}/SFTP/{dataset_object.config.target_schema}/{dataset_object.config.target_table}/{trg_file.last_modified.year}/{trg_file.last_modified.month}/{trg_file.last_modified.day}/{trg_file.file_name}"

                                    try:
                                        # Download and upload data file
                                        dat_local_path = DownloadSFTPFile(sftp_credentials, dat_file, dataset_object.config.source_path)
                                        UploadFileToS3(dat_local_path, config.raw_s3_bucket, dat_file_key)
                                        log.info(f"{dat_file.file_name} sent to S3 raw bucket.")

                                        # Download and upload trigger file
                                        trg_local_path = DownloadSFTPFile(sftp_credentials, trg_file, dataset_object.config.source_path)
                                        UploadFileToS3(trg_local_path, config.raw_s3_bucket, trg_file_key)
                                        log.info(f"{trg_file.file_name} sent to S3 raw bucket.")

                                        # Parse Trigger File metadata
                                        trg_data = ParseSMCTriggerFile(dataset_object=dataset_object, trg_key=trg_file_key)

                                        # Create DataFrame with metadata for Snowflake
                                        datafiles_df = pd.DataFrame(
                                            {
                                                "RECORD_ID": [str(uuid.uuid4())],
                                                "DS_SOURCE_PROVIDER": [dataset_object.config.provider],
                                                "DS_SCHEMA": [dataset_object.config.target_schema],
                                                "DS_TABLE": [dataset_object.config.target_table],
                                                "DS_DATAFEED_TYPE": [dat_file.smc_file_type],
                                                "DAT_FILENAME": [dat_file.file_name],
                                                "DAT_MODIFIED_DATE": [dat_file.last_modified],
                                                "DAT_CREATED_DATE": [dat_file.last_modified],
                                                "TRG_FILENAME": [trg_file.file_name],
                                                "TRG_MODIFIED_DATE": [trg_file.last_modified],
                                                "TRG_CREATED_DATE": [trg_file.last_modified],
                                                "TRG_RUN_NUMBER": [int(trg_data["Run_Number"].iloc[0])],
                                                "TRG_EXTRACT_BEGIN": [datetime.strptime(str(trg_data["Extract_Begin_DateTime"].iloc[0]), "%Y%m%d%H%M%S")],
                                                "TRG_EXTRACT_END": [datetime.strptime(str(trg_data["Extract_End_DateTime"].iloc[0]), "%Y%m%d%H%M%S")],
                                                "DAT_RAW_KEY": [dat_file_key],
                                                "DAT_RAW_TIMESTAMP": [datetime.now()],
                                                "TRG_RAW_KEY": [trg_file_key],
                                                "TRG_RAW_TIMESTAMP": [datetime.now()],
                                                "DAT_STAGED_TIMESTAMP": [None],
                                                "DAT_STAGE_KEY": [None],
                                                "SEQUENCE_NUMBER": [None],
                                                "SNOWFLAKE_INGESTED": [None],
                                            },
                                            dtype=str,
                                        )

                                        log.info(f"Prepared metadata DataFrame: {datafiles_df}")

                                        # Upsert into Snowflake
                                        DataframeToSnowflake(
                                            target_database=config.datafile_management_db_name,
                                            target_schema=config.datafile_management_schema_name,
                                            target_table=config.datafile_management_table_name,
                                            primary_key="RECORD_ID",
                                            dataframe=datafiles_df,
                                            operation="upsert",
                                        )
                                        log.info("Successfully upserted metadata to the management database.")

                                        new_files = True
                                    except Exception as e:
                                        log.error(f"Failed to process file {dat_file.file_name} or upsert metadata: {e}")
                                        raise
                                else:
                                    # All files found have already been processed, no need go further in staging
                                    log.info(f"{dat_file.file_name} and {trg_file.file_name} are already in the database.")
                    if new_files:
                        return True
                else:
                    # No files were found, no need to process anything and no need go further in staging
                    log.info("No files found on server.")
                    return False

            except Exception as e:
                log.error(f"Error processing SFTP files: {e}")
                raise RuntimeError(f"Error processing SFTP files: {e}")
    else:
        log.info(f"Dataset: {dataset_object.config.target_table} is disabled")


def SFTPSourceToRaw(dataset_object: BaseDataset, start_time: datetime, end_time: datetime) -> bool:
    """
    Processes files from an SFTP source, uploads them to an S3 raw bucket, and updates metadata in the Snowflake
    management table for all new files.

    Parameters:
    ----------
    dataset_object : BaseDataset
        The dataset object containing metadata such as provider, schema, table, and SFTP credentials.

    Functionality:
    --------------
    1. Fetches existing data file records from the Snowflake management database.
    2. Retrieves and filters files from the SFTP source path.
    3. Identifies matching files and processes new ones.
    4. Uploads files to the S3 raw bucket.
    5. Upserts metadata about the processed files into the Snowflake management database.

    Returns:
    --------
    None

    Raises:
    -------
    RuntimeError : If an error occurs during file processing or metadata upsertion.
    """
    if dataset_object.config.enabled:
        # Retrieve existing data file records from the management database
        query = f"""
        SELECT * FROM {config.datafile_management_db_name}.{config.datafile_management_schema_name}.{config.datafile_management_table_name} 
        WHERE ds_source_provider = '{dataset_object.config.provider}'
        AND ds_schema = '{dataset_object.config.target_schema}'
        AND ds_table = '{dataset_object.config.target_table}'  
        """
        try:
            SFTP_DataFiles_dataframe = ExecuteQuery(sql_query=query, database=config.datafile_management_db_name, schema=config.datafile_management_schema_name, as_pandas=True)
        except Exception as e:
            log.error(f"Failed to fetch existing data file records: {e}")
            raise RuntimeError(f"Error fetching data file records: {e}")

        # Retrieve SFTP credentials
        try:
            sftp_credentials = GetSecretJson(dataset_object.config.credentials_secret)
        except Exception as e:
            log.error(f"Failed to retrieve SFTP credentials: {e}")
            raise RuntimeError(f"Error retrieving SFTP credentials: {e}")

        if sftp_credentials:
            try:
                # Retrieve list of files from the SFTP path
                source_files = ListSFTPFilesInPath(sftp_credentials, dataset_object=dataset_object, start_time=start_time, end_time=end_time)
                log.info(f"Found {len(source_files)} files with the prefix: {dataset_object.config.file_prefix}")

                sftp_source_files = [SFTPSourceFile(x["file_name"], x["modified_time"]) for x in source_files]

                if len(sftp_source_files) > 0:
                    # Setup an empty list to collect DataFrames with metadata updates
                    datafile_dfs = []
                    for dat_file in sftp_source_files:
                        dat_file_key = f"{dataset_object.config.provider}/SFTP/{dataset_object.config.target_schema}/{dataset_object.config.target_table}/{dat_file.last_modified.year}/{dat_file.last_modified.month}/{dat_file.last_modified.day}/{dat_file.file_name}"

                        if not (SFTP_DataFiles_dataframe["DAT_FILENAME"].str.contains(dat_file.file_name)).any():
                            log.info(f"Processing new file: {dat_file.file_name}")
                            dat_local_path = DownloadSFTPFile(sftp_credentials, dat_file, dataset_object.config.source_path)
                            UploadFileToS3(dat_local_path, config.raw_s3_bucket, dat_file_key)
                            log.info(f"{dat_file.file_name} sent to S3 raw bucket.")

                            # Prepare new metadata for the file
                            new_metadata = {
                                "RECORD_ID": str(uuid.uuid4()),
                                "DS_SOURCE_PROVIDER": dataset_object.config.provider,
                                "DS_SCHEMA": dataset_object.config.target_schema,
                                "DS_TABLE": dataset_object.config.target_table,
                                "DS_DATAFEED_TYPE": None,
                                "DAT_FILENAME": dat_file.file_name,
                                "DAT_MODIFIED_DATE": ConvertTimestampToSQLDateTime(dat_file.last_modified),
                                "DAT_CREATED_DATE": ConvertTimestampToSQLDateTime(dat_file.last_modified),
                                "TRG_FILENAME": None,
                                "TRG_MODIFIED_DATE": None,
                                "TRG_CREATED_DATE": None,
                                "TRG_RUN_NUMBER": None,
                                "TRG_EXTRACT_BEGIN": None,
                                "TRG_EXTRACT_END": None,
                                "DAT_RAW_KEY": dat_file_key,
                                "DAT_RAW_TIMESTAMP": ConvertTimestampToSQLDateTime(datetime.now()),
                                "TRG_RAW_KEY": None,
                                "TRG_RAW_TIMESTAMP": None,
                                "DAT_STAGED_TIMESTAMP": None,
                                "DAT_STAGE_KEY": None,
                                "SEQUENCE_NUMBER": None,
                                "SNOWFLAKE_INGESTED": None,
                            }

                            datafile_dfs.append(pd.DataFrame([new_metadata]))

                    # If we have new or updated metadata, upsert into the management table
                    if datafile_dfs:
                        try:
                            metadata_df = pd.concat(datafile_dfs, ignore_index=True)
                            DataframeToSnowflake(
                                target_database=config.datafile_management_db_name,
                                target_schema=config.datafile_management_schema_name,
                                target_table=config.datafile_management_table_name,
                                primary_key="RECORD_ID",
                                dataframe=metadata_df,
                                operation="upsert",
                            )
                            log.info("Successfully upserted metadata to the management database.")
                            return True
                        except Exception as e:
                            log.error(f"Failed to upsert metadata to the management table: {e}")
                            raise
                    else:
                        # No files were found, no need to process anything and no need go further in staging
                        log.info("No new files found on server.")
                        return False

            except Exception as e:
                log.error(f"Error processing SFTP files: {e}")
                raise RuntimeError(f"Error processing SFTP files: {e}")
    else:
        log.info(f"Dataset: {dataset_object.config.target_table} is disabled")


def S3SourceToRaw(dataset_object: BaseDataset, start_date: datetime = pendulum.datetime(year=1970, month=1, day=1), end_date: datetime = pendulum.today().add(years=1)) -> bool:
    """
    Processes files from an S3 bucket and moves them to a raw S3 bucket for staging.
    Updates metadata about these files in the data management table.

    Parameters:
    ----------
    dataset_object : BaseDataset
        A dataset object that contains S3 metadata and configuration required for processing.

    Functionality:
    --------------
    1. Retrieves a list of files from the S3 source bucket.
    2. Copies these files to the raw S3 bucket.
    3. Checks if the file metadata exists in the management database.
    4. If metadata exists and the file has changed, updates the metadata.
    5. If the file is new, inserts new metadata records.
    6. Finally, sends the updated metadata to the management database.

    Notes:
    ------
    - The function expects the dataset object to have `config`, `provider`, `connection`, `schema`, and `table`.
    - The files are copied to the raw S3 bucket defined in `config.raw_s3_bucket`.
    - Data management metadata is stored in the table `DATAFILE_MANAGEMENT`.

    Returns:
    -------
    None

    Example:
    --------
    ```
    dataset_object = BaseDataset(
        provider="my_provider",
        schema="public",
        table="data_table",
        config={"source_bucket": "source-bucket", "source_key": "data/"}
    )
    S3SourceToRaw(dataset_object)
    ```
    """
    if dataset_object.config.enabled:
        try:
            s3_resource = GetAWSResource("s3")
        except Exception as e:
            log.error(f"Failed to get AWS S3 resource: {e}")
            raise

        # Retrieve the S3 objects (files) based on the source key
        files = []
        try:
            source_bucket = s3_resource.Bucket(dataset_object.config.source_bucket)
            if dataset_object.config.source_key.endswith("/"):
                objects = source_bucket.objects.filter(Prefix=dataset_object.config.source_key)
                files = [obj for obj in objects if not obj.key.endswith("/")]
            else:
                file = s3_resource.Object(dataset_object.config.source_bucket, dataset_object.config.source_key)
                files.append(file)

            if not files:
                log.warning(f"No files found for the given source key: {dataset_object.config.source_key}")
                return
        except Exception as e:
            log.error(f"Error retrieving files from S3 bucket {dataset_object.config.source_bucket}: {e}")
            raise

        # Fetch existing metadata from the management table
        query = f"""
        SELECT * FROM {config.datafile_management_db_name}.{config.datafile_management_schema_name}.{config.datafile_management_table_name} 
        WHERE ds_source_provider = '{dataset_object.config.provider}'
        AND ds_schema = '{dataset_object.config.target_schema}'
        AND ds_table = '{dataset_object.config.target_table}'  
        """
        try:
            existing_metadata = ExecuteQuery(query, config.datafile_management_db_name, config.datafile_management_schema_name, as_pandas=True)
            log.info(f"Fetched metadata for {dataset_object.config.target_table} from the management database.")
        except Exception as e:
            log.error(f"Failed to execute metadata query for {dataset_object.config.target_table}: {e}")
            raise

        # Setup an empty list to collect DataFrames with metadata updates
        datafile_dfs = []

        for file in files:
            try:
                source_filename = file.key.split("/")[-1]
                file_modified_date = file.last_modified.replace(tzinfo=None)

                matched_record = existing_metadata.loc[existing_metadata["DAT_FILENAME"] == source_filename]

                # Create raw key for the file in the raw S3 bucket
                raw_key = (
                    f"{dataset_object.config.provider}/{dataset_object.config.connection}/{dataset_object.config.target_schema}/"
                    f"{dataset_object.config.target_table}/{file_modified_date.year}/{file_modified_date.month}/{file_modified_date.day}/{source_filename}"
                )

                # Prepare new metadata for the file
                new_metadata = {
                    "RECORD_ID": None,
                    "DS_SOURCE_PROVIDER": dataset_object.config.provider,
                    "DS_SCHEMA": dataset_object.config.target_schema,
                    "DS_TABLE": dataset_object.config.target_table,
                    "DS_DATAFEED_TYPE": None,
                    "DAT_FILENAME": source_filename,
                    "DAT_MODIFIED_DATE": ConvertTimestampToSQLDateTime(file_modified_date),
                    "DAT_CREATED_DATE": ConvertTimestampToSQLDateTime(datetime.now()),
                    "TRG_FILENAME": None,
                    "TRG_MODIFIED_DATE": None,
                    "TRG_CREATED_DATE": None,
                    "TRG_RUN_NUMBER": None,
                    "TRG_EXTRACT_BEGIN": None,
                    "TRG_EXTRACT_END": None,
                    "DAT_RAW_KEY": raw_key,
                    "DAT_RAW_TIMESTAMP": ConvertTimestampToSQLDateTime(datetime.now()),
                    "TRG_RAW_KEY": None,
                    "TRG_RAW_TIMESTAMP": None,
                    "DAT_STAGED_TIMESTAMP": None,
                    "DAT_STAGE_KEY": None,
                    "SEQUENCE_NUMBER": None,
                    "SNOWFLAKE_INGESTED": None,
                }

                if file.last_modified > start_date and file.last_modified < end_date:
                    if not matched_record.empty:
                        log.info(f"File '{source_filename}' already exists in the management database.")
                        db_modified_date = matched_record["DAT_MODIFIED_DATE"].iloc[0]

                        log.info(f"File Modified Date in Database: {db_modified_date}.  Current File Modified Date: {file_modified_date}")

                        if pd.to_datetime(db_modified_date) != file_modified_date:
                            log.info(f"File '{source_filename}' has changed. Updating metadata.")

                            # Copy file to raw S3 bucket
                            copy_source = {"Bucket": dataset_object.config.source_bucket, "Key": file.key}
                            try:
                                s3_resource.meta.client.copy(copy_source, config.raw_s3_bucket, raw_key)
                                log.info(f"Successfully copied file {source_filename} to raw S3 bucket.")
                            except Exception as e:
                                log.error(f"Failed to copy file {source_filename} to raw S3 bucket: {e}")
                                raise

                            new_metadata["RECORD_ID"] = matched_record["RECORD_ID"].iloc[0]
                            datafile_dfs.append(pd.DataFrame([new_metadata]))
                        else:
                            log.info(f"File '{source_filename}' has not changed. No metadata update required.")
                    else:
                        log.info(f"New file '{source_filename}' detected. Inserting metadata.")
                        new_metadata["RECORD_ID"] = str(uuid.uuid4())
                        datafile_dfs.append(pd.DataFrame([new_metadata]))

                        # Copy file to raw S3 bucket
                        copy_source = {"Bucket": dataset_object.config.source_bucket, "Key": file.key}
                        try:
                            s3_resource.meta.client.copy(copy_source, config.raw_s3_bucket, raw_key)
                            log.info(f"Successfully copied file {source_filename} to raw S3 bucket.")
                        except Exception as e:
                            log.error(f"Failed to copy file {source_filename} to raw S3 bucket: {e}")
                            raise
                else:
                    log.info(f"File '{source_filename}' does not fall in the specified date range: Start Date={start_date}|End Date={end_date}. Skipping.")

            except Exception as e:
                log.error(f"Error processing file {file.key}: {e}")
                raise

        # If we have new or updated metadata, upsert into the management table
        if datafile_dfs:
            try:
                metadata_df = pd.concat(datafile_dfs, ignore_index=True)
                DataframeToSnowflake(
                    target_database=config.datafile_management_db_name,
                    target_schema=config.datafile_management_schema_name,
                    target_table=config.datafile_management_table_name,
                    primary_key="RECORD_ID",
                    dataframe=metadata_df,
                    operation="upsert",
                )
                log.info("Successfully upserted metadata to the management database.")
                return True
            except Exception as e:
                log.error(f"Failed to upsert metadata to the management table: {e}")
                raise
    else:
        log.info(f"Dataset: {dataset_object.config.target_table} is disabled")


def RawToStage(dataset_object: BaseDataset):
    """
    Processes raw files from either an S3 bucket or an SFTP server and stages them in an S3 staging bucket.
    Updates metadata about these files in the data management table (e.g., Snowflake).

    Parameters:
    ----------
    dataset_object : BaseDataset
        A dataset object that contains metadata such as provider, schema, table, and S3/SFTP connection details.

    Functionality:
    --------------
    1. Retrieves existing file metadata from the management database.
    2. Determines the connection type (S3 or SFTP) and prepares file handling settings (e.g., file type, delimiter).
    3. Sorts the metadata and processes each file:
        - Loads the file from S3 or SFTP.
        - If the file is new, it is uploaded to the S3 staging bucket as a Parquet file.
        - Updates metadata for each staged file.
    4. Deletes local files after successful upload to prevent local disk from filling up.
    5. Updates the management table with new or modified file metadata.

    Notes:
    ------
    - The function handles both S3 and SFTP files.
    - For SFTP connections, it assumes CSV files with a '|' delimiter.
    - Local parquet files are deleted after successful upload to S3.
    - If the file has already been staged, it is skipped.
    - Data is uploaded to the S3 bucket defined in `config.stage_s3_bucket`.

    Returns:
    -------
    None

    Example:
    --------
    ```python
    dataset_object = BaseDataset(
        provider="my_provider",
        target_schema="public",
        target_table="data_table",
        connection="s3",
        config={"file_type": "csv", "delimiter": ",", "skip_rows": 1}
    )
    RawToStage(dataset_object)
    ```
    """

    if dataset_object.config.enabled:
        # SQL query to fetch existing file metadata
        query = f"""
        SELECT * FROM {config.datafile_management_db_name}.{config.datafile_management_schema_name}.{config.datafile_management_table_name} 
        WHERE ds_source_provider = '{dataset_object.config.provider}'
        AND ds_schema = '{dataset_object.config.target_schema}'
        AND ds_table = '{dataset_object.config.target_table}'
        """
        try:
            files_df = ExecuteQuery(query, config.datafile_management_db_name, config.datafile_management_schema_name, as_pandas=True)
            log.info(f"Fetched metadata for dataset {dataset_object.config.target_table} from the management database.")
        except Exception as e:
            log.error(f"Failed to execute metadata query for dataset {dataset_object.config.target_table}: {e}")
            raise

        # Determine file type, delimiter, and skip rows based on connection type
        if dataset_object.config.connection.lower() == "s3":
            file_type = dataset_object.config.file_type.lower()
            delimiter = dataset_object.config.s3_delimiter
            skip_rows = dataset_object.config.s3_skip_rows
            sort_columns = ["DAT_MODIFIED_DATE"]
        elif dataset_object.config.provider.lower() in ["smc", "smc_test", "inovalon"] and dataset_object.config.connection.lower() == "sftp":
            file_type = "csv"
            delimiter = "|"
            header = 0
            skip_rows = None
            sort_columns = ["DAT_MODIFIED_DATE", "TRG_RUN_NUMBER"]
        elif dataset_object.config.provider.lower() not in ["smc", "smc_test", "inovalon"] and dataset_object.config.connection.lower() == "sftp":
            if dataset_object.config.compressed:
                file_type = dataset_object.config.compressed_file_type.lower()
            else:
                file_type = dataset_object.config.file_type.lower()
            delimiter = dataset_object.config.sftp_delimiter
            quote_char = dataset_object.config.quote_char
            escape_char = dataset_object.config.escape_char
            skip_rows = dataset_object.config.sftp_skip_rows
            sort_columns = ["DAT_MODIFIED_DATE"]
        else:
            log.error(f"Unknown dataset type for provider: {dataset_object.config.provider}, connection: {dataset_object.config.connection}")
            raise ValueError("Unsupported provider or connection type.")

        # Get the Max SEQUENCE_NUMBER from the datafiles dataframe
        max_sequence = files_df["SEQUENCE_NUMBER"].max(skipna=True)
        max_sequence = 0 if pd.isna(max_sequence) else max_sequence

        # Get just the un-staged files
        not_staged = files_df[files_df["DAT_STAGE_KEY"].isna()]

        # Process the unstaged files
        if len(not_staged) > 0:
            # Sort the unstaged files
            not_staged = not_staged.sort_values(by=sort_columns, ascending=True).reset_index(drop=True)

            # Process each file in the metadata DataFrame
            for idx, row in not_staged.iterrows():
                sequence_number = str(int(max_sequence + idx + 1))

                # Determine modified date based on connection type

                # file_modified_date = row.DAT_MODIFIED_DATE if dataset_object.config.connection.lower() in "s3" else row.TRG_EXTRACT_END
                dat_raw_URL = f"s3://{config.raw_s3_bucket}/{row.DAT_RAW_KEY}"

                try:
                    if dataset_object.config.connection.lower() == "s3":
                        file_modified_date = row.DAT_MODIFIED_DATE
                        # Load the data from S3
                        table_df = DataframeFromS3(dat_raw_URL, file_type, dataset_object.config.columns, delimiter, skip_rows)
                    elif dataset_object.config.provider.lower() in ["smc", "smc_test", "inovalon"] and dataset_object.config.connection.lower() == "sftp":
                        file_modified_date = row.TRG_EXTRACT_END
                        # Load the data from SMC SFTP
                        table_df = DataframeFromS3(dat_raw_URL, file_type, dataset_object.config.columns, delimiter, skip_rows, header)
                    elif dataset_object.config.provider.lower() not in ["smc", "smc_test", "inovalon"] and dataset_object.config.connection.lower() == "sftp":
                        file_modified_date = row.DAT_MODIFIED_DATE
                        if dataset_object.config.compressed:
                            column_names = [col.name for col in dataset_object.config.columns]

                            DownloadFileFromS3(bucket=config.raw_s3_bucket, object_name=row.DAT_RAW_KEY, local_file=os.path.join(config.working_directory, row.DAT_FILENAME))

                            if dataset_object.config.compression_method.lower() == "tar":
                                import tarfile

                                compressed_file = tarfile.open(os.path.join(config.working_directory, row.DAT_FILENAME))
                                compressed_file.extractall(os.path.join(config.working_directory, dataset_object.config.target_table))

                                for root, dir, files in os.walk(os.path.join(config.working_directory, dataset_object.config.target_table)):
                                    for file in files:
                                        if file.startswith(dataset_object.config.compressed_file_prefix) and file.endswith(dataset_object.config.compressed_file_type):
                                            inner_file_path = os.path.join(root, file)
                            elif dataset_object.config.compression_method.lower() == "gz":
                                import gzip
                                import shutil

                                with gzip.open(os.path.join(config.working_directory, row.DAT_FILENAME), "rb") as f_in:
                                    with open(os.path.join(config.working_directory, row.DAT_FILENAME.replace(".gz", "")), "wb") as f_out:
                                        shutil.copyfileobj(f_in, f_out)

                                inner_file_path = os.path.join(config.working_directory, row.DAT_FILENAME.replace(".gz", ""))

                            else:
                                log.error(f"Unknown file compression type for provider: {dataset_object.config.provider}, connection: {dataset_object.config.connection}")
                                raise

                            table_df = pd.read_csv(filepath_or_buffer=inner_file_path, sep=delimiter, quotechar=quote_char, escapechar=escape_char, names=column_names, skiprows=skip_rows)
                        else:
                            table_df = DataframeFromS3(s3_url=dat_raw_URL, file_type=file_type, datatype_columns=dataset_object.config.columns, delimiter=delimiter, skip_rows=skip_rows)
                    else:
                        log.error(f"Unknown dataset type for provider: {dataset_object.config.provider}, connection: {dataset_object.config.connection}")

                except Exception as e:
                    log.error(f"Failed to load data from for {row.DAT_RAW_KEY}: {e}")
                    raise RuntimeError(f"Error loading data: {e}")

                if len(table_df) > 0:
                    # Perform DataQuality checks on the dataset
                    failed_df, passed_df = dataset_object.PerformDataQuality(table_df)

                    # Perform Data Transformations on the dataset
                    transformed_df = dataset_object.PerformDataTransformations(passed_df)

                    # Replace NaN-like and literal blank values with None
                    transformed_df.replace([np.nan, "None", "nan", "NaN", "nat", "NaT", ""], None, inplace=True)

                    # Drop duplicate records
                    # transformed_df.drop_duplicates(dataset_object.config.primary_key, keep="last")

                    # Write the DataFrame to a parquet file and upload it to S3
                    stage_key_path = (
                        f"{dataset_object.config.provider}/{dataset_object.config.connection}/{dataset_object.config.target_schema}/"
                        f"{dataset_object.config.target_table}/{file_modified_date.year}/{file_modified_date.month}/{file_modified_date.day}"
                    )
                    passed_s3_file_name = f"{dataset_object.config.target_table}_{sequence_number}.parquet"
                    failed_s3_file_name = f"{dataset_object.config.target_table}_{sequence_number}_QUARANTINE.parquet"
                    passed_local_file_path = os.path.join(local_cache, passed_s3_file_name)
                    failed_local_file_path = os.path.join(local_cache, failed_s3_file_name)

                    try:
                        # Upload data quality PASSED records to S3
                        transformed_df.to_parquet(passed_local_file_path, compression="gzip")
                        UploadFileToS3(local_file=passed_local_file_path, bucket=config.stage_s3_bucket, object_key=f"{stage_key_path}/{passed_s3_file_name}")
                        log.info(f"File {passed_s3_file_name} uploaded to S3 staging bucket.")

                        # Remove the local file after successful upload
                        if os.path.exists(passed_local_file_path):
                            os.remove(passed_local_file_path)
                            log.info(f"Local file {passed_local_file_path} removed after successful upload.")
                        else:
                            log.warning(f"Local file {passed_local_file_path} not found for deletion.")

                        if failed_df is not None:
                            # Upload data quality FAILED records to S3 as QUARANTINE
                            failed_df.to_parquet(failed_local_file_path, compression="gzip")
                            UploadFileToS3(local_file=failed_local_file_path, bucket=config.stage_s3_bucket, object_key=f"{stage_key_path}/{failed_s3_file_name}")
                            log.info(f"File {failed_s3_file_name} uploaded to S3 staging bucket.")

                            # Remove the local file after successful upload
                            if os.path.exists(failed_local_file_path):
                                os.remove(failed_local_file_path)
                                log.info(f"Local file {failed_local_file_path} removed after successful upload.")
                            else:
                                log.warning(f"Local file {failed_local_file_path} not found for deletion.")

                    except Exception as e:
                        log.error(f"Failed to write or upload parquet file for {row.DAT_RAW_KEY}: {e}")
                        raise RuntimeError(f"Error during file processing: {e}")

                    # Update metadata for staged file
                    not_staged.at[idx, "DAT_STAGE_KEY"] = f"{stage_key_path}/{passed_s3_file_name}"
                    not_staged.at[idx, "DAT_STAGED_TIMESTAMP"] = ConvertTimestampToSQLDateTime(datetime.now())
                    not_staged.at[idx, "SEQUENCE_NUMBER"] = sequence_number
                else:
                    # Handle empty DataFrame case
                    log.warning(f"No data found for {row.DAT_RAW_KEY}. Marking as 'Empty Source'.")
                    not_staged.at[idx, "DAT_STAGE_KEY"] = "Empty Source"
                    not_staged.at[idx, "DAT_STAGED_TIMESTAMP"] = ConvertTimestampToSQLDateTime(datetime.now())
                    not_staged.at[idx, "SEQUENCE_NUMBER"] = sequence_number
                    not_staged.at[idx, "SNOWFLAKE_INGESTED"] = True

            # Update the metadata database with the new file's metadata
            try:
                DataframeToSnowflake(
                    target_database=config.datafile_management_db_name,
                    target_schema=config.datafile_management_schema_name,
                    target_table=config.datafile_management_table_name,
                    primary_key="RECORD_ID",
                    dataframe=not_staged,
                    operation="upsert",
                )
                log.info(f"Metadata for dataset {dataset_object.config.target_table} successfully updated.")
            except Exception as e:
                log.error(f"Failed to upsert metadata to Snowflake for dataset {dataset_object.config.target_table}: {e}")
                raise RuntimeError(f"Error updating metadata: {e}")
        else:
            log.info(f"Dataset: {dataset_object.config.target_table} has no unstaged files")
    else:
        log.info(f"Dataset: {dataset_object.config.target_table} is disabled")


def StageToSnowflake(dataset_object: BaseDataset):
    """
    Stages files from the S3 staging bucket to Snowflake and updates the data management table.

    Parameters:
    ----------
    dataset_object : BaseDataset
        A dataset object containing metadata such as provider, schema, table, and Snowflake connection details.

    Functionality:
    --------------
    1. Retrieves files that have not yet been ingested into Snowflake from the management database.
    2. Loads the files from the S3 staging bucket, processes them, and sends them to Snowflake.
    3. Updates the Snowflake ingestion status in the management table.

    Notes:
    ------
    - The function expects the dataset object to have `target_database`, `target_schema`, and `target_table`.
    - Data is uploaded to Snowflake using the `DataframeToSnowflake` function.
    - Metadata about file ingestion is updated in the management table.

    Returns:
    -------
    None

    Example:
    --------
    ```python
    dataset_object = BaseDataset(
        provider="my_provider",
        target_schema="public",
        target_table="data_table",
        connection="s3",
        config={"file_type": "parquet"}
    )
    StageToSnowflake(dataset_object)
    ```
    """
    if dataset_object.config.enabled:
        # Fetch existing metadata from the management table
        query = f"""
        SELECT * FROM {config.datafile_management_db_name}.{config.datafile_management_schema_name}.{config.datafile_management_table_name} 
        WHERE (SNOWFLAKE_INGESTED IS NULL OR SNOWFLAKE_INGESTED = 'FALSE')
        AND LOWER(ds_source_provider) = '{dataset_object.config.provider.lower()}'
        AND LOWER(ds_schema) = '{dataset_object.config.target_schema.lower()}'
        AND LOWER(ds_table) = '{dataset_object.config.target_table.lower()}';
        """
        try:
            files_df = ExecuteQuery(query, config.datafile_management_db_name, config.datafile_management_schema_name, as_pandas=True)
            log.info(f"Fetched metadata for table {dataset_object.config.target_table} from the management database.")
        except Exception as e:
            log.error(f"Failed to execute metadata query for table {dataset_object.config.target_table}: {e}")
            raise

        # Sort files by sequence number
        files_df = files_df.sort_values(by=["SEQUENCE_NUMBER"], ascending=True)
        send_to_dfm_table = False

        # Process each file in the metadata DataFrame
        for idx, row in files_df.iterrows():
            log.info(f"Processing {row.DAT_STAGE_KEY}. SNOWFLAKE_INGESTED = {row.SNOWFLAKE_INGESTED}")
            if row.SNOWFLAKE_INGESTED is None or row.SNOWFLAKE_INGESTED is False:
                source_data = row.DAT_FILENAME
                dat_stage_key = row.DAT_STAGE_KEY
                dat_stage_URL = f"s3://{config.stage_s3_bucket}/{dat_stage_key}"

                try:
                    # Load data from S3
                    table_df = DataframeFromS3(s3_url=dat_stage_URL, file_type="parquet")
                    log.info(f"Loaded data from {dat_stage_URL}")

                    # Replace literal "None" strings with None
                    table_df.replace("None", None, inplace=True)

                    # INVOVALON SMC Overrides
                    if dataset_object.config.provider.lower() in ["inovalon", "smc"] and dataset_object.config.connection.lower() == "sftp":
                        # Uppercasing all column names in the dataframe.
                        # TODO: If MentorMate no longer sources our Stage files, this can be dropped and the columns be renamed in the config files.
                        table_df.columns = [col.upper() for col in table_df.columns]
                        dataset_object.config.primary_key = dataset_object.config.primary_key.upper()

                        data_feed_name, date_stamp, data_type, file_type = ParseSMCFileName(row.DAT_FILENAME)
                        log.info(f"Setting load operation: {data_type.lower()}")
                        if data_type.lower() == "adhoc":
                            data_load_operation = "replace"
                        else:
                            data_load_operation = dataset_object.config.operation
                    else:
                        data_load_operation = dataset_object.config.operation

                    log.info(
                        f"Performing {data_load_operation} for dataset into {dataset_object.config.target_database}.{dataset_object.config.target_schema}.{dataset_object.config.target_table}"
                    )

                    # Send data to Snowflake
                    DataframeToSnowflake(
                        target_database=dataset_object.config.target_database,
                        target_schema=dataset_object.config.target_schema,
                        target_table=dataset_object.config.target_table,
                        primary_key=dataset_object.config.primary_key,
                        dataframe=table_df,
                        include_metrics=True,
                        source_data=source_data,
                        operation=data_load_operation,
                    )
                    log.info(f"Successfully ingested data from {row.DAT_RAW_KEY} into Snowflake.")

                    # Mark the file as ingested
                    files_df.at[idx, "SNOWFLAKE_INGESTED"] = True
                    send_to_dfm_table = True

                except Exception as e:
                    log.error(f"Failed to ingest {row.DAT_RAW_KEY} into Snowflake: {e}")
                    if "Length mismatch" in e:
                        log.debug(table_df.columns)
                    raise RuntimeError(f"Error ingesting file into Snowflake: {e}")

        # Update the data management table if any file was ingested
        if send_to_dfm_table:
            try:
                DataframeToSnowflake(
                    target_database=config.datafile_management_db_name,
                    target_schema=config.datafile_management_schema_name,
                    target_table=config.datafile_management_table_name,
                    primary_key="RECORD_ID",
                    dataframe=files_df,
                    operation="upsert",
                )
                log.info("Successfully updated the datafile management table.")
            except Exception as e:
                log.error(f"Failed to update the datafile management table: {e}")
                raise RuntimeError(f"Error updating the management table: {e}")
    else:
        log.info(f"Dataset: {dataset_object.config.target_table} is disabled")
