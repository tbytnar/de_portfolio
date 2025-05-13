from datetime import datetime
import logging
import pandas as pd
from sqlalchemy import create_engine
import common_library.common_config as config  # noqa: F401
from common_library.common_aws import UploadFileToS3
from common_library.common_snowflake import ExecuteQuery, GetConnectionURL
import os

log = logging.getLogger(__name__)
local_cache = "/tmp/"


def ExportFive9List(
    file_name: str,
    sql_query: str,
    source_database: str,
    source_schema: str,
    target_s3_key: str,
    list_name: str,
):
    """
    Extracts data from a Snowflake database, saves it as a CSV, and uploads it to S3 for Five9.

    Parameters:
    ----------
    file_name : str
        The name of the CSV file to be generated.

    sql_query : str
        The SQL query to execute for retrieving data from Snowflake.

    source_database : str
        The name of the database in Snowflake where the query should be executed.

    source_schema : str
        The schema within the database containing the target data.

    target_s3_key : str
        The S3 key (folder path) where the file should be uploaded.

    list_name : str
        The name of the Five9 list being created.

    Functionality:
    --------------
    - Executes the provided SQL query against Snowflake.
    - Fetches the results and stores them in a Pandas DataFrame.
    - Saves the DataFrame as a CSV file in the local `/tmp/` directory.
    - Uploads the generated file to the specified S3 bucket and key.
    - If the dataset contains specific identifiers (`referral_id`, `order_id`, `rx_number`), creates a subset for Five9.
    - Stores Five9 data in a Snowflake patient history table for tracking.
    - Ensures database operations are executed efficiently, with error handling for failures.

    Returns:
    -------
    None

    Raises:
    -------
    RuntimeError
        If uploading to Snowflake fails or if the query execution fails.

    Example:
    --------
    ```python
    ExportFive9List(
        file_name="five9_patients.csv",
        sql_query="SELECT * FROM patient_data WHERE status='active'",
        source_database="HEALTH_DB",
        source_schema="PUBLIC",
        target_s3_key="five9/lists",
        list_name="ActivePatients"
    )
    ```
    """

    five9_list_df: pd.DataFrame = ExecuteQuery(sql_query, source_database, source_schema, as_pandas=True)

    five9_list_df.columns = [x.lower() for x in five9_list_df.columns]

    local_file_path = os.path.join(local_cache, file_name)

    five9_list_df.to_csv(local_file_path, sep=",", header=True, index=False)
    UploadFileToS3(
        local_file=local_file_path,
        bucket=config.reporting_s3_bucket,
        object_key=f"{target_s3_key}/{file_name}",
    )

    if len(five9_list_df) > 0:
        search_set = set(["referral_id", "order_id", "rx_number"])
        if search_set.issubset(five9_list_df.columns):
            new_pdf = pd.DataFrame(five9_list_df[["person_id", "referral_id", "order_id", "rx_number"]])
            new_pdf = new_pdf.rename(columns={"person_id": "PATIENT_ID"})
            new_pdf = new_pdf.rename(columns={"referral_id": "REFERRAL_ID"})
            new_pdf = new_pdf.rename(columns={"rx_number": "RX_NUMBER"})
            new_pdf = new_pdf.rename(columns={"order_id": "ORDER_ID"})
        else:
            new_pdf = pd.DataFrame(five9_list_df["person_id"])
            new_pdf = new_pdf.rename(columns={"person_id": "PATIENT_ID"})

        new_pdf["LIST_NAME"] = list_name
        new_pdf["DATE_ADDED"] = str(datetime.now())

        # Log first three lines of dataframe for debugging purposes
        log.info(new_pdf.head(3))

        # Create connection URL
        sf_url = GetConnectionURL(database=config.five9_database, schema=config.five9_patient_history_schema_name)

        # Create the SQLAlchemy engine
        engine = create_engine(sf_url)

        # Use a context manager for database connection
        with engine.connect() as conn:
            try:
                # Upload DataFrame to Patient History Table
                new_pdf.to_sql(
                    name=config.five9_patient_history_table_name,
                    con=conn,
                    if_exists="append",  # Replace if the table exists
                    index=False,  # Do not write the DataFrame's index
                    chunksize=1000,  # Use chunking for large datasets
                )
            except Exception as e:
                # Rollback in case of failure
                log.error(f"Failed to update table: {e}")
                raise RuntimeError(f"Error updating table: {e}")
