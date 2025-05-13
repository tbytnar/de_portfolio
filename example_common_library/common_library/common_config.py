from datetime import timedelta, datetime
import logging
import os

log = logging.getLogger(__name__)

working_directory = "/tmp"

# Apache Airflow Configurations
defaultDagArgs = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 18),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": "no_reply@example.com",
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "dagrun_timeout": None,
    "max_active_runs": 1,
    "depends_on_past": True,
    "wait_for_downstream": True,
}

# Time Configurations
business_hour_start = 7
business_hour_end = 19

# Cron Schedules
staging_pipeline_schedule_hourly_during_business_hours = "59 3-19 * * *"  # At 59 minutes past the hour, between 03:59 AM and 07:59 PM
staging_pipeline_schedule_hourly_all_hours = "30 * * * *"  # At 30 minutes past the hour
staging_pipeline_schedule_daily = "59 3 * * *"  # At 3:00 AM


# Environment Based Configurations
if "PRODUCTION" in os.environ:
    # AWS S3 Buckets
    raw_s3_bucket = "raw-useast1-greengrass-prod"
    stage_s3_bucket = "stage-useast1-greengrass-prod"
    reporting_s3_bucket = "reporting-useast1-prod"

    # Snowflake Databases
    reporting_database = "PROD_REPORTS_CORE"
    datafile_management_db_name = "PROD_PIPELINE_CORE"

    # Snowflake Schemas
    pipeline_schema = "METADATA"
    datafile_management_schema_name = "METADATA"
    five9_patient_history_schema_name = "METADATA"

    # Snowflake Tables
    datafile_management_table_name = "DATAFILE_MANAGEMENT"
else:
    # AWS S3 Buckets
    raw_s3_bucket = "raw-useast1-greengrass-test"
    stage_s3_bucket = "stage-useast1-greengrass-test"
    reporting_s3_bucket = "reporting-useast1-test"

    # Snowflake Databases
    reporting_database = "TEST_REPORTS_CORE"
    datafile_management_db_name = "TEST_PIPELINE_CORE"

    # Snowflake Schemas
    pipeline_schema = "METADATA"
    datafile_management_schema_name = "METADATA"
    five9_patient_history_schema_name = "METADATA"

    # Snowflake Tables
    datafile_management_table_name = "DATAFILE_MANAGEMENT_TEST"


# Pandas Datatype Conversion From Dataset Spec
pd_converters_map = {
    "string": str,
    "int": int,
    "float": float,
    "date": str,
    "bool": bool,
    # "timestamp": pd_convert_to_timestamp # 02-21-2024 - This wasn't working
    "timestamp": str,
}

# Metadata columns appended to report records
report_exclude_column_prefixes = ["VENDOR_", ""]
