import yaml
import os
import logging
import math
import pendulum
from common_library.common_snowflake import DataframeToSnowflake, ExecuteQuery, ExecuteSQLtoPandas
from common_library.common_aws import UploadFileToS3
import common_library.common_config as config
import traceback
from snowflake.connector.errors import ProgrammingError
from pandas import DataFrame

# Set up logging
logger = logging.getLogger(__name__)


class ReportDestination:
    """
    Represents the destination where a generated report will be formatted and submitted.

    Attributes:
    ----------
    description : str
        A human-readable description of the report destination.

    formatter : ReportFormatter
        Defines the formatting rules for the report (e.g., CSV, Excel).

    submitter : ReportSubmitter
        Specifies how the report should be submitted (e.g., SFTP, AWS Transfer).

    Functionality:
    --------------
    - Stores information about where and how the report should be delivered.
    - Utilizes `ReportFormatter` to format the report before submission.
    - Uses `ReportSubmitter` to send the report via different methods (e.g., SFTP, AWS Transfer).
    """

    def __init__(self, destination_config) -> None:
        self.description = destination_config.get("description", None)
        self.formatter = ReportFormatter(destination_config.get("formatter", None))
        self.submitter = ReportSubmitter(destination_config.get("submitter", None))

    def __str__(self) -> str:
        return f"ReportDestination(description={self.description},\n formatter={self.formatter},\n submitter={self.submitter})"


class ReportFormatter:
    """
    Represents formatting options for reports before submission.

    Attributes:
    ----------
    type : str
        The report format type (e.g., 'csv', 'excel', 'bcbst_copay').

    filename_format : str
        The filename template for the generated report.

    restate_filename_format : str
        Alternative filename format used for restated reports.

    fix_filename_format : str
        Alternative filename format used for fixed reports.

    carboncopy_path : str
        The directory where a copy of the report should be saved.

    enabled : bool
        Indicates whether this formatter is active.

    args : dict
        Additional arguments for the formatter.

    Functionality:
    --------------
    - Defines the output format and naming conventions for reports.
    - Used by the `generate_report` function to format the output before submission.
    """

    def __init__(self, formatter_config) -> None:
        self.type = formatter_config.get("type", None)
        self.filename_format = formatter_config.get("filename_format", None)
        self.restate_filename_format = formatter_config.get("restate_filename_format", None)
        self.fix_filename_format = formatter_config.get("fix_filename_format", None)
        self.carboncopy_path = formatter_config.get("carboncopy_path", None)
        self.enabled = formatter_config.get("enabled", None)
        self.args = formatter_config.get("args", None)

    def __str__(self) -> str:
        return f"ReportFormatter(type={self.type},\n filename_format={self.filename_format},\n fix_filename_format={self.fix_filename_format},\n restate_filename_format={self.restate_filename_format},\n carboncopy_path={self.carboncopy_path},\n enabled={self.enabled},\n args={self.args})"


class ReportSubmitter:
    """
    Defines how a report is submitted after formatting.

    Attributes:
    ----------
    type : str
        The submission type (e.g., 'sftp', 'aws_transfer', 'sharepoint', 'mock').

    enabled : bool
        Determines whether the submission method is active.

    args : dict
        Additional parameters required for submission.

    Functionality:
    --------------
    - Handles report delivery via various submission methods.
    - Works with `generate_report` to ensure reports are delivered to the correct location.
    """

    def __init__(self, submitter_config) -> None:
        self.type = submitter_config.get("type", None)
        self.enabled = submitter_config.get("enabled", None)
        self.args = submitter_config.get("args", {})

    def __str__(self) -> str:
        return f"ReportSubmitter(type={self.type},\n enabled={self.enabled},\n args={self.args})"


class ReportConfig:
    """
    Handles configuration settings for a report, including retrieval, formatting, and submission.

    Attributes:
    ----------
    enabled : bool
        Determines if the report is active.

    recipient : str
        The recipient of the report.

    agreement_name : str
        The agreement name linked to the report.

    frequency : str
        The frequency of the report (e.g., 'daily', 'weekly', 'monthly').

    schedule : str
        The cron schedule for the report execution.

    database : str
        The Snowflake database containing report data.

    target_schema : str
        The schema where the report data is stored.

    target_table : str
        The table where report data is extracted from.

    remediation_type : str
        The type of remediation process used for failed reports.

    remediation_location : str
        The location of remediation logs.

    query_overrides : dict
        A dictionary of query parameters that can be overridden.

    destinations : list
        A list of `ReportDestination` objects defining where the report should be sent.

    Functionality:
    --------------
    - Loads report configuration from YAML and SQL files.
    - Defines how reports are retrieved, formatted, and submitted.
    - Supports frequency-based scheduling and query overrides.
    """

    def __init__(self, report_yaml_path: str, report_sql_path: str, report_ddl_path: str = None) -> None:
        """
        Initializes the ReportConfig object by loading YAML and SQL files.

        Args:
            report_path (str): Path to the report configuration directory.
        """

        with open(report_yaml_path, "r") as f:
            config_data = yaml.safe_load(f)

        self.enabled = config_data.get("enabled", False)
        self.recipient = config_data.get("recipient", None)
        self.agreement_name = config_data.get("agreement_name", None)
        self.frequency = config_data.get("frequency", None)
        self.start_of_week = config_data.get("start_of_week", None)
        self.boundary_date = config_data.get("boundary_date", None)
        self.schedule = config_data.get("schedule", None)
        self.database = config_data.get("database", None)
        self.target_schema = config_data.get("target_schema", None)
        self.target_table = config_data.get("target_table", None)
        self.remediation_type = config_data.get("remediation_type", None)
        self.remediation_location = config_data.get("remediation_location", None)
        self.mfr_reference_id = config_data.get("mfr_reference_id", None)
        self.query_overrides = config_data.get("query_overrides", {})
        self.destination_configs = config_data.get("destinations", {})
        if self.destination_configs is not None:
            self.destinations = [ReportDestination(x) for x in self.destination_configs]

        with open(report_sql_path, "r") as f:
            self.sql_query = f.read()

        if report_ddl_path:
            with open(report_ddl_path, "r") as f:
                self.target_table_ddl = f.read()

    def __repr__(self) -> str:
        return f"ReportConfig(enabled={self.enabled}, recipient={self.recipient}, agreement_name={self.agreement_name}, frequency={self.frequency})"


def gather_reports(parent_path, include_disabled: bool = False):
    """
    Retrieves and loads all available report configurations.

    Parameters:
    ----------
    parent_path : str
        The directory containing report configuration files.

    include_disabled : bool, optional
        If True, includes reports that are disabled.

    Functionality:
    --------------
    - Scans directories for report configurations.
    - Loads and returns active reports.
    - If `include_disabled=True`, also returns reports marked as inactive.

    Returns:
    -------
    list
        A list of `ReportConfig` objects.
    """

    report_objects = []
    # Walk through the directory recursively
    for root, dirs, files in os.walk(parent_path):
        yaml_file = None
        sql_file = None
        ddl_file = None

        # Check for the presence of .yaml and .sql files in the current directory
        for file in files:
            if file == ("report_config.yaml"):
                yaml_file = os.path.join(root, file)
            elif file == ("query.sql"):
                sql_file = os.path.join(root, file)
            elif file == ("table_ddl.sql"):
                ddl_file = os.path.join(root, file)

        # If both YAML and SQL files are found, add them to the result
        if yaml_file and sql_file:
            if ddl_file:
                report_config = ReportConfig(yaml_file, sql_file, ddl_file)
            else:
                report_config = ReportConfig(yaml_file, sql_file, None)

            if report_config.enabled or include_disabled:
                report_objects.append(report_config)

    return report_objects


def generate_report(
    report_object: ReportConfig,
    destination: ReportDestination,
    override_start_date: str = None,
    override_end_date: str = None,
    override_file_date: str = None,
) -> str:
    """
    Generates a report based on the ReportConfig and uploads it to S3.

    Parameters:
    ----------
    report_object : ReportConfig
        The configuration object containing report details.

    destination : ReportDestination
        The report destination details.

    override_start_date : str, optional
        Custom start date for the report.

    override_end_date : str, optional
        Custom end date for the report.

    override_file_date : str, optional
        Custom file generation date.

    Functionality:
    --------------
    - Retrieves report data from Snowflake.
    - Adjusts the report's time frame based on scheduling frequency.
    - Formats the report in the specified file format (CSV, Excel, etc.).
    - Uploads the generated report to the designated S3 path.
    - Handles errors related to data retrieval, formatting, or submission.

    Returns:
    -------
    str
        The generated report filename.
    """

    pendulum.week_starts_at(getattr(pendulum, report_object.start_of_week, None))
    if override_file_date is None:
        today = pendulum.today()
    else:
        today = pendulum.parse(override_file_date)

    # Logic for handling different frequencies
    if override_start_date is None and override_end_date is None:
        # Handle frequency-specific logic as before
        if report_object.frequency == "sl-half":
            if today.month in [4, 5, 6, 7, 8, 9]:
                last_year = today.subtract(years=1).year
                start_date = pendulum.datetime(last_year, 10, 1)
                end_date = pendulum.datetime(today.year, 3, 31)
            else:
                start_date = pendulum.datetime(today.year, 4, 1)
                end_date = pendulum.datetime(today.year, 9, 30)
        elif report_object.frequency == "quarter":
            target_date = today.subtract(months=1)
            start_date = target_date.first_of(report_object.frequency)
            end_date = target_date.last_of(report_object.frequency)
        elif report_object.frequency == "month":
            target_date = today.subtract(months=1)
            start_date = target_date.start_of(report_object.frequency)
            end_date = target_date.end_of(report_object.frequency)
        elif report_object.frequency == "week":
            target_date = today.subtract(weeks=1)
            start_date = target_date.start_of(report_object.frequency)
            end_date = target_date.end_of(report_object.frequency)
        elif report_object.frequency == "day":
            if today.day_of_week == pendulum.MONDAY:
                start_date = today.subtract(days=3).start_of(report_object.frequency)
                end_date = today.subtract(days=1).end_of(report_object.frequency)
            else:
                target_date = today.subtract(days=1)
                start_date = target_date.start_of(report_object.frequency)
                end_date = target_date.end_of(report_object.frequency)
        else:
            start_date = today.start_of(report_object.frequency)
            end_date = today.end_of(report_object.frequency)
    else:
        start_date = pendulum.parse(override_start_date)
        end_date = pendulum.parse(override_end_date)

    query_tokens = {}
    if report_object.enabled:
        if report_object.query_overrides:
            for override in report_object.query_overrides:
                query_tokens.update(override)

        query_tokens["%DATESTART%"] = f"'{start_date}'"
        query_tokens["%DATEEND%"] = f"'{end_date}'"

        final_sql_query = report_object.sql_query
        for token, value in query_tokens.items():
            final_sql_query = final_sql_query.replace(token, value)

        report_df = ExecuteSQLtoPandas(
            sql_query=final_sql_query,
            database="STAGED_DATA",
            schema="PUBLIC",
        )

        try:
            if report_object.frequency == "quarter":
                quarter_num = math.ceil(target_date.month / 3.0)
                formatted_filename = today.strftime(destination.formatter.filename_format).replace("%QUARTERNUM%", str(quarter_num))
            elif report_object.frequency == "month":
                formatted_filename = (
                    today.strftime(destination.formatter.filename_format) if report_object.boundary_date != "beginning" else start_date.strftime(destination.formatter.filename_format)
                )
            elif report_object.frequency == "week":
                formatted_filename = (
                    start_date.strftime(destination.formatter.filename_format) if report_object.boundary_date == "beginning" else end_date.strftime(destination.formatter.filename_format)
                )
            else:
                formatted_filename = today.strftime(destination.formatter.filename_format)

            logger.info(destination)

            if destination.formatter.type == "csv":
                from common_library.Reporting.formatters import format_csv

                format_csv(df=report_df, filename=formatted_filename, destination=destination)
                logger.info(f"Report: {report_object.agreement_name} for {report_object.recipient} successfully generated as {formatted_filename}.")
            elif destination.formatter.type == "excel":
                from common_library.Reporting.formatters import format_excel

                format_excel(df=report_df, filename=formatted_filename, destination=destination)
                logger.info(f"Report: {report_object.agreement_name} for {report_object.recipient} successfully generated as {formatted_filename}.")
            elif destination.formatter.type == "bcbst_copay":
                from common_library.Reporting.formatters import format_bcbst_copay

                format_bcbst_copay(df=report_df, filename=formatted_filename, destination=destination)
                logger.info(f"Report: {report_object.agreement_name} for {report_object.recipient} successfully generated as {formatted_filename}.")
            else:
                logger.error(f"Unknown formatter supplied: {destination.formatter.type}")

            UploadFileToS3(
                local_file=os.path.join(config.working_directory, formatted_filename),
                bucket=config.reporting_s3_bucket,
                object_key=destination.formatter.carboncopy_path.lower() + formatted_filename,
            )
        except Exception as e:
            logger.error(traceback.format_exc())
            raise e

    else:
        logger.info(f"Report: {report_object.agreement_name} for {report_object.recipient} is disabled.")

    return formatted_filename


def submit_report(
    report_object: ReportConfig,
    destination: ReportDestination,
    output_filename: str,
    global_skip: str,
    report_run_id: str = None,
):
    """
    Submits a generated report to the specified destination.

    Parameters:
    ----------
    report_object : ReportConfig
        The report configuration object.

    destination : ReportDestination
        The report destination details.

    output_filename : str
        The name of the generated report file.

    global_skip : str
        Determines whether report submission should be skipped.

    report_run_id : str, optional
        The unique identifier for the report execution.

    Functionality:
    --------------
    - Determines the appropriate submission method (SFTP, AWS Transfer, SharePoint, etc.).
    - Uploads the report to the designated location.
    - Logs the submission response and updates metadata.

    Returns:
    -------
    None
    """

    if hasattr(destination, "submitter"):
        logger.info(f"Global Skip is set to: {global_skip}")
        if report_object.enabled and destination.submitter.enabled and global_skip != "true":
            if destination.submitter.type != "none" or destination.submitter.type is not None:
                s3_file_key = f"/{config.reporting_s3_bucket}/{destination.formatter.carboncopy_path.lower()}{output_filename}"

                if destination.submitter.type == "aws_transfer":
                    from common_library.Reporting.submitters import submit_aws_transfer

                    response = submit_aws_transfer(s3_file_key=s3_file_key, submitter_args=destination.submitter.args)
                    logger.info(f"Report: {report_object.agreement_name} for {report_object.recipient} successfully submitted. Response: {response}")
                elif destination.submitter.type == "sftp":
                    from common_library.Reporting.submitters import submit_sftp

                    response = submit_sftp(s3_file_key=s3_file_key, submitter_args=destination.submitter.args)
                    logger.info(f"Report: {report_object.agreement_name} for {report_object.recipient} successfully submitted. Response: {response}")
                elif destination.submitter.type == "sharepoint":
                    from common_library.Reporting.submitters import submit_sharepoint

                    response = submit_sharepoint(s3_file_key=s3_file_key, submitter_args=destination.submitter.args)
                    logger.info(f"Report: {report_object.agreement_name} for {report_object.recipient} successfully submitted. Response: {response}")
                elif destination.submitter.type == "mock":
                    from common_library.Reporting.submitters import submit_mock

                    response = submit_mock(s3_file_key=s3_file_key, submitter_args=destination.submitter.args)
                    logger.info(f"Report: {report_object.agreement_name} for {report_object.recipient} successfully submitted. Response: {response}")
                else:
                    logger.error(f"Unknown Submitter Type: {destination.submitter.type}")

                logger.info(response)
                if report_run_id:
                    logger.info("Updating metatdata record into FILE_EXPORTS table")
                    update_query = f"UPDATE {config.reporting_database}.PIPELINE.FILE_EXPORTS SET SUBMITTED_DATE = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) WHERE REPORT_RUN_ID = '{report_run_id}'"
                    ExecuteQuery(update_query, config.reporting_database, "PIPELINE")
            else:
                logger.info(f"No submitter configured for {report_object.recipient}-{report_object.agreement_name}")
        else:
            logger.info(f"Submitter disabled for {report_object.recipient}-{report_object.agreement_name}")
    else:
        logger.info(f"No submitters configured for {report_object.recipient}-{report_object.agreement_name}")


def extract_report_data(
    report_object: ReportConfig,
    report_run_id: str,
    report_start_date: str = None,
    report_end_date: str = None,
):
    """
    Extracts data from Snowflake based on the report configuration and stores it in a dedicated table.

    Parameters:
    ----------
    report_object : ReportConfig
        The configuration object containing report details.

    report_run_id : str
        The unique identifier for the report execution.

    report_start_date : str, optional
        Custom start date for the report extraction.

    report_end_date : str, optional
        Custom end date for the report extraction.

    Functionality:
    --------------
    - Determines the report time frame based on frequency settings.
    - Queries the report's source database and extracts data based on the configured query.
    - Checks if data for the given period already exists to avoid duplicate extractions.
    - If necessary, creates the target schema and table in Snowflake.
    - Loads the extracted data into a Snowflake table for further processing.

    Returns:
    -------
    None

    Raises:
    -------
    RuntimeError
        If the query execution or data insertion into Snowflake fails.
    """

    pendulum.week_starts_at(getattr(pendulum, report_object.start_of_week, None))
    if report_start_date is None:
        if report_object.frequency == "hour":
            today = pendulum.now()
        else:
            today = pendulum.today()
    else:
        today = pendulum.from_format(report_start_date, "YYYY-MM-DDTHH:mm:ss")

    # Logic for handling different frequencies
    if report_start_date is None and report_end_date is None:
        target_date, start_date, end_date = parse_report_frequency(report_object=report_object, todays_date=today)
    else:
        start_date = report_start_date
        end_date = report_end_date

    ddl_tokens = {}
    ddl_tokens["%TARGET_DATABASE%"] = config.reporting_database
    ddl_tokens["%TARGET_SCHEMA%"] = report_object.target_schema
    ddl_tokens["%TARGET_TABLE%"] = report_object.target_table

    final_table_ddl = report_object.target_table_ddl
    for token, value in ddl_tokens.items():
        final_table_ddl = final_table_ddl.replace(token, value)

    # Query the database to make sure this data hasn't already been extracted
    report_table_fqdn = f"{config.reporting_database}.{report_object.target_schema}.{report_object.target_table}"
    data_check_query = f"""
SELECT COUNT(*) as total_records
FROM {report_table_fqdn}
WHERE REPORT_START_DATE = '{start_date}'
AND REPORT_END_DATE = '{end_date}'
"""
    try:
        check_results = ExecuteQuery(
            sql_query=data_check_query,
            database=config.reporting_database,
            schema=report_object.target_schema,
        )[0][0]
    except ProgrammingError as db_ex:
        if db_ex.errno == 2003 and db_ex.sqlstate == "02000":
            logger.info("Schema does not exist, executing create SCHEMA and TABLE statements")
            ExecuteQuery(
                sql_query=f"CREATE SCHEMA {config.reporting_database}.{report_object.target_schema};",
                database=config.reporting_database,
                schema=report_object.target_schema,
            )
            ExecuteQuery(
                sql_query=final_table_ddl,
                database=config.reporting_database,
                schema=report_object.target_schema,
            )
            check_results = 0
        if db_ex.errno == 2003 and db_ex.sqlstate == "42S02":
            logger.info("Table does not exist, executing create statement")
            ExecuteQuery(
                sql_query=final_table_ddl,
                database=config.reporting_database,
                schema=report_object.target_schema,
            )
            check_results = 0
        else:
            logger.error(
                f"SQL Execution Error: {db_ex.msg} \n Snowflake Query Id: {db_ex.sfqid} \n Error Number: {db_ex.errno} \n SQL State: {db_ex.sqlstate} \n Error Type: {type(db_ex)}"
            )

    if check_results > 0:
        logger.warning(f"{report_table_fqdn} already has data for Start Date: {start_date} | End Date: {end_date}.  Skipping Data Extraction.")
    else:
        query_tokens = {}
        if report_object.enabled:
            if report_object.query_overrides:
                for override in report_object.query_overrides:
                    query_tokens.update(override)

            query_tokens["%DATESTART%"] = f"'{start_date}'"
            query_tokens["%DATEEND%"] = f"'{end_date}'"
            query_tokens["%REPORT_RUN_ID%"] = f"'{report_run_id}'"

            final_sql_query = report_object.sql_query
            for token, value in query_tokens.items():
                logger.info(f"Replacing Token: {token} with value {value}")
                final_sql_query = final_sql_query.replace(token, value)

            report_df = ExecuteQuery(
                sql_query=final_sql_query,
                database="STAGED_DATA",
                schema="PUBLIC",
                as_pandas=True,
            )

            DataframeToSnowflake(
                target_database=config.reporting_database,
                target_schema=report_object.target_schema,
                target_table=report_object.target_table,
                primary_key=None,
                dataframe=report_df,
                operation="append",
                include_metrics=True,
                target_ddl=final_table_ddl,
            )


def parse_report_frequency(report_object: ReportConfig, todays_date: pendulum.DateTime):
    """
    Determines the start and end dates for report extraction based on the report frequency.

    Parameters:
    ----------
    report_object : ReportConfig
        The configuration object containing report scheduling details.

    todays_date : pendulum.DateTime
        The reference date used to determine the report's time range.

    Functionality:
    --------------
    - Computes the reporting period based on the configured frequency.
    - Supports various frequencies:
        - `"sl-half"`: Six-month periods (April-September, October-March).
        - `"quarter"`: Calendar quarters (Q1, Q2, Q3, Q4).
        - `"month"`: Previous calendar month.
        - `"week"`: Previous week based on the configured start-of-week setting.
        - `"day"`: Previous day, with adjustments for weekends.

    Returns:
    -------
    tuple[pd.DateTime, pd.DateTime, pd.DateTime]
        A tuple containing:
        - `target_date`: The target date for the report.
        - `start_date`: The computed start date for the report extraction.
        - `end_date`: The computed end date for the report extraction.

    Example:
    --------
    ```python
    target_date, start_date, end_date = parse_report_frequency(report_object, pendulum.today())
    print(f"Extracting data from {start_date} to {end_date}")
    ```
    """

    if report_object.frequency == "sl-half":
        if todays_date.month in [4, 5, 6, 7, 8, 9]:
            last_year = todays_date.subtract(years=1).year
            start_date = pendulum.datetime(last_year, 10, 1)
            end_date = pendulum.datetime(todays_date.year, 3, 31)
            target_date = start_date.start_of("day")
        else:
            start_date = pendulum.datetime(todays_date.year, 4, 1)
            end_date = pendulum.datetime(todays_date.year, 9, 30)
            target_date = start_date.start_of("day")
    elif report_object.frequency == "quarter":
        target_date = todays_date.subtract(months=1)
        start_date = target_date.first_of(report_object.frequency)
        end_date = target_date.last_of(report_object.frequency)
    elif report_object.frequency == "month":
        target_date = todays_date.subtract(months=1)
        start_date = target_date.start_of(report_object.frequency)
        end_date = target_date.end_of(report_object.frequency)
    elif report_object.frequency == "week":
        target_date = todays_date.subtract(weeks=1)
        start_date = target_date.start_of(report_object.frequency)
        end_date = target_date.end_of(report_object.frequency)
    elif report_object.frequency == "day":
        if todays_date.day_of_week == pendulum.MONDAY:
            start_date = todays_date.subtract(days=3).start_of(report_object.frequency)
            end_date = todays_date.subtract(days=1).end_of(report_object.frequency)
            target_date = todays_date.start_of(report_object.frequency)
        else:
            target_date = todays_date.subtract(days=1)
            start_date = target_date.start_of(report_object.frequency)
            end_date = target_date.end_of(report_object.frequency)
    elif report_object.frequency == "hour":
        # I want to intelligently determine the start and end hour based on our "business hours" model 7am-7pm est
        # If the current hour is 7am I want to report starting at 8pm the previous day
        # If the current hour is 8am-7pm, I want to report on the previous hour

        if todays_date.hour == (config.business_hour_start + 1):  # Add 1 to account for an hour lag in data availability
            start_date = todays_date.subtract(hours=13).start_of(report_object.frequency)
            end_date = todays_date.subtract(hours=2).end_of(report_object.frequency)
            target_date = todays_date.subtract(hours=13).start_of(report_object.frequency)
        else:
            start_date = todays_date.subtract(hours=2).start_of(report_object.frequency)
            end_date = todays_date.subtract(hours=2).end_of(report_object.frequency)
            target_date = todays_date.start_of(report_object.frequency)
    else:
        start_date = todays_date.start_of(report_object.frequency)
        end_date = todays_date.end_of(report_object.frequency)
        target_date = todays_date.start_of(report_object.frequency)

    return target_date, start_date, end_date


def format_dataframe(
    report_object: ReportConfig, report_dataframe: DataFrame, report_destination: ReportDestination, target_date, start_date, end_date, todays_date, is_restatement: bool = False
):
    """
    Formats a report DataFrame according to the specified formatting rules.

    Parameters:
    ----------
    report_object : ReportConfig
        The configuration object containing report details.

    report_dataframe : DataFrame
        The Pandas DataFrame containing the extracted report data.

    report_destination : ReportDestination
        The destination configuration specifying formatting rules.

    target_date : datetime
        The reference date used for filename formatting.

    start_date : datetime
        The computed start date for the report extraction.

    end_date : datetime
        The computed end date for the report extraction.

    todays_date : datetime
        The current date, used for filename generation.

    is_restatement : bool, optional
        If True, appends `_RESTATE` to the filename.

    Functionality:
    --------------
    - Generates a formatted filename based on the report frequency and destination rules.
    - Applies date-based formatting logic (e.g., quarterly reports include the quarter number).
    - Supports appending "_RESTATE" to filenames when applicable.
    - Calls appropriate formatting functions (`CSV`, `Excel`, `BCBST Copay`) based on report type.
    - Returns the fully formatted filename for the report.

    Returns:
    -------
    str
        The generated report filename.

    Raises:
    -------
    ValueError
        If an unknown report format is specified.

    Example:
    --------
    ```python
    formatted_filename = format_dataframe(report_object, report_dataframe, report_destination, target_date, start_date, end_date, todays_date)
    print(f"Generated report: {formatted_filename}")
    ```
    """

    if report_object.frequency == "quarter":
        quarter_num = math.ceil(target_date.month / 3.0)
        formatted_filename = todays_date.strftime(report_destination.formatter.filename_format).replace("%QUARTERNUM%", str(quarter_num))
    elif report_object.frequency == "month":
        formatted_filename = (
            todays_date.strftime(report_destination.formatter.filename_format)
            if report_object.boundary_date != "beginning"
            else start_date.strftime(report_destination.formatter.filename_format)
        )
    elif report_object.frequency == "week":
        formatted_filename = (
            start_date.strftime(report_destination.formatter.filename_format)
            if report_object.boundary_date == "beginning"
            else end_date.strftime(report_destination.formatter.filename_format)
        )
    else:
        formatted_filename = todays_date.strftime(report_destination.formatter.filename_format)

    if is_restatement:
        split_formatted_filename = formatted_filename.split(".")
        formatted_filename = split_formatted_filename[0] + "_RESTATE" + split_formatted_filename[1]

    if report_destination.formatter.type == "csv":
        from common_library.Reporting.formatters import format_csv

        format_csv(df=report_dataframe, filename=formatted_filename, destination=report_destination)
        logger.info(f"Report: {report_object.agreement_name} for {report_object.recipient} successfully generated.")
    elif report_destination.formatter.type == "excel":
        from common_library.Reporting.formatters import format_excel

        format_excel(df=report_dataframe, filename=formatted_filename, destination=report_destination)
        logger.info(f"Report: {report_object.agreement_name} for {report_object.recipient} successfully generated.")
    elif report_destination.formatter.type == "bcbst_copay":
        from common_library.Reporting.formatters import format_bcbst_copay

        format_bcbst_copay(df=report_dataframe, filename=formatted_filename, destination=report_destination)
        logger.info(f"Report: {report_object.agreement_name} for {report_object.recipient} successfully generated.")
    else:
        logger.error(f"Unknown formatter supplied: {report_destination.formatter.type}")

    return formatted_filename


def generate_report_v2(
    report_object: ReportConfig,
    destination: ReportDestination,
    report_run_id: str,
    override_start_date: str = None,
    override_end_date: str = None,
    override_file_date: str = None,
    is_restatement: bool = False,
) -> str:
    """
    Generates a report, formats it, and uploads it to S3.

    Parameters:
    ----------
    report_object : ReportConfig
        The configuration object containing report details.

    destination : ReportDestination
        The destination configuration specifying formatting and submission details.

    report_run_id : str
        The unique identifier for the report execution.

    override_start_date : str, optional
        Custom start date for report extraction.

    override_end_date : str, optional
        Custom end date for report extraction.

    override_file_date : str, optional
        Custom file generation date.

    is_restatement : bool, optional
        If True, appends `_RESTATE` to the filename.

    Functionality:
    --------------
    - Determines the report's date range using `parse_report_frequency`.
    - Extracts relevant report data from the Snowflake database.
    - Formats the extracted data according to the specified format (CSV, Excel, etc.).
    - Uploads the formatted file to an S3 bucket.
    - Logs the report generation process for tracking and debugging.
    - Stores the report file metadata in the `FILE_EXPORTS` table for tracking.

    Returns:
    -------
    str
        The generated report filename.

    Raises:
    -------
    RuntimeError
        If the report extraction, formatting, or upload process fails.

    Example:
    --------
    ```python
    report_filename = generate_report_v2(report_object, destination, report_run_id)
    print(f"Report successfully generated: {report_filename}")
    ```
    """

    pendulum.week_starts_at(getattr(pendulum, report_object.start_of_week, None))
    if override_file_date is None:
        if report_object.frequency == "hour":
            today = pendulum.now()
        else:
            today = pendulum.today()
    else:
        today = pendulum.parse(override_file_date)

    # Logic for handling different frequencies
    if override_start_date is None and override_end_date is None:
        target_date, start_date, end_date = parse_report_frequency(report_object=report_object, todays_date=today)
    else:
        target_date = pendulum.parse(override_start_date)
        start_date = pendulum.parse(override_start_date)
        end_date = pendulum.parse(override_end_date)

    report_table_fqdn = f"{config.reporting_database}.{report_object.target_schema}.{report_object.target_table}"

    report_data_query = f"""
SELECT *
FROM {report_table_fqdn}
WHERE REPORT_START_DATE = '{start_date}'
AND REPORT_END_DATE = '{end_date}'
"""

    # logger.info(report_data_query)
    if report_object.enabled:
        report_df = ExecuteQuery(
            sql_query=report_data_query,
            database="STAGED_DATA",
            schema="PUBLIC",
            as_pandas=True,
        )

        # DECOMMED 2/26/2025: Insert record and report ids to cross reference table
        # xref_data = report_df[["REPORT_RUN_ID", "REPORT_RECORD_ID"]]
        # DataframeToSnowflake(
        #     target_database=config.reporting_database, target_schema="PIPELINE", target_table="REPORTS_TO_RECORDS", dataframe=xref_data, operation="append", primary_key=None
        # )

        # Drop Metadata columns from dataframe before exporting to a file
        columns_to_drop = [column for prefix in config.report_exclude_column_prefixes for column in report_df.columns if column.startswith(prefix)]

        report_df = report_df.drop(columns=columns_to_drop)

        try:
            formatted_filename = format_dataframe(
                report_object=report_object,
                report_dataframe=report_df,
                report_destination=destination,
                target_date=target_date,
                start_date=start_date,
                end_date=end_date,
                todays_date=today,
            )

            UploadFileToS3(
                local_file=os.path.join(config.working_directory, formatted_filename),
                bucket=config.reporting_s3_bucket,
                object_key=destination.formatter.carboncopy_path.lower() + formatted_filename,
            )

            # Insert record into FILE_EXPORTS table
            file_export_insert = f"""
            INSERT INTO {config.reporting_database}.PIPELINE.FILE_EXPORTS (REPORT_DATABASE, REPORT_SCHEMA, REPORT_TABLE, REPORT_RUN_ID, EXPORTED_FILE_NAME, EXPORTED_DATE)
            SELECT '{config.reporting_database}', '{report_object.target_schema}', '{report_object.target_table}', '{report_run_id}', '{formatted_filename}', TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP())
            """
            ExecuteQuery(file_export_insert, config.reporting_database, "PIPELINE")
        except Exception as e:
            logger.error(e)
            raise e

    else:
        logger.info(f"Report: {report_object.agreement_name} for {report_object.recipient} is disabled.")

    return formatted_filename
