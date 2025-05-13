import json
import os
import logging
import pandas as pd
import pendulum

log = logging.getLogger(__name__)


# class Config(object):
#     """
#     Represents a dataset provider's configuration.

#     Attributes:
#     ----------
#     provider : str
#         The name of the dataset provider.

#     connection : str
#         The type of connection (e.g., 'S3', 'SFTP', 'Snowflake').

#     schedule : str
#         The dataset execution schedule (e.g., daily, hourly).

#     def_file : str
#         The file path to the dataset's configuration file.

#     concurrency : str
#         The maximum number of concurrent executions allowed.

#     Functionality:
#     --------------
#     - Loads a dataset configuration from a JSON file.
#     - Validates that the provider and connection type in the config match the directory structure.
#     - Stores configuration details for use in data pipelines.

#     Raises:
#     -------
#     BaseException
#         If the provider or connection type does not match the expected values.
#     """

#     provider = str
#     connection = str
#     schedule = str
#     def_file = str
#     concurrency = str

#     def __init__(self, configFile):
#         with open(configFile) as f:
#             configuration = json.load(f)
#             config_connection_type = configuration.get("connection_type")
#             path_connection_type = configFile.split(os.sep)[-2]
#             config_provider = configuration.get("provider")
#             path_provider = configFile.split(os.sep)[-3]

#             if config_connection_type != path_connection_type or config_provider != path_provider:
#                 raise BaseException(f"Invalid Config: {config_provider}:{path_provider} {config_connection_type}{path_connection_type}")
#             else:
#                 self.provider = config_provider
#                 self.connection = config_connection_type
#                 self.schedule = configuration.get("schedule")
#                 self.def_file = configFile
#                 self.concurrency = configuration.get("concurrency")

#     def __str__(self):
#         printStr = f"Config for: {self.provider} - {self.connection}\n"
#         printStr = printStr + f"Definition File: {self.def_file}\n"
#         printStr = printStr + f"Schedule: {self.schedule}\n"
#         printStr = printStr + f"Concurrency: {self.concurrency}\n"
#         return printStr


# class Dataset(object):
#     """
#     Represents a dataset and its metadata, transformation steps, and configurations.

#     Attributes:
#     ----------
#     def_file : str
#         The file path to the dataset definition.

#     provider : str
#         The name of the dataset provider.

#     connection : str
#         The connection type (e.g., 'S3', 'SFTP', 'Snowflake').

#     schema : str
#         The database schema where the dataset resides.

#     table : str
#         The table associated with the dataset.

#     priority : str
#         Indicates if the dataset has high priority processing.

#     s3_metadata : dict
#         Metadata related to S3 storage (e.g., bucket names, keys).

#     sftp_metadata : dict
#         Metadata related to SFTP storage (e.g., credentials, file paths).

#     extraction_steps : dict
#         Steps defining how data is extracted from the source.

#     cleansing_steps : dict
#         Data cleansing steps to be applied before ingestion.

#     conversion_steps : dict
#         Steps for data type conversions and transformations.

#     configuration : dict
#         General dataset configuration, including column definitions.

#     dedupeKeyColumns : list
#         List of columns used for deduplication.

#     datatypeColumns : list
#         List of dataset columns with their expected data types.

#     Functionality:
#     --------------
#     - Reads dataset metadata from a JSON configuration file.
#     - Stores information about data extraction, cleansing, and transformation steps.
#     - Determines deduplication keys and data types from the configuration file.
#     - Used for managing data ingestion and transformation workflows.
#     """

#     def_file = str
#     provider = str
#     connection = str
#     schema = str
#     table = str
#     priority = str
#     s3_metadata = dict()
#     sftp_metadata = dict()
#     extraction_steps = dict()
#     cleansing_steps = dict()
#     conversion_steps = dict()
#     configuration = dict()
#     dedupeKeyColumns = str
#     datatypeColumns = object

#     def __init__(self, datasetFile):
#         self.def_file = datasetFile
#         with open(datasetFile) as f:
#             dataset = json.load(f)
#             self.provider = dataset.get("provider")
#             self.connection = dataset.get("connection_type")
#             self.schema = dataset.get("schema")
#             self.table = dataset.get("table")
#             self.priority = dataset.get("priority")
#             self.s3_metadata = dataset.get("s3_metadata", {})
#             self.sftp_metadata = dataset.get("sftp_metadata", {})
#             self.extraction_steps = dataset.get("extraction_steps", {})
#             self.cleansing_steps = dataset.get("cleansing_steps", {})
#             self.conversion_steps = dataset.get("conversion_steps", {})
#             self.configuration = dataset.get("configuration", {})
#             primary_key = self.configuration.get("primary_key", None)
#             self.dedupeKeyColumns = primary_key.split(",") if primary_key else None
#             self.datatypeColumns = self.configuration.get("columns", None)

#     def __str__(self):
#         printStr = f"Dataset Name: {self.provider} - {self.schema} - {self.table}\n"
#         printStr = printStr + f"Definition File: {self.def_file}\n"
#         printStr = printStr + f"Connection Type: {self.connection}\n"
#         printStr = printStr + f"Priority: {self.priority}\n"
#         printStr = printStr + "S3 Metadata: \n"
#         for item in self.s3_metadata:
#             printStr = printStr + f"    {item} \n"
#         printStr = printStr + "SFTP Metadata: \n"
#         for item in self.sftp_metadata:
#             printStr = printStr + f"    {item} \n"
#         printStr = printStr + "Extraction Steps: \n"
#         for step in self.extraction_steps:
#             printStr = printStr + f"    {step} \n"
#         printStr = printStr + "Cleansing Steps: \n"
#         for step in self.cleansing_steps:
#             printStr = printStr + f"    {step} \n"
#         printStr = printStr + "Conversion Steps: \n"
#         for step in self.conversion_steps:
#             printStr = printStr + f"    {step} \n"
#         printStr = printStr + "Configuration: \n"
#         for item in self.configuration:
#             printStr = printStr + f"    {item} \n"
#         return printStr


# def GetProviderConfigs(assets_path):
#     connection_configs = []
#     for root, dirs, files in os.walk(os.path.join(assets_path, "Datasets"), topdown=False):
#         for name in files:
#             if name == "config.json":
#                 config_file = os.path.join(root, name)
#                 connection_configs.append(Config(config_file))
#     return connection_configs


# def GetDatasetObjects(assets_path, priority_only):
#     dataset_objects = []
#     for root, dirs, files in os.walk(os.path.join(assets_path, "Datasets"), topdown=False):
#         for name in files:
#             if name.endswith(".json") and name != "config.json":
#                 dataset_file = os.path.join(root, name)
#                 dataset = Dataset(dataset_file)
#                 if priority_only:
#                     if dataset.priority.lower() == "true":
#                         dataset_objects.append(Dataset(dataset_file))
#                 else:
#                     dataset_objects.append(Dataset(dataset_file))
#     return dataset_objects


# Helper function to convert nested dictionaries into class objects
# def from_dict(cls, data):
#     """Helper function to convert dictionary to dataclass object."""
#     # Handle lists of objects (like the phone_numbers list)
#     if isinstance(data, dict):
#         field_types = {f.name: f.type for f in cls.__dataclass_fields__.values()}
#         return cls(**{key: from_dict(field_types[key], value) for key, value in data.items()})
#     elif isinstance(data, list):
#         # If it's a list, convert each element in the list to the appropriate class
#         return [from_dict(cls.__args__[0], item) for item in data]
#     else:
#         # Primitive types (int, str, etc.) just get returned as is
#         return data


class DataQualityCheck:
    """
    Represents a data quality rule applied to dataset columns.

    Attributes:
    ----------
    check_rule : str
        The name of the rule (e.g., 'not_null', 'greater_than', 'match_regex').

    values : dict
        Optional parameters for rule validation (e.g., min/max values).

    Functionality:
    --------------
    - Stores validation rules that apply to dataset columns.
    - Can be used to check for missing values, numeric constraints, or regex matches.
    """

    def __init__(self, check_data):
        self.check_rule = check_data.get("rule")
        self.values = check_data.get("values")

    def __repr__(self):
        return f"DataQualityCheck(name={self.check_rule}, values={self.values})"


class DatasetColumn:
    """
    Represents a dataset column, including its name, data type, and validation rules.

    Attributes:
    ----------
    name : str
        The column name.

    data_type : str
        The expected data type (e.g., 'string', 'int').

    checks : list
        List of `DataQualityCheck` objects that define validation rules.

    Functionality:
    --------------
    - Stores metadata about dataset columns.
    - Includes validation rules that enforce data integrity.
    - Used for quality assurance before ingestion into a database.
    """

    def __init__(self, column_data):
        self.name = column_data.get("name")
        self.data_type = column_data.get("data_type")
        self.checks = [DataQualityCheck(x) for x in column_data.get("checks", [])]

    def __repr__(self):
        return f"DatasetColumn(name={self.name}, data_type={self.data_type}, expectations={self.checks})"


class DatasetConfig:
    """
    Represents the complete configuration for a dataset.

    Attributes:
    ----------
    enabled : bool
        Determines whether the dataset is active.

    provider : str
        The name of the dataset provider.

    connection : str
        The type of connection (e.g., S3, Snowflake).

    target_database : str
        The target database name.

    target_schema : str
        The target schema name.

    target_table : str
        The target table name.

    primary_key : str
        The primary key(s) for upsert operations.

    operation : str
        The type of database operation (e.g., 'insert', 'upsert', 'replace').

    date_start : datetime
        The starting date for data extraction.

    columns : list
        List of `DatasetColumn` objects describing the dataset structure.

    Functionality:
    --------------
    - Loads dataset configuration settings from JSON or YAML.
    - Provides metadata about the dataset for ingestion pipelines.
    - Stores data quality rules and transformation logic.
    """

    def __init__(self, config_data):
        # Initialize class attributes based on the YAML structure
        self.enabled = config_data.get("enabled")
        self.provider = config_data.get("provider")
        self.connection = config_data.get("connection")
        self.target_database = config_data.get("target_database")
        self.target_schema = config_data.get("target_schema")
        self.target_table = config_data.get("target_table")
        self.primary_key = config_data.get("primary_key")
        self.operation = config_data.get("operation")
        self.date_start = pendulum.parse(str(config_data.get("date_start")))

        # Parse columns
        yaml_columns = config_data.get("columns")
        self.columns = [DatasetColumn(x) for x in yaml_columns]

        # Parse s3_metadata section
        s3_metadata = config_data.get("s3_metadata", {})
        if s3_metadata != {}:
            self.source_bucket = s3_metadata.get("source_bucket")
            self.source_key = s3_metadata.get("source_key")
            self.file_type = s3_metadata.get("file_type")
            self.s3_delimiter = s3_metadata.get("delimiter")
            self.s3_skip_rows = s3_metadata.get("skip_rows")
            self.purge_source = s3_metadata.get("purge_source")

        # Parse sftp_metadata section
        sftp_metadata = config_data.get("sftp_metadata", {})
        if sftp_metadata != {}:
            self.credentials_secret = sftp_metadata.get("credentials_secret")
            self.source_path = sftp_metadata.get("source_path")
            self.file_prefix = sftp_metadata.get("file_prefix")
            self.compressed = sftp_metadata.get("compressed")
            self.compression_method = sftp_metadata.get("compression_method")
            self.compressed_file_prefix = sftp_metadata.get("compressed")
            self.compressed_file_type = sftp_metadata.get("compressed_file_prefix")
            self.file_type = sftp_metadata.get("file_type")
            self.sftp_delimiter = sftp_metadata.get("delimiter")
            self.quote_char = sftp_metadata.get("quote_char")
            self.escape_char = sftp_metadata.get("escape_char")
            self.sftp_skip_rows = sftp_metadata.get("skip_rows")

    def __repr__(self):
        return f"""DatasetConfig(enabled={self.enabled},
                provider={self.provider},
                connection={self.connection},
                target_database={self.target_database},
                target_schema={self.target_schema},
                target_table={self.target_table},
                primary_key={self.primary_key},
                operation={self.operation},
                date_start={self.date_start})"""

    def perform_data_quality_check(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Performs data quality checks on the given DataFrame and separates valid and invalid rows.

        Parameters:
        ----------
        df : pd.DataFrame
            The input DataFrame containing dataset records.

        Functionality:
        --------------
        - Iterates over all dataset columns and their defined quality checks.
        - Validates each column based on configured rules (e.g., `not_null`, `is_numeric`, `value_between`).
        - If a check fails, marks the row as invalid and logs the failed rule.
        - Stores failed checks separately and retains valid rows for further processing.
        - Supports rules such as:
            - `not_null`: Ensures values are not null.
            - `is_numeric`: Checks if values are numeric.
            - `value_between`: Ensures numeric values fall within a specified range.
            - `greater_than` / `less_than`: Enforces numerical thresholds.
            - `match_regex`: Validates strings against a regex pattern.
        - Returns two DataFrames:
            - **Valid rows** that passed all checks.
            - **Invalid rows** with a list of failed validation rules.

        Returns:
        -------
        tuple[pd.DataFrame, pd.DataFrame]
            A tuple containing:
            - A DataFrame of valid records that passed all checks.
            - A DataFrame of invalid records with reasons for failure.

        Raises:
        -------
        ValueError
            If a rule requires parameters (e.g., min/max values for `value_between`) and they are missing.

        Example:
        --------
        ```python
        dataset_config = DatasetConfig(config_data)
        valid_df, invalid_df = dataset_config.perform_data_quality_check(input_dataframe)

        print("Valid records:", valid_df.shape[0])
        print("Invalid records:", invalid_df.shape[0])
        ```
        """

        valid_rows = df.copy()
        invalid_rows = pd.DataFrame()
        failed_checks = pd.DataFrame()

        for column in self.columns:
            for data_quality_check in column.checks:
                # Retrieve check values for later reference
                if data_quality_check.values:
                    min_value = data_quality_check.values.get("min_value", None)
                    max_value = data_quality_check.values.get("max_value", None)
                    regex = data_quality_check.values.get("regex", None)

                ## CHECK_RULE: COLUMN VALUE IS NOT NULL
                if data_quality_check.check_rule == "not_null":
                    invalid_condition = df[column.name].isnull()
                    failed_check_value = f"{column.name} is NULL"

                ## CHECK_RULE: COLUMN VALUE IS NUMERIC
                elif data_quality_check.check_rule == "is_numeric":
                    invalid_condition = pd.to_numeric(df[column.name], errors="coerce").isnull()
                    failed_check_value = f"{column.name} has non-numeric values"

                ## CHECK_RULE: COLUMN VALUE (MUST BE NUMERIC) BETWEEN TWO NUMBERS
                elif data_quality_check.check_rule == "value_between":
                    if min_value is None or max_value is None:
                        raise ValueError(f"Missing min_value or max_value for 'value_between' check in column: {column.name}")
                    df[column.name] = pd.to_numeric(df[column.name], errors="coerce")
                    invalid_condition = df[column.name].isnull() | (df[column.name] < min_value) | (df[column.name] > max_value)
                    failed_check_value = f"{column.name} has values outside the range [{min_value}, {max_value}]"

                ## CHECK_RULE: COLUMN VALUE (MUST BE NUMERIC) IS GREATER THAN NUMBER
                elif data_quality_check.check_rule == "greater_than":
                    if min_value is None:
                        raise ValueError(f"Missing min_value for 'greater_than' check in column: {column.name}")
                    df[column.name] = pd.to_numeric(df[column.name], errors="coerce")
                    invalid_condition = df[column.name].isnull() | (df[column.name] <= min_value)
                    failed_check_value = f"{column.name} has values less than or equal to {min_value} "

                ## CHECK_RULE: COLUMN VALUE (MUST BE NUMERIC) IS LESS THAN NUMBER
                elif data_quality_check.check_rule == "less_than":
                    if max_value is None:
                        raise ValueError(f"Missing max_value for 'less_than' check in column: {column.name}")
                    df[column.name] = pd.to_numeric(df[column.name], errors="coerce")
                    invalid_condition = df[column.name].isnull() | (df[column.name] >= max_value)
                    failed_check_value = f"{column.name} has values greater than or equal to {max_value}"

                ## CHECK_RULE: COLUMN VALUE MATCHES REGEX
                elif data_quality_check.check_rule == "match_regex":
                    invalid_condition = ~df[column.name].str.contains(regex, na=False)
                    failed_check_value = f"{column.name} has values that do not match regex: {regex}"
                else:
                    raise ValueError(f"Unknown Data Quality Rule: {data_quality_check.check_rule}")

                # Ensure invalid_condition contains no NaN values (replace NaN with False)
                invalid_condition = invalid_condition.fillna(False)

                # Separate valid and invalid rows based on the condition
                new_invalid_rows = df[invalid_condition]
                new_failed_checks = df[invalid_condition]
                new_failed_checks["FAILED_CHECK"] = failed_check_value
                failed_checks = pd.concat([failed_checks, new_failed_checks])
                invalid_rows = pd.concat([invalid_rows, new_invalid_rows])

                # Remove invalid rows from valid_rows (retain only rows that pass all checks)
                valid_rows = valid_rows[~invalid_condition]

        # Drop duplicate rows that might be accumulated during concatenation
        valid_rows.drop_duplicates(inplace=True)
        invalid_rows.drop_duplicates(inplace=True)

        failed_checks = failed_checks.groupby(level=0)["FAILED_CHECK"].apply(list)
        invalid_rows["FAILED_CHECKS"] = failed_checks

        return valid_rows, invalid_rows


class BaseDataset:
    """
    Abstract base class representing a dataset with transformation and validation capabilities.

    Attributes:
    ----------
    config : DatasetConfig
        The dataset configuration object containing metadata and processing rules.

    schedule : str
        The execution schedule for the dataset (e.g., 'daily', 'hourly').

    Functionality:
    --------------
    - Serves as a base class for dataset processing.
    - Stores dataset configuration metadata, including data sources and quality checks.
    - Defines hooks for data quality checks (`PerformDataQuality`) and transformations (`PerformDataTransformations`).
    - Intended to be extended by dataset-specific implementations.
    - Provides a structured framework for ingesting, validating, and transforming datasets.

    Methods:
    --------
    - `PerformDataQuality(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]`
        - Checks for data integrity based on configured quality rules.
        - Returns a tuple containing valid and invalid records.

    - `PerformDataTransformations(df: pd.DataFrame) -> pd.DataFrame`
        - Applies necessary transformations (e.g., data type conversions, renaming columns).
        - Returns a transformed DataFrame.

    Example:
    --------
    ```python
    class CustomDataset(BaseDataset):
        def PerformDataQuality(self, df):
            # Custom validation logic
            return super().PerformDataQuality(df)

        def PerformDataTransformations(self, df):
            # Custom transformation logic
            return super().PerformDataTransformations(df)

    dataset = CustomDataset()
    valid_df, invalid_df = dataset.PerformDataQuality(input_dataframe)
    transformed_df = dataset.PerformDataTransformations(valid_df)
    ```
    """

    config: DatasetConfig
    schedule: str

    def PerformDataQuality(self, df: pd.DataFrame):
        return None, df

    def PerformDataTransformations(self, df: pd.DataFrame):
        pass
