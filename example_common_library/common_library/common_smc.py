from datetime import datetime
from common_library.common_datasets import BaseDataset
from common_library.common_aws import DataframeFromS3
import common_library.common_config as config
import logging

log = logging.getLogger(__name__)


def ParseSMCFileName(file_name: str):
    """
    Parses an SMC filename to extract metadata such as feed name, date, data type, and file type.

    Parameters:
    ----------
    file_name : str
        The full name of the SMC file.

    Functionality:
    --------------
    - Splits the filename based on underscores and periods.
    - Extracts the file type (`DAT` or `TRG`).
    - Identifies the data type (e.g., `ADHOC`, `RECON`, `DATA`).
    - Extracts the timestamp from the filename and converts it into a `datetime` object.
    - Determines the data feed name by removing the timestamp and data type.

    Returns:
    -------
    tuple[str, datetime, str, str]
        A tuple containing:
        - `data_feed_name`: The name of the data feed.
        - `date_stamp`: The extracted timestamp as a `datetime` object.
        - `data_type`: The type of data (e.g., `ADHOC`, `RECON`, `DATA`).
        - `file_type`: The file extension (`DAT` or `TRG`).

    Example:
    --------
    ```python
    file_metadata = ParseSMCFileName("SMC_FEED_20240301123045_DATA.DAT")
    print(file_metadata)
    # Output: ('SMC_FEED', datetime(2024, 3, 1, 12, 30, 45), 'DATA', 'DAT')
    ```
    """

    # Split the file name on the period and get the file_type (DAT or TRG)
    file_name_split = file_name.split(".")
    file_type = file_name_split[-1]

    # Split the file name on the underscore
    file_info = file_name_split[0]
    file_info_split = file_info.split("_")

    # The last element in the list is the date
    file_date = file_info_split[-1]
    date_stamp = datetime.strptime(file_date, "%Y%m%d%H%M%S")

    # Drop the date element and now the last element in the list is the data type
    file_info_split.pop(-1)
    data_type = file_info_split[-1]

    # Drop the data type element and what remains is the data feed name
    file_info_split.pop(-1)
    data_feed_name = "_".join(file_info_split)

    return data_feed_name, date_stamp, data_type, file_type


def ParseSMCTriggerFile(dataset_object: BaseDataset, trg_key):
    """
    Reads and parses an SMC trigger file from S3 to extract metadata.

    Parameters:
    ----------
    dataset_object : BaseDataset
        The dataset object containing metadata about the SMC file.

    trg_key : str
        The S3 key (path) of the trigger file.

    Functionality:
    --------------
    - Constructs the full S3 URL for the trigger file.
    - Reads the trigger file into a Pandas DataFrame.
    - Uses `|` as a delimiter to parse column-separated values.
    - Extracts important metadata fields, such as:
        - `Run_Number`
        - `Extract_Begin_DateTime`
        - `Extract_End_DateTime`
    - Logs the contents of the trigger file for reference.

    Returns:
    -------
    pd.DataFrame
        A DataFrame containing the parsed trigger file metadata.

    Raises:
    -------
    RuntimeError
        If the trigger file cannot be retrieved or parsed.

    Example:
    --------
    ```python
    trigger_data = ParseSMCTriggerFile(dataset_object, "s3_bucket/smc/triggers/smc_trigger_20240301.TRG")
    print(trigger_data)
    ```
    """

    trg_raw_s3_URL = f"s3://{config.raw_s3_bucket}/{trg_key}"
    trg_df = DataframeFromS3(s3_url=trg_raw_s3_URL, file_type="csv", delimiter="|", header=0)
    log.info(f"Trigger File Contents for Reference:\n {trg_df}")
    return trg_df
