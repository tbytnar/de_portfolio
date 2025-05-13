import paramiko
from datetime import datetime
from time import sleep
import logging
import common_library.common_config as common_config
from common_library.common_datasets import BaseDataset
import pytz
import os

log = logging.getLogger(__name__)


class SFTPSourceFile:
    """
    Represents a file retrieved from an SFTP source.

    Attributes:
    ----------
    file_name : str
        The full name of the file on the SFTP server.

    file_name_no_ext : str
        The file name without the extension.

    smc_file_type : str
        The type of file (e.g., 'DATA', 'DATA_TRG', 'ADHOC', etc.).

    last_modified : datetime
        The last modified timestamp of the file.

    Functionality:
    --------------
    - Stores metadata about files downloaded from an SFTP server.
    - Categorizes files based on naming conventions (e.g., `DATA_TRG`, `ADHOC`).
    - Determines the S3 key structure for storage after download.
    - Used to manage file ingestion workflows.
    """

    file_name = str
    file_name_no_ext = str
    smc_file_type = str
    last_modified = datetime

    def __init__(self, file_name, file_last_modified):
        self.file_name = file_name
        self.file_name_no_ext = file_name.split(".")[0]
        self.last_modified = file_last_modified
        if ".trg" in file_name.lower() and "_data" in file_name.lower():
            self.smc_file_type = "DATA_TRG"
        elif ".dat" in file_name.lower() and "_data" in file_name.lower():
            self.smc_file_type = "DATA"
        elif ".trg" in file_name.lower() and "_adhoc" in file_name.lower():
            self.smc_file_type = "ADHOC_TRG"
        elif ".dat" in file_name.lower() and "_adhoc" in file_name.lower():
            self.smc_file_type = "ADHOC"
        elif ".trg" in file_name.lower() and "_recon" in file_name.lower():
            self.smc_file_type = "RECON_TRG"
        elif ".dat" in file_name.lower() and "_recon" in file_name.lower():
            self.smc_file_type = "RECON"
        else:
            self.smc_file_type = "UNKNOWN"


def ConnectToSFTPSource(credentials, optimize_transport=True):
    """
    Establishes a connection to an SFTP server using the provided credentials.

    Parameters:
    ----------
    credentials : dict
        A dictionary containing SFTP connection details (`Host`, `Port`, `Username`, `Password` or `PrivateKey`).

    optimize_transport : bool, optional
        If True, optimizes the transport settings for high-performance file transfers.

    Functionality:
    --------------
    - Establishes an SFTP connection using Paramiko.
    - Supports both password and private key authentication.
    - Optimizes transport settings to improve transfer performance.
    - Returns an active SFTP client for further operations.

    Returns:
    -------
    paramiko.SFTPClient
        An active SFTP client connection.

    Raises:
    -------
    paramiko.AuthenticationException
        If authentication fails due to incorrect credentials.

    paramiko.SSHException
        If an error occurs while setting up the SSH transport.

    Example:
    --------
    ```python
    credentials = {"Host": "sftp.example.com", "Port": 22, "Username": "user", "Password": "pass"}
    sftp_client = ConnectToSFTPSource(credentials)
    ```
    """

    host = credentials["Host"]
    port = int(credentials["Port"])
    username = credentials["Username"]

    transport = paramiko.Transport((host, port))
    if optimize_transport:
        transport.default_window_size = 4294967294  # 2147483647
        transport.packetizer.REKEY_BYTES = pow(2, 40)
        transport.packetizer.REKEY_PACKETS = pow(2, 40)

    if credentials.get("PrivateKey", False):
        pkpath = "/tmp/key"
        with open(pkpath, "w") as f:
            f.write(credentials["PrivateKey"].replace(r"\n", "\n"))
        pkey = paramiko.RSAKey.from_private_key_file(pkpath)
        transport.connect(username=username, pkey=pkey)
    else:
        password = credentials["Password"]
        transport.connect(username=username, password=password)

    return paramiko.SFTPClient.from_transport(transport)


def ListSFTPFilesInPath(sftp_credentials, dataset_object: BaseDataset, start_time: datetime, end_time: datetime):
    """
    Lists files in the specified SFTP directory that match the dataset prefix and fall within the time range.

    Parameters:
    ----------
    sftp_credentials : dict
        The authentication details for connecting to the SFTP server.

    dataset_object : BaseDataset
        The dataset object containing SFTP configuration details.

    start_time : datetime
        The earliest modified timestamp for files to include.

    end_time : datetime
        The latest modified timestamp for files to include.

    Functionality:
    --------------
    - Connects to the SFTP server using the provided credentials.
    - Changes to the dataset's configured source directory.
    - Retrieves the list of files in the directory.
    - Filters files based on the dataset's configured file prefix.
    - Only includes files modified within the specified time range.
    - Returns a list of matching files with their metadata.

    Returns:
    -------
    list
        A list of dictionaries containing `file_name` and `modified_time` for each matching file.

    Raises:
    -------
    RuntimeError
        If the SFTP connection fails or the directory is inaccessible.

    Example:
    --------
    ```python
    matching_files = ListSFTPFilesInPath(sftp_credentials, dataset_object, start_time, end_time)
    print(matching_files)
    ```
    """

    sftp_connection = ConnectToSFTPSource(sftp_credentials)
    try:
        sftp_connection.chdir(dataset_object.config.source_path)
        log.info(f"Dataset File Prefix:{dataset_object.config.file_prefix} configured with start date of: {dataset_object.config.date_start}")
        log.info(f"Dataset File Prefix:{dataset_object.config.file_prefix} checking time range: Start={start_time} End={end_time}")
        file_list = []
        files = sftp_connection.listdir_attr()

        for file in files:
            filename = file.filename

            if dataset_object.config.file_prefix in filename:
                modified_time = pytz.utc.localize(datetime.fromtimestamp(file.st_mtime))
                log.info(f"Found File: {filename} with modified_time of {modified_time}")
                if modified_time >= start_time and modified_time <= end_time:
                    file_list.append({"file_name": filename, "modified_time": modified_time})
        sftp_connection.close()
        return file_list
    except Exception as e:
        log.error(e)
        raise e
    finally:
        sftp_connection.close()


def LogProgress(transferred, toBeTransferred):
    """
    Logs the progress of an SFTP file transfer.

    Parameters:
    ----------
    transferred : int
        The number of bytes transferred so far.

    toBeTransferred : int
        The total number of bytes to be transferred.

    Functionality:
    --------------
    - Logs the current progress of a file transfer.
    - Useful for monitoring large file downloads.
    - Introduces a small delay to prevent excessive logging.

    Returns:
    -------
    None

    Example:
    --------
    ```python
    sftp.get(remotepath, localpath, callback=LogProgress)
    ```
    """
    log.info("Transferred: {0}\tOut of: {1}".format(transferred, toBeTransferred))
    sleep(2.5)


def DownloadSFTPFile(sftp_credentials, source_file: SFTPSourceFile, source_file_path: str):
    """
    Downloads a file from an SFTP server to the local working directory.

    Parameters:
    ----------
    sftp_credentials : dict
        The authentication details for connecting to the SFTP server.

    source_file : SFTPSourceFile
        The metadata of the file to be downloaded.

    source_file_path : str
        The path on the SFTP server where the file is located.

    Functionality:
    --------------
    - Establishes an SFTP connection using the provided credentials.
    - Checks if the file has already been downloaded to avoid duplicate downloads.
    - Downloads the file from the remote SFTP directory to the local working directory.
    - Logs the progress of the download.
    - Closes the SFTP connection after the transfer is complete.

    Returns:
    -------
    str
        The local file path where the downloaded file is saved.

    Raises:
    -------
    RuntimeError
        If the file download fails due to connectivity issues or insufficient permissions.

    Example:
    --------
    ```python
    local_path = DownloadSFTPFile(sftp_credentials, source_file, dataset_object)
    print(f"File downloaded to: {local_path}")
    ```
    """

    log.info(f"Downloading {source_file.file_name} : {source_file.last_modified}")
    if not os.path.exists(os.path.join(common_config.working_directory, source_file.file_name)):
        sftp_connection = ConnectToSFTPSource(sftp_credentials, optimize_transport=True)

        try:
            sftp_file_path = source_file_path + "/" + str(source_file.file_name)
            log.info(f"Downloading {sftp_file_path} to {common_config.working_directory}/{source_file.file_name}")
            sftp_file = sftp_connection.file(filename=sftp_file_path, mode="r")
            sftp_file_size = sftp_file.stat().st_size

            with open(os.path.join(common_config.working_directory, source_file.file_name), "wb") as local_file:
                sftp_file.prefetch(sftp_file_size)
                sftp_file.set_pipelined()
                while True:
                    data = sftp_file.read((1024 * 1024 * 5))
                    if not data:
                        break
                    local_file.write(data)
                    LogProgress(len(data), sftp_file_size)
        except Exception as e:
            log.error(e)
            raise e
        finally:
            sftp_connection.close()

    else:
        log.info(f"{source_file.file_name} has already been downloaded.")

    return f"{common_config.working_directory}/{source_file.file_name}"
