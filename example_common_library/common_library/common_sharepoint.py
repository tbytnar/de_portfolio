from office365.sharepoint.client_context import ClientContext
import logging
import common_library.common_config as config
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

working_dir = config.working_directory


def upload_file_to_sharepoint(sharepoint_secrets, file_path, site_url, folder_url):
    """
    Uploads a file to a specified SharePoint folder.

    Parameters:
    ----------
    sharepoint_secrets : dict
        A dictionary containing authentication details for SharePoint:
        - `"tenant_id"`: The Azure AD tenant ID.
        - `"client_id"`: The application client ID.
        - `"tenant_url"`: The SharePoint tenant URL.
        - `"thumbprint"`: The certificate thumbprint for authentication.
        - `"private_cert"`: The private key certificate content.

    file_path : str
        The full local path of the file to be uploaded.

    site_url : str
        The SharePoint site URL where the file should be uploaded.

    folder_url : str
        The relative path of the destination folder in SharePoint.

    Functionality:
    --------------
    - Establishes a secure connection to SharePoint using certificate-based authentication.
    - Retrieves the specified folder in the SharePoint document library.
    - Reads the file from the local system and uploads it to the designated folder.
    - Logs the success or failure of the upload operation.

    Returns:
    -------
    office365.sharepoint.files.File
        The uploaded file object from SharePoint.

    Raises:
    -------
    RuntimeError
        If authentication fails or the file upload is unsuccessful.

    Example:
    --------
    ```python
    sharepoint_secrets = {
        "tenant_id": "12345678-90ab-cdef-1234-567890abcdef",
        "client_id": "abcd1234-efgh-5678-ijkl-9876543210mn",
        "tenant_url": "https://companyname.sharepoint.com",
        "thumbprint": "ABC123456DEF",
        "private_cert": "-----BEGIN CERTIFICATE-----\nMIIBIjANBgkqhki..."
    }

    uploaded_file = upload_file_to_sharepoint(
        sharepoint_secrets=sharepoint_secrets,
        file_path="report.csv",
        site_url="FinanceTeamSite",
        folder_url="Shared Documents/Reports"
    )
    print(f"File uploaded successfully: {uploaded_file}")
    ```
    """

    tenant_id = sharepoint_secrets.get("tenant_id", None)
    client_id = sharepoint_secrets.get("client_id", None)
    tenant_url = sharepoint_secrets.get("tenant_url", None)
    thumbprint = sharepoint_secrets.get("thumbprint", None)
    private_cert = sharepoint_secrets.get("private_cert", None)

    with open(os.path.join(working_dir, "spnt_pvt_key.pem"), "w") as private_cert_file:
        private_cert_file.write(private_cert)

    cert_credentials = {
        "tenant": tenant_id,
        "client_id": client_id,
        "thumbprint": thumbprint,
        "cert_path": os.path.join(working_dir, "spnt_pvt_key.pem"),
    }

    ctx = ClientContext(f"{tenant_url}/sites/{site_url}").with_client_certificate(**cert_credentials)

    folder = ctx.web.ensure_folder_path(folder_url).get().select(["ServerRelativePath"]).execute_query()

    with open(file_path, "rb") as f:
        file = folder.files.upload(f).execute_query()
        return file
