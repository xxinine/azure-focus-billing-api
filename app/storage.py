"""Azure Blob write helpers.

DuckDB's azure extension can READ az:// but cannot WRITE to containers, so
curated parquet is written locally by DuckDB and uploaded with the Azure SDK
using the same credential model as the rest of the app.
"""
from __future__ import annotations

from azure.storage.blob import BlobServiceClient

from .config import Settings


def _authority(account_url: str) -> str | None:
    """AAD authority for the sovereign cloud the account lives in.
    None means the azure-identity default (Azure public cloud)."""
    if ".chinacloudapi.cn" in account_url:
        return "https://login.chinacloudapi.cn"
    return None


def _credential(settings: Settings):
    from azure.identity import (
        CertificateCredential,
        ClientSecretCredential,
        DefaultAzureCredential,
    )

    authority = _authority(settings.blob_account_url)
    if settings.azure_storage_auth_mode == "service_principal":
        if settings.azure_client_secret:
            return ClientSecretCredential(
                settings.azure_tenant_id,
                settings.azure_client_id,
                settings.azure_client_secret,
                authority=authority,
            )
        return CertificateCredential(
            settings.azure_tenant_id,
            settings.azure_client_id,
            certificate_path=settings.azure_client_certificate_path,
            authority=authority,
        )
    # managed_identity / az cli dev
    return DefaultAzureCredential(authority=authority) if authority else DefaultAzureCredential()


def blob_service(settings: Settings, account_url: str) -> BlobServiceClient:
    mode = settings.azure_storage_auth_mode
    if mode == "connection_string":
        return BlobServiceClient.from_connection_string(
            settings.azure_storage_connection_string
        )
    if mode == "sas":
        return BlobServiceClient(account_url, credential=settings.azure_storage_sas_token)
    return BlobServiceClient(account_url, credential=_credential(settings))


def delete_prefix(settings: Settings, account_url: str, container: str, prefix: str) -> int:
    """Delete all blobs under a prefix (partition overwrite). Returns count."""
    svc = blob_service(settings, account_url)
    cc = svc.get_container_client(container)
    n = 0
    for b in cc.list_blobs(name_starts_with=prefix):
        cc.delete_blob(b.name)
        n += 1
    return n


def upload_file(
    settings: Settings, account_url: str, container: str, blob_name: str, local_path: str
) -> None:
    svc = blob_service(settings, account_url)
    bc = svc.get_blob_client(container, blob_name)
    with open(local_path, "rb") as f:
        bc.upload_blob(f, overwrite=True)
