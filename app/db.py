"""DuckDB-backed query layer over curated FOCUS Parquet on Azure Blob.

Curated layout (hive-partitioned):
  {root}/dataset=<daily|monthly>/cloud=<china|global>/subscription=<key>/period=YYYY-MM/*.parquet
"""
from __future__ import annotations

import threading
from typing import Any

import duckdb

from .config import Settings, SubscriptionConfig, get_settings

_conn_lock = threading.Lock()
_conn: duckdb.DuckDBPyConnection | None = None


def _account_name(account_url: str) -> str:
    return account_url.split("//", 1)[-1].split(".", 1)[0]


def _endpoint_suffix(account_url: str) -> str:
    """Storage endpoint suffix, e.g. blob.core.windows.net (public) or
    blob.core.chinacloudapi.cn (Azure China / 21Vianet)."""
    host = account_url.split("//", 1)[-1]
    return host.split(".", 1)[1] if "." in host else host


def _authority_host(account_url: str) -> str | None:
    """AAD authority for the sovereign cloud the storage account lives in.

    DuckDB's azure extension (and azure-identity) resolve OAuth tokens against
    the global authority (login.microsoftonline.com) by default, which returns
    HTTP 400 for a China-tenant service principal. Return the China authority
    for *.chinacloudapi.cn accounts; None means use the SDK default (public)."""
    if ".chinacloudapi.cn" in account_url:
        return "https://login.chinacloudapi.cn"
    return None


def _q(value: str) -> str:
    """Escape a string as a DuckDB SQL single-quoted literal.

    CREATE SECRET does not support bound parameters, so values are inlined.
    Inputs come from trusted server-side config, not request data.
    """
    return "'" + value.replace("'", "''") + "'"


def _create_secret(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    name: str,
    account_url: str,
    container: str,
) -> None:
    """Create one DuckDB azure secret scoped to a specific container/account."""
    account = _account_name(account_url)
    scope = f"az://{container}"
    endpoint = _endpoint_suffix(account_url)
    mode = settings.azure_storage_auth_mode

    if mode == "service_principal":
        if not (settings.azure_tenant_id and settings.azure_client_id):
            raise ValueError("AZURE_TENANT_ID and AZURE_CLIENT_ID are required")
        common = (
            f"TENANT_ID {_q(settings.azure_tenant_id)}, "
            f"CLIENT_ID {_q(settings.azure_client_id)}, "
        )
        if settings.azure_client_secret:
            cred = f"CLIENT_SECRET {_q(settings.azure_client_secret)}, "
        elif settings.azure_client_certificate_path:
            cred = f"CLIENT_CERTIFICATE_PATH {_q(settings.azure_client_certificate_path)}, "
        else:
            raise ValueError(
                "service_principal auth needs AZURE_CLIENT_SECRET or AZURE_CLIENT_CERTIFICATE_PATH"
            )
        con.execute(
            f"CREATE OR REPLACE SECRET {name} (TYPE azure, PROVIDER service_principal, "
            f"{common}{cred}ACCOUNT_NAME {_q(account)}, ENDPOINT {_q(endpoint)}, "
            f"SCOPE {_q(scope)});"
        )
    elif mode == "connection_string":
        if not settings.azure_storage_connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING is required")
        con.execute(
            f"CREATE OR REPLACE SECRET {name} (TYPE azure, "
            f"CONNECTION_STRING {_q(settings.azure_storage_connection_string)}, "
            f"SCOPE {_q(scope)});"
        )
    elif mode == "sas":
        if not settings.azure_storage_sas_token:
            raise ValueError("AZURE_STORAGE_SAS_TOKEN is required")
        conn = f"BlobEndpoint={account_url};SharedAccessSignature={settings.azure_storage_sas_token}"
        con.execute(
            f"CREATE OR REPLACE SECRET {name} (TYPE azure, "
            f"CONNECTION_STRING {_q(conn)}, SCOPE {_q(scope)});"
        )
    else:  # managed_identity / az cli dev -> credential chain
        con.execute(
            f"CREATE OR REPLACE SECRET {name} (TYPE azure, PROVIDER credential_chain, "
            f"ACCOUNT_NAME {_q(account)}, ENDPOINT {_q(endpoint)}, SCOPE {_q(scope)});"
        )


def _configure_azure(con: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    import os

    con.execute("INSTALL azure; LOAD azure;")
    # DuckDB's azure extension resolves OAuth tokens via the Azure C++ SDK, which
    # reads AZURE_AUTHORITY_HOST. Without this, a China-tenant SP gets HTTP 400.
    authority = _authority_host(settings.blob_account_url)
    if authority:
        os.environ["AZURE_AUTHORITY_HOST"] = authority
    # Distinct (account, container) targets: raw export + curated.
    targets = {
        ("raw", settings.blob_account_url, settings.blob_container),
        ("cur", settings.curated_account_url_effective, settings.curated_container_effective),
    }
    seen: set[tuple[str, str]] = set()
    for name, url, container in targets:
        key = (url, container)
        if key in seen:
            continue
        seen.add(key)
        _create_secret(con, settings, f"az_{name}", url, container)



def get_connection() -> duckdb.DuckDBPyConnection:
    global _conn
    with _conn_lock:
        if _conn is None:
            settings = get_settings()
            con = duckdb.connect(database=":memory:")
            if settings.storage_backend == "azure_blob":
                _configure_azure(con, settings)
            _conn = con
        return _conn


def curated_root(settings: Settings) -> str:
    if settings.storage_backend == "local":
        return f"{settings.local_data_root.rstrip('/')}/{settings.curated_prefix}"
    return f"az://{settings.curated_container_effective}/{settings.curated_prefix}"


def partition_relpath(dataset: str, cloud: str, subscription_key: str, period: str) -> str:
    return (
        f"dataset={dataset}/cloud={cloud}"
        f"/subscription={subscription_key}/period={period}"
    )


def write_partition(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    dataset: str,
    cloud: str,
    subscription_key: str,
    period: str,
    select_sql: str,
) -> tuple[str, int]:
    """Write one curated partition (full overwrite). Returns (location, rows).

    DuckDB cannot write to az://, so for the azure backend we COPY to a local
    temp parquet and upload via the Azure SDK, clearing the partition prefix
    first for idempotency.
    """
    import os

    rel = partition_relpath(dataset, cloud, subscription_key, period)
    rows = con.execute(f"SELECT count(*) FROM ({select_sql})").fetchone()[0]

    if settings.storage_backend == "local":
        out_dir = f"{curated_root(settings)}/{rel}"
        os.makedirs(out_dir, exist_ok=True)
        out_file = f"{out_dir}/data.parquet"
        con.execute(
            f"COPY ({select_sql}) TO '{out_file}' (FORMAT parquet, COMPRESSION snappy);"
        )
        return out_file, int(rows)

    # azure_blob: DuckDB -> local temp -> SDK upload
    import tempfile

    from .storage import delete_prefix, upload_file

    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    try:
        con.execute(
            f"COPY ({select_sql}) TO '{tmp}' (FORMAT parquet, COMPRESSION snappy);"
        )
        account = settings.curated_account_url_effective
        container = settings.curated_container_effective
        prefix = f"{settings.curated_prefix}/{rel}/"
        blob_name = f"{prefix}data.parquet"
        delete_prefix(settings, account, container, prefix)
        upload_file(settings, account, container, blob_name, tmp)
    finally:
        os.remove(tmp)
    return f"az://{container}/{blob_name}", int(rows)




def _glob_for(settings: Settings, dataset: str, subs: list[SubscriptionConfig], period: str) -> list[str]:
    root = curated_root(settings)
    globs = []
    for s in subs:
        globs.append(
            f"{root}/dataset={dataset}/cloud={s.cloud}"
            f"/subscription={s.subscription_key}/period={period}/*.parquet"
        )
    return globs


def _scan_expr(globs: list[str]) -> str:
    files = ", ".join(f"'{g}'" for g in globs)
    return (
        f"read_parquet([{files}], union_by_name=true, "
        f"hive_partitioning=true, filename=false)"
    )


def query_billing(
    *,
    dataset: str,
    subs: list[SubscriptionConfig],
    period: str,
    where_sql: str,
    where_params: list[Any],
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], int]:
    """Run a partition-pruned, paginated query. Returns (rows, total)."""
    settings = get_settings()
    con = get_connection()
    globs = _glob_for(settings, dataset, subs, period)
    if not globs:
        return [], 0

    scan = _scan_expr(globs)
    base = f"SELECT * FROM {scan} WHERE {where_sql}"

    try:
        total = con.execute(
            f"SELECT count(*) FROM ({base})", where_params
        ).fetchone()[0]
    except duckdb.IOException:
        # Partition path does not exist yet -> treat as empty result.
        return [], 0

    offset = (page - 1) * page_size
    rel = con.execute(
        f"{base} ORDER BY \"ChargePeriodStart\" LIMIT ? OFFSET ?",
        [*where_params, page_size, offset],
    )
    cols = [d[0] for d in rel.description]
    rows = [dict(zip(cols, r)) for r in rel.fetchall()]
    return rows, int(total)
