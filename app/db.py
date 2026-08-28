"""DuckDB-backed query layer over curated FOCUS Parquet on Azure Blob.

Curated layout (hive-partitioned):
  {root}/dataset=<daily|monthly>/cloud=<china|global>/subscription=<key>/period=YYYY-MM/*.parquet
"""
from __future__ import annotations

import threading
from decimal import Decimal
from typing import Any

import duckdb

from .config import Settings, SubscriptionConfig, get_settings
from .schema import (
    CORE_COST_COLUMNS,
    CORE_STRING_COLUMNS,
    CORE_TIME_COLUMNS,
    NON_RESOURCE_CHARGE_CATEGORIES,
    VALID_CHARGE_CATEGORIES,
)

_conn_lock = threading.Lock()
_conn: duckdb.DuckDBPyConnection | None = None


class BillingDataQualityError(ValueError):
    """Raised when query results violate the required billing contract."""

    def __init__(self, violations: dict[str, int]):
        self.violations = violations
        detail = ", ".join(f"{field}={count}" for field, count in violations.items())
        super().__init__(f"billing core field validation failed: {detail}")


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


def _inline_sql_params(sql: str, params: list[Any]) -> str:
    """Inline trusted internal filter values for DuckDB CTE scan reuse."""
    parts = sql.split("?")
    if len(parts) != len(params) + 1:
        raise ValueError("SQL placeholder count does not match parameter count")
    rendered = parts[0]
    for value, suffix in zip(params, parts[1:]):
        if value is None:
            literal = "NULL"
        elif isinstance(value, bool):
            literal = "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            literal = str(value)
        else:
            literal = _q(str(value))
        rendered += literal + suffix
    return rendered


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


def curated_root(settings: Settings, prefix: str | None = None) -> str:
    storage_prefix = prefix or settings.curated_prefix
    if settings.storage_backend == "local":
        return f"{settings.local_data_root.rstrip('/')}/{storage_prefix}"
    return f"az://{settings.curated_container_effective}/{storage_prefix}"


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
    prefix: str | None = None,
) -> tuple[str, int]:
    """Write one curated partition (full overwrite). Returns (location, rows).

    DuckDB cannot write to az://, so for the azure backend we COPY to a local
    temp parquet and upload via the Azure SDK, clearing the partition prefix
    first for idempotency.
    """
    import os

    storage_prefix = prefix or settings.curated_prefix
    rel = partition_relpath(dataset, cloud, subscription_key, period)
    rows = con.execute(f"SELECT count(*) FROM ({select_sql})").fetchone()[0]

    if settings.storage_backend == "local":
        out_dir = f"{curated_root(settings, storage_prefix)}/{rel}"
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
        partition_prefix = f"{storage_prefix}/{rel}/"
        blob_name = f"{partition_prefix}data.parquet"
        delete_prefix(settings, account, container, partition_prefix)
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


def _tax_basis_glob_for(
    settings: Settings,
    dataset: str,
    subs: list[SubscriptionConfig],
    period: str,
) -> list[str]:
    root = curated_root(settings, settings.tax_basis_prefix)
    return [
        f"{root}/dataset={dataset}/cloud={sub.cloud}"
        f"/subscription={sub.subscription_key}/period={period}/*.parquet"
        for sub in subs
    ]


def _scan_expr(globs: list[str]) -> str:
    files = ", ".join(f"'{g}'" for g in globs)
    return (
        f"read_parquet([{files}], union_by_name=true, "
        f"hive_partitioning=true, filename=false)"
    )


def _normalize_core_fields(source_sql: str) -> str:
    return f"""
        SELECT * REPLACE (
            coalesce(
                nullif(trim(CAST("ServiceProviderName" AS VARCHAR)), ''),
                nullif(trim(CAST("ProviderName" AS VARCHAR)), '')
            ) AS "ServiceProviderName",
            coalesce(
                nullif(trim(CAST("PublisherName" AS VARCHAR)), ''),
                nullif(trim(CAST("ProviderName" AS VARCHAR)), '')
            ) AS "PublisherName"
        )
        FROM ({source_sql})
    """


def build_tax_basis_sql(source_sql: str, dataset: str) -> str:
    """Pre-aggregate taxable costs and retain one representative FOCUS row."""
    if dataset not in ("daily", "monthly"):
        raise ValueError(f"unsupported dataset: {dataset}")

    normalized_source = _normalize_core_fields(source_sql)
    daily = dataset == "daily"
    scope_projection = (
        ', CAST("ChargePeriodStart" AS DATE) AS "__TaxDate"' if daily else ""
    )
    scope_column = ['"__TaxDate"'] if daily else []
    context_columns = [
        '"ServiceName"',
        '"BillingAccountId"',
        '"BillingCurrency"',
        '"BillingPeriodStart"',
        '"BillingPeriodEnd"',
        '"ServiceCategory"',
        '"ServiceSubcategory"',
        '"ServiceProviderName"',
        '"InvoiceIssuerName"',
        '"ProviderName"',
    ]
    grouping = scope_column + context_columns
    grouping_sql = ", ".join(grouping)
    representative_columns = ", ".join(scope_column + context_columns)
    exclude_scope = " EXCLUDE (\"__TaxDate\")" if daily else ""
    return f"""
        WITH tax_base AS (
            SELECT *{scope_projection}
            FROM ({normalized_source})
            WHERE "ChargeCategory" IS DISTINCT FROM 'Tax'
        ),
        totals AS (
            SELECT
                {grouping_sql},
                sum("BilledCost") AS "TaxBasisBilledCost",
                sum("EffectiveCost") AS "TaxBasisEffectiveCost",
                sum("ListCost") AS "TaxBasisListCost",
                sum("ContractedCost") AS "TaxBasisContractedCost",
                min("ChargePeriodStart") AS "TaxBasisChargePeriodStart",
                max("ChargePeriodEnd") AS "TaxBasisChargePeriodEnd"
            FROM tax_base
            GROUP BY {grouping_sql}
        ),
        representatives AS (
            SELECT * FROM tax_base
            QUALIFY row_number() OVER (
                PARTITION BY {representative_columns}
                ORDER BY "ChargePeriodStart", "ResourceId" NULLS LAST
            ) = 1
        )
        SELECT representative.*{exclude_scope} REPLACE (
            totals."TaxBasisBilledCost" AS "BilledCost",
            totals."TaxBasisEffectiveCost" AS "EffectiveCost",
            totals."TaxBasisListCost" AS "ListCost",
            totals."TaxBasisContractedCost" AS "ContractedCost",
            totals."TaxBasisChargePeriodStart" AS "ChargePeriodStart",
            totals."TaxBasisChargePeriodEnd" AS "ChargePeriodEnd"
        )
        FROM representatives AS representative
        JOIN totals USING ({grouping_sql})
    """


def write_tax_basis_partition(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    dataset: str,
    cloud: str,
    subscription_key: str,
    period: str,
    source_sql: str,
) -> tuple[str, int]:
    return write_partition(
        con,
        settings,
        dataset=dataset,
        cloud=cloud,
        subscription_key=subscription_key,
        period=period,
        select_sql=build_tax_basis_sql(source_sql, dataset),
        prefix=settings.tax_basis_prefix,
    )


def rebuild_tax_basis_partition(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    dataset: str,
    sub: SubscriptionConfig,
    period: str,
) -> tuple[str, int]:
    source_sql = f"SELECT * FROM {_scan_expr(_glob_for(settings, dataset, [sub], period))}"
    return write_tax_basis_partition(
        con,
        settings,
        dataset=dataset,
        cloud=sub.cloud,
        subscription_key=sub.subscription_key,
        period=period,
        source_sql=source_sql,
    )


def _validate_core_fields(
    con: duckdb.DuckDBPyConnection,
    source_sql: str,
    params: list[Any],
) -> None:
    checks: list[tuple[str, str]] = []
    for column in CORE_COST_COLUMNS:
        checks.append(
            (column, f'"{column}" IS NULL OR NOT isfinite(CAST("{column}" AS DOUBLE))')
        )
    for column in CORE_TIME_COLUMNS:
        checks.append((column, f'"{column}" IS NULL'))
    for column in CORE_STRING_COLUMNS:
        checks.append(
            (column, f'"{column}" IS NULL OR trim(CAST("{column}" AS VARCHAR)) = \'\'')
        )

    allowed_categories = ", ".join(_q(value) for value in VALID_CHARGE_CATEGORIES)
    non_resource_categories = ", ".join(
        _q(value) for value in NON_RESOURCE_CHARGE_CATEGORIES
    )
    checks.extend(
        [
            (
                "BillingCurrency",
                '"BillingCurrency" IS NOT NULL AND NOT regexp_full_match('
                'CAST("BillingCurrency" AS VARCHAR), \'[A-Z]{3}\')',
            ),
            (
                "ChargeCategory",
                f'"ChargeCategory" IS NOT NULL AND "ChargeCategory" NOT IN ({allowed_categories})',
            ),
            (
                "BillingPeriodEnd",
                '"BillingPeriodStart" IS NOT NULL AND "BillingPeriodEnd" IS NOT NULL '
                'AND "BillingPeriodEnd" <= "BillingPeriodStart"',
            ),
            (
                "ChargePeriodEnd",
                '"ChargePeriodStart" IS NOT NULL AND "ChargePeriodEnd" IS NOT NULL '
                'AND "ChargePeriodEnd" <= "ChargePeriodStart"',
            ),
            (
                "ResourceId",
                f'"ChargeCategory" NOT IN ({non_resource_categories}) AND '
                '("ResourceId" IS NULL OR trim(CAST("ResourceId" AS VARCHAR)) = \'\')',
            ),
        ]
    )

    projections = ", ".join(
        f'count(*) FILTER (WHERE {condition}) AS "{index}"'
        for index, (_, condition) in enumerate(checks)
    )
    counts = con.execute(f"SELECT {projections} FROM ({source_sql})", params).fetchone()
    violations: dict[str, int] = {}
    for (field, _), count in zip(checks, counts):
        if count:
            violations[field] = violations.get(field, 0) + int(count)
    if violations:
        raise BillingDataQualityError(violations)


def _validate_tax_group_context(
    con: duckdb.DuckDBPyConnection,
    source_sql: str,
    params: list[Any],
) -> None:
    row = con.execute(
        f'SELECT "ServiceName" FROM ({source_sql}) '
        'WHERE "ChargeCategory" IS DISTINCT FROM \'Tax\' '
        'GROUP BY "ServiceName" HAVING count(DISTINCT "BillingCurrency") > 1 '
        'LIMIT 1',
        params,
    ).fetchone()
    if row:
        raise BillingDataQualityError({f"TaxCurrency[{row[0]}]": 1})


def _tax_rows_sql(source_sql: str, rate: str) -> str:
    return f"""
        WITH source AS ({source_sql}),
        tax_base AS (
            SELECT * FROM source WHERE "ChargeCategory" IS DISTINCT FROM 'Tax'
        ),
        tax_totals AS (
            SELECT
                "ServiceName",
                round(sum("BilledCost") * {rate}, 6) AS "BilledCost",
                round(sum("EffectiveCost") * {rate}, 6) AS "EffectiveCost",
                round(sum("ListCost") * {rate}, 6) AS "ListCost",
                round(sum("ContractedCost") * {rate}, 6) AS "ContractedCost",
                min("BillingPeriodStart") AS "BillingPeriodStart",
                max("BillingPeriodEnd") AS "BillingPeriodEnd",
                min("ChargePeriodStart") AS "ChargePeriodStart",
                max("ChargePeriodEnd") AS "ChargePeriodEnd"
            FROM tax_base
            GROUP BY "ServiceName"
        ),
        tax_representatives AS (
            SELECT * FROM tax_base
            QUALIFY row_number() OVER (
                PARTITION BY "ServiceName"
                ORDER BY "ChargePeriodStart", "ResourceId" NULLS LAST
            ) = 1
        ),
        tax_rows AS (
            SELECT representative.* REPLACE (
                totals."BilledCost" AS "BilledCost",
                totals."EffectiveCost" AS "EffectiveCost",
                totals."ListCost" AS "ListCost",
                totals."ContractedCost" AS "ContractedCost",
                totals."BillingPeriodStart" AS "BillingPeriodStart",
                totals."BillingPeriodEnd" AS "BillingPeriodEnd",
                totals."ChargePeriodStart" AS "ChargePeriodStart",
                totals."ChargePeriodEnd" AS "ChargePeriodEnd",
                'Tax' AS "ChargeCategory",
                representative."ServiceName" || ' tax' AS "ChargeDescription",
                NULL AS "ResourceId"
            )
            FROM tax_representatives AS representative
            JOIN tax_totals AS totals USING ("ServiceName")
        )
        SELECT * FROM tax_rows
    """


def _with_tax_rows(source_sql: str, rate: str) -> str:
    return f"""
        WITH source AS ({source_sql}),
        tax_rows AS ({_tax_rows_sql(source_sql, rate)})
        SELECT * FROM source
        UNION ALL
        SELECT * FROM tax_rows
    """


def query_billing(
    *,
    dataset: str,
    subs: list[SubscriptionConfig],
    period: str,
    where_sql: str,
    where_params: list[Any],
    page: int,
    page_size: int,
    include_tax: bool = True,
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    """Run a partition-pruned query. Returns rows, total, and cost summary."""
    settings = get_settings()
    con = get_connection()
    globs = _glob_for(settings, dataset, subs, period)
    if not globs:
        return [], 0, {"costsByCurrency": [], "includesDerivedTax": False}

    scan = _scan_expr(globs)
    raw_base = f"SELECT * FROM {scan} WHERE {where_sql}"
    base = _normalize_core_fields(raw_base)

    try:
        _validate_core_fields(con, base, where_params)
        detail_costs = con.execute(
            f'SELECT "BillingCurrency", count(*), sum("BilledCost") '
            f'FROM ({base}) GROUP BY "BillingCurrency"',
            where_params,
        ).fetchall()
        detail_total = sum(int(row_count) for _, row_count, _ in detail_costs)
    except duckdb.IOException:
        # Partition path does not exist yet -> treat as empty result.
        return [], 0, {"costsByCurrency": [], "includesDerivedTax": False}

    tax_sql: str | None = None
    tax_params: list[Any] = []
    tax_total = 0
    tax_costs: list[tuple[Any, Any, Any]] = []
    if settings.tax_enabled and include_tax:
        def tax_stats(sql: str, params: list[Any]) -> list[tuple[Any, Any, Any]]:
            return con.execute(
                f'SELECT "BillingCurrency", count(*), sum("BilledCost") '
                f'FROM ({sql}) GROUP BY "BillingCurrency"',
                params,
            ).fetchall()

        def use_detail_tax_fallback() -> tuple[
            str, list[Any], list[tuple[Any, Any, Any]]
        ]:
            tax_base = _normalize_core_fields(
                f"SELECT * FROM {scan} WHERE {tax_where_sql}"
            )
            _validate_tax_group_context(con, tax_base, [])
            fallback_sql = _tax_rows_sql(tax_base, str(settings.tax_rate))
            return fallback_sql, [], tax_stats(fallback_sql, [])

        basis_scan = _scan_expr(_tax_basis_glob_for(settings, dataset, subs, period))
        tax_where_sql = _inline_sql_params(where_sql, where_params)
        basis = f"SELECT * FROM {basis_scan} WHERE {tax_where_sql}"
        try:
            _validate_tax_group_context(con, basis, [])
            tax_sql = _tax_rows_sql(basis, str(settings.tax_rate))
            tax_params = []
            tax_costs = tax_stats(tax_sql, tax_params)
            tax_total = sum(int(row_count) for _, row_count, _ in tax_costs)
            if tax_total == 0 and detail_total > 0:
                tax_sql, tax_params, tax_costs = use_detail_tax_fallback()
                tax_total = sum(int(row_count) for _, row_count, _ in tax_costs)
        except duckdb.IOException:
            # Existing curated partitions may predate tax-basis generation.
            tax_sql, tax_params, tax_costs = use_detail_tax_fallback()
            tax_total = sum(int(row_count) for _, row_count, _ in tax_costs)

    costs_by_currency: dict[str, Decimal] = {}
    for currency, _, billed_cost in [*detail_costs, *tax_costs]:
        costs_by_currency[str(currency)] = costs_by_currency.get(
            str(currency), Decimal("0")
        ) + Decimal(str(billed_cost))
    summary = {
        "costsByCurrency": [
            {
                "billingCurrency": currency,
                "totalBilledCost": float(costs_by_currency[currency]),
            }
            for currency in sorted(costs_by_currency)
        ],
        "includesDerivedTax": tax_total > 0,
    }

    total = detail_total + tax_total
    offset = (page - 1) * page_size
    rows: list[dict[str, Any]] = []

    def fetch_page(
        sql: str, params: list[Any], limit: int, row_offset: int
    ) -> list[dict[str, Any]]:
        rel = con.execute(
            f"SELECT * FROM ({sql}) "
            'ORDER BY "ChargePeriodStart", "ServiceName", "ChargeCategory", '
            '"ResourceId" NULLS LAST, "ChargeDescription", "SkuId" '
            "LIMIT ? OFFSET ?",
            [*params, limit, row_offset],
        )
        columns = [description[0] for description in rel.description]
        return [dict(zip(columns, record)) for record in rel.fetchall()]

    if offset < detail_total:
        detail_limit = min(page_size, detail_total - offset)
        rows.extend(fetch_page(base, where_params, detail_limit, offset))
        tax_limit = page_size - detail_limit
        if tax_sql and tax_limit:
            rows.extend(fetch_page(tax_sql, tax_params, tax_limit, 0))
    elif tax_sql:
        rows.extend(fetch_page(tax_sql, tax_params, page_size, offset - detail_total))

    return rows, int(total), summary
