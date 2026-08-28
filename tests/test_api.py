"""End-to-end test using the local storage backend + generated sample data."""
from __future__ import annotations

import importlib
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

SUBS = [
    {
        "subscriptionKey": "global-prod-01",
        "subscriptionId": "00000000-0000-0000-0000-000000000001",
        "cloud": "global",
        "dailyPrefix": "focus-cost/global-prod-01/autotsp-focus-cost-daily-parquet",
        "monthlyPrefix": "focus-cost/global-prod-01/autotsp-focus-cost-monthly-parquet",
    },
    {
        "subscriptionKey": "china-fin-02",
        "subscriptionId": "00000000-0000-0000-0000-000000000002",
        "cloud": "china",
        "dailyPrefix": "focus-cost/china-fin-02/autotsp-focus-cost-daily-parquet",
        "monthlyPrefix": "focus-cost/china-fin-02/autotsp-focus-cost-monthly-parquet",
    },
]


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    data_root = tmp_path_factory.mktemp("data")
    os.environ["STORAGE_BACKEND"] = "local"
    os.environ["LOCAL_DATA_ROOT"] = str(data_root)
    os.environ["CURATED_PREFIX"] = "curated/focus"
    os.environ["FOCUS_SUBSCRIPTIONS_CONFIG_JSON"] = json.dumps(SUBS)

    # Reload modules so settings pick up the env above.
    import app.config as config

    config.get_settings.cache_clear()
    import app.db as db

    importlib.reload(db)
    import scripts.gen_sample as gen

    importlib.reload(gen)

    con = db.get_connection()
    for sub in config.get_settings().subscriptions:
        for dataset, period, rows in [("daily", "2026-06", 200), ("monthly", "2026-06", 20)]:
            records = gen._generate(sub, dataset, period, rows)
            gen._write(con, sub, dataset, period, records)

    import app.main as main

    importlib.reload(main)
    return TestClient(main.app)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert len(r.json()["subscriptions"]) == 2


@pytest.mark.parametrize("tax_basis_prefix", ["", "   ", "/"])
def test_settings_rejects_empty_tax_basis_prefix(tax_basis_prefix):
    from pydantic import ValidationError

    from app.config import Settings

    with pytest.raises(ValidationError, match="TAX_BASIS_PREFIX must not be empty"):
        Settings(_env_file=None, tax_basis_prefix=tax_basis_prefix)


@pytest.mark.parametrize("tax_basis_prefix", ["curated/focus", "/curated/focus/"])
def test_settings_rejects_conflicting_tax_basis_prefix(tax_basis_prefix):
    from pydantic import ValidationError

    from app.config import Settings

    with pytest.raises(
        ValidationError, match="TAX_BASIS_PREFIX must differ from CURATED_PREFIX"
    ):
        Settings(
            _env_file=None,
            curated_prefix="curated/focus/",
            tax_basis_prefix=tax_basis_prefix,
        )


def test_daily_pagination(client, caplog):
    caplog.set_level("INFO", logger="uvicorn.error")
    r = client.get("/api/v1/billing/daily", params={"cloud": "global", "date": "2026-06-15", "pageSize": 10})
    assert r.status_code == 200
    body = r.json()
    assert body["elapsedMs"] >= 0
    assert body["pagination"]["pageSize"] == 10
    assert len(body["data"]) <= 10
    record = next(record for record in caplog.records if record.message.startswith("billing_query\n"))
    payload = json.loads(record.message.removeprefix("billing_query\n"))
    assert payload["status"] == "ok"
    assert payload["dataset"] == "daily"
    assert payload["summary"]["costsByCurrency"][0]["billingCurrency"] == "USD"
    assert payload["summary"]["includesDerivedTax"] is True
    assert payload["elapsed_ms"] >= 0
    # all rows belong to the requested day
    for row in body["data"]:
        assert str(row["ChargePeriodStart"]).startswith("2026-06-15")


def test_manual_ingest_returns_and_logs_elapsed_time(client, monkeypatch, caplog):
    import app.routers.admin as admin

    caplog.set_level("INFO", logger="uvicorn.error")
    monkeypatch.setattr(
        admin,
        "run_ingest",
        lambda dataset, period, subscription: [
            {
                "subscriptionKey": "global-prod-01",
                "dataset": dataset,
                "period": period,
                "status": "ok",
                "rows": 12,
            }
        ],
    )
    response = client.post(
        "/api/v1/admin/ingest",
        json={"dataset": "daily", "period": "2026-06"},
    )

    assert response.status_code == 200
    assert response.json()["elapsedMs"] >= 0
    assert "billing_ingest status=ok dataset=daily" in caplog.text
    assert "elapsed_ms=" in caplog.text


def test_monthly_aggregated_cloud(client):
    r = client.get("/api/v1/billing/monthly", params={"cloud": "china", "month": "2026-06", "pageSize": 50})
    assert r.status_code == 200
    body = r.json()
    assert body["pagination"]["total"] > 0


@pytest.fixture
def tax_enabled(monkeypatch):
    import app.config as config

    monkeypatch.setenv("TAX_ENABLED", "true")
    monkeypatch.setenv("TAX_RATE", "0.10")
    config.get_settings.cache_clear()
    yield
    config.get_settings.cache_clear()


def test_monthly_tax_is_grouped_by_service_and_paginated(client, tax_enabled):
    r = client.get(
        "/api/v1/billing/monthly",
        params={"cloud": "global", "month": "2026-06", "pageSize": 1000},
    )
    assert r.status_code == 200
    body = r.json()
    rows = body["data"]
    derived_tax = [
        row
        for row in rows
        if row["ChargeCategory"] == "Tax" and row["ResourceId"] is None
    ]
    original = [row for row in rows if row not in derived_tax]
    taxable = [row for row in original if row["ChargeCategory"] != "Tax"]
    service_names = {row["ServiceName"] for row in taxable}

    assert len(original) == 20
    assert len(derived_tax) == len(service_names)
    assert body["pagination"]["total"] == len(original) + len(service_names)
    assert {row["ServiceName"] for row in derived_tax} == service_names
    assert body["summary"]["includesDerivedTax"] is True
    assert body["summary"]["costsByCurrency"] == [
        {
            "billingCurrency": "USD",
            "totalBilledCost": pytest.approx(sum(row["BilledCost"] for row in rows)),
        }
    ]

    small_page = client.get(
        "/api/v1/billing/monthly",
        params={"cloud": "global", "month": "2026-06", "pageSize": 3},
    ).json()
    assert small_page["summary"] == body["summary"]

    expected_costs = defaultdict(lambda: defaultdict(float))
    for row in taxable:
        for column in ("BilledCost", "EffectiveCost", "ListCost", "ContractedCost"):
            expected_costs[row["ServiceName"]][column] += row[column]
    for row in derived_tax:
        for column in ("BilledCost", "EffectiveCost", "ListCost", "ContractedCost"):
            assert row[column] == pytest.approx(
                round(expected_costs[row["ServiceName"]][column] * 0.10, 6)
            )


def test_monthly_tax_can_be_disabled_per_request(client, tax_enabled):
    r = client.get(
        "/api/v1/billing/monthly",
        params={
            "cloud": "global",
            "month": "2026-06",
            "pageSize": 1000,
            "includeTax": "false",
        },
    )

    assert r.status_code == 200
    body = r.json()
    assert body["pagination"]["total"] == 20
    assert not any(
        row["ChargeCategory"] == "Tax" and row["ResourceId"] is None
        for row in body["data"]
    )
    assert body["summary"]["includesDerivedTax"] is False
    assert body["summary"]["costsByCurrency"] == [
        {
            "billingCurrency": "USD",
            "totalBilledCost": pytest.approx(
                sum(row["BilledCost"] for row in body["data"])
            ),
        }
    ]


def test_derived_tax_rows_follow_all_original_details(client, tax_enabled):
    first = client.get(
        "/api/v1/billing/monthly",
        params={"cloud": "global", "month": "2026-06", "pageSize": 20},
    ).json()
    second = client.get(
        "/api/v1/billing/monthly",
        params={
            "cloud": "global",
            "month": "2026-06",
            "page": 2,
            "pageSize": 20,
        },
    ).json()

    assert len(first["data"]) == 20
    assert all(row["ResourceId"] is not None for row in first["data"])
    assert second["data"]
    assert all(
        row["ChargeCategory"] == "Tax" and row["ResourceId"] is None
        for row in second["data"]
    )

    boundary = client.get(
        "/api/v1/billing/monthly",
        params={"cloud": "global", "month": "2026-06", "pageSize": 22},
    ).json()["data"]
    derived_positions = [
        index
        for index, row in enumerate(boundary)
        if row["ChargeCategory"] == "Tax" and row["ResourceId"] is None
    ]
    assert derived_positions == list(range(20, len(boundary)))


def test_daily_tax_basis_is_scoped_to_requested_date(client, tax_enabled):
    response = client.get(
        "/api/v1/billing/daily",
        params={"cloud": "global", "date": "2026-06-15", "pageSize": 1000},
    )

    assert response.status_code == 200
    rows = response.json()["data"]
    derived_tax = [
        row
        for row in rows
        if row["ChargeCategory"] == "Tax" and row["ResourceId"] is None
    ]
    original = [row for row in rows if row not in derived_tax]
    taxable_services = {
        row["ServiceName"] for row in original if row["ChargeCategory"] != "Tax"
    }
    assert {row["ServiceName"] for row in derived_tax} == taxable_services
    assert all(
        str(row["ChargePeriodStart"]).startswith("2026-06-15")
        for row in derived_tax
    )


def test_tax_filter_params_are_safely_inlined():
    from app.db import _inline_sql_params

    sql = _inline_sql_params(
        'CAST("ChargePeriodStart" AS DATE) = CAST(? AS DATE)',
        ["2026-08-01"],
    )
    assert "?" not in sql
    assert "CAST('2026-08-01' AS DATE)" in sql
    assert _inline_sql_params("value = ?", ["a'b"]) == "value = 'a''b'"


def test_tax_basis_normalizes_missing_provider_fields():
    import duckdb

    from app.config import SubscriptionConfig
    from app.db import build_tax_basis_sql
    from scripts.gen_sample import _as_relation, _row

    sub = SubscriptionConfig.model_validate(SUBS[0])
    start = datetime(2026, 6, 1)
    record = _row(sub, start, start + timedelta(days=1))
    record["ChargeCategory"] = "Usage"
    record["ServiceProviderName"] = None
    record["PublisherName"] = ""
    con = duckdb.connect(":memory:")
    con.register("charges", _as_relation(con, [record], list(record)))

    row = con.execute(
        f'SELECT "ServiceProviderName", "PublisherName" '
        f'FROM ({build_tax_basis_sql("SELECT * FROM charges", "daily")})'
    ).fetchone()
    assert row == ("Microsoft", "Microsoft")


def test_tax_rows_satisfy_core_field_contract(client, tax_enabled):
    r = client.get(
        "/api/v1/billing/monthly",
        params={"cloud": "global", "month": "2026-06", "pageSize": 1000},
    )
    assert r.status_code == 200
    rows = r.json()["data"]
    string_fields = (
        "BillingAccountId",
        "BillingCurrency",
        "ChargeCategory",
        "ServiceCategory",
        "ServiceSubcategory",
        "ServiceName",
        "ServiceProviderName",
        "InvoiceIssuerName",
        "ProviderName",
        "PublisherName",
    )
    required_fields = (
        "BilledCost",
        "EffectiveCost",
        "ListCost",
        "ContractedCost",
        "BillingPeriodStart",
        "BillingPeriodEnd",
        "ChargePeriodStart",
        "ChargePeriodEnd",
    )
    for row in rows:
        assert all(row[field] is not None for field in required_fields)
        assert all(str(row[field]).strip() for field in string_fields)
        if row["ChargeCategory"] not in ("Tax", "Credit", "Adjustment"):
            assert str(row["ResourceId"]).strip()


def test_tax_basis_excludes_only_existing_tax():
    import duckdb

    from app.config import SubscriptionConfig
    from app.db import _with_tax_rows
    from scripts.gen_sample import _as_relation, _row

    sub = SubscriptionConfig.model_validate(SUBS[0])
    start = datetime(2026, 6, 1)
    base = _row(sub, start, start + timedelta(days=1))
    records = []
    for category, cost in (("Tax", 100.0), ("Credit", -10.0), ("Adjustment", 5.0)):
        record = base.copy()
        record["ChargeCategory"] = category
        for column in ("BilledCost", "EffectiveCost", "ListCost", "ContractedCost"):
            record[column] = cost
        records.append(record)

    con = duckdb.connect(":memory:")
    con.register("charges", _as_relation(con, records, list(base)))
    rows = con.execute(
        f'SELECT "BilledCost", "ResourceId" FROM ({_with_tax_rows("SELECT * FROM charges", "0.10")}) '
        'WHERE "ChargeCategory" = \'Tax\''
    ).fetchall()

    derived = [cost for cost, resource_id in rows if resource_id is None]
    assert derived == [pytest.approx(-0.5)]


def test_tax_group_allows_multiple_non_currency_contexts():
    import duckdb

    from app.config import SubscriptionConfig
    from app.db import _validate_tax_group_context
    from scripts.gen_sample import _as_relation, _row

    sub = SubscriptionConfig.model_validate(SUBS[0])
    start = datetime(2026, 6, 1)
    base = _row(sub, start, start + timedelta(days=1))
    base["ServiceName"] = "Virtual Machines"
    records = []
    contexts = (
        ("account-1", "Security", "Microsoft", "Microsoft"),
        ("account-2", "Other", "Marketplace Publisher", "Partner"),
    )
    for account_id, category, publisher, provider in contexts:
        record = base.copy()
        record["BillingAccountId"] = account_id
        record["ServiceCategory"] = category
        record["PublisherName"] = publisher
        record["ProviderName"] = provider
        records.append(record)

    con = duckdb.connect(":memory:")
    con.register("charges", _as_relation(con, records, list(records[0])))

    _validate_tax_group_context(con, "SELECT * FROM charges", [])


def test_tax_group_rejects_multiple_currencies():
    import duckdb

    from app.config import SubscriptionConfig
    from app.db import BillingDataQualityError, _validate_tax_group_context
    from scripts.gen_sample import _as_relation, _row

    sub = SubscriptionConfig.model_validate(SUBS[0])
    start = datetime(2026, 6, 1)
    base = _row(sub, start, start + timedelta(days=1))
    base["ServiceName"] = "Virtual Machines"
    records = []
    for currency in ("USD", "CNY"):
        record = base.copy()
        record["BillingCurrency"] = currency
        records.append(record)

    con = duckdb.connect(":memory:")
    con.register("charges", _as_relation(con, records, list(records[0])))

    with pytest.raises(BillingDataQualityError) as exc_info:
        _validate_tax_group_context(con, "SELECT * FROM charges", [])
    assert exc_info.value.violations["TaxCurrency[Virtual Machines]"] == 1


def test_query_validation_rejects_blank_core_field():
    import duckdb

    from app.config import SubscriptionConfig
    from app.db import BillingDataQualityError, _validate_core_fields
    from scripts.gen_sample import _as_relation, _row

    sub = SubscriptionConfig.model_validate(SUBS[0])
    start = datetime(2026, 6, 1)
    record = _row(sub, start, start + timedelta(days=1))
    record["ServiceName"] = "   "
    con = duckdb.connect(":memory:")
    con.register("invalid_billing", _as_relation(con, [record], list(record)))

    with pytest.raises(BillingDataQualityError) as exc_info:
        _validate_core_fields(con, "SELECT * FROM invalid_billing", [])
    assert exc_info.value.violations["ServiceName"] == 1


def test_query_validation_allows_unknown_service_subcategory_pair():
    import duckdb

    from app.config import SubscriptionConfig
    from app.db import _validate_core_fields
    from scripts.gen_sample import _as_relation, _row

    sub = SubscriptionConfig.model_validate(SUBS[0])
    start = datetime(2026, 6, 1)
    record = _row(sub, start, start + timedelta(days=1))
    record["ServiceCategory"] = "Compute"
    record["ServiceSubcategory"] = "Object Storage"
    con = duckdb.connect(":memory:")
    con.register("invalid_service", _as_relation(con, [record], list(record)))

    _validate_core_fields(con, "SELECT * FROM invalid_service", [])


def test_query_normalizes_azure_missing_core_fields():
    import duckdb

    from app.config import SubscriptionConfig
    from app.db import _normalize_core_fields, _validate_core_fields
    from scripts.gen_sample import _as_relation, _row

    sub = SubscriptionConfig.model_validate(SUBS[0])
    start = datetime(2026, 6, 1)
    record = _row(sub, start, start + timedelta(days=1))
    record.update(
        {
            "ServiceCategory": "Other",
            "ServiceSubcategory": "Other",
            "ServiceProviderName": None,
            "PublisherName": "",
        }
    )
    con = duckdb.connect(":memory:")
    con.register("azure_billing", _as_relation(con, [record], list(record)))
    normalized = _normalize_core_fields("SELECT * FROM azure_billing")

    _validate_core_fields(con, normalized, [])
    row = con.execute(
        f'SELECT "ServiceProviderName", "PublisherName", "ServiceSubcategory" '
        f"FROM ({normalized})"
    ).fetchone()
    assert row == ("Microsoft", "Microsoft", "Other")


def test_monthly_single_subscription(client):
    r = client.get(
        "/api/v1/billing/monthly",
        params={
            "cloud": "global",
            "month": "2026-06",
            "subscriptionId": "00000000-0000-0000-0000-000000000001",
        },
    )
    assert r.status_code == 200
    for row in r.json()["data"]:
        assert row["SubAccountId"] == "00000000-0000-0000-0000-000000000001"


def test_bad_date(client):
    r = client.get("/api/v1/billing/daily", params={"cloud": "global", "date": "2026/06/15"})
    assert r.status_code == 400


def test_unknown_cloud_has_no_subs(client):
    r = client.get("/api/v1/billing/daily", params={"cloud": "global", "date": "2099-01-01"})
    assert r.status_code == 200
    assert r.json()["pagination"]["total"] == 0
