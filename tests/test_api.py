"""End-to-end test using the local storage backend + generated sample data."""
from __future__ import annotations

import importlib
import json
import os

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


def test_daily_pagination(client):
    r = client.get("/api/v1/billing/daily", params={"cloud": "global", "date": "2026-06-15", "pageSize": 10})
    assert r.status_code == 200
    body = r.json()
    assert body["pagination"]["pageSize"] == 10
    assert len(body["data"]) <= 10
    # all rows belong to the requested day
    for row in body["data"]:
        assert str(row["ChargePeriodStart"]).startswith("2026-06-15")


def test_monthly_aggregated_cloud(client):
    r = client.get("/api/v1/billing/monthly", params={"cloud": "china", "month": "2026-06", "pageSize": 50})
    assert r.status_code == 200
    body = r.json()
    assert body["pagination"]["total"] > 0


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
