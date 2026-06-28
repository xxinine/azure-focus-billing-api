"""Generate FOCUS 1.3-shaped sample data into the CURATED layout.

Writes directly to curated partitions (local or Azure Blob, per STORAGE_BACKEND)
so the API can be exercised without real exports. For real data, use the
ingestion pipeline instead.

Usage:
  python -m scripts.gen_sample --dataset daily --period 2026-06 --rows 500
  python -m scripts.gen_sample --dataset monthly --period 2026-06 --rows 50
"""
from __future__ import annotations

import argparse
import random
from datetime import date, datetime, timedelta

import duckdb

from app.config import SubscriptionConfig, get_settings
from app.db import get_connection, write_partition
from app.schema import FOCUS_1_3_COLUMNS

SERVICES = [
    ("Virtual Machines", "Compute"),
    ("Azure SQL Database", "Databases"),
    ("Storage Accounts", "Storage"),
    ("Azure Kubernetes Service", "Compute"),
    ("Application Gateway", "Networking"),
]
REGIONS = [("eastus", "East US"), ("chinaeast2", "China East 2"), ("westeurope", "West Europe")]
CHARGE_CATEGORIES = ["Usage", "Purchase", "Tax"]


def _row(sub: SubscriptionConfig, charge_start: datetime, charge_end: datetime) -> dict:
    svc, cat = random.choice(SERVICES)
    region_id, region_name = random.choice(REGIONS)
    qty = round(random.uniform(0.5, 100), 4)
    unit_price = round(random.uniform(0.01, 5), 6)
    billed = round(qty * unit_price, 6)
    currency = "CNY" if sub.cloud == "china" else "USD"
    rg = random.choice(["rg-app", "rg-data", "rg-net"])

    values = {c: None for c in FOCUS_1_3_COLUMNS}
    values.update(
        {
            "BilledCost": billed,
            "EffectiveCost": billed,
            "ListCost": round(billed * 1.1, 6),
            "ContractedCost": billed,
            "ListUnitPrice": unit_price,
            "ContractedUnitPrice": unit_price,
            "BillingCurrency": currency,
            "BillingAccountId": f"ba-{sub.subscription_key}",
            "BillingAccountName": f"Account {sub.subscription_key}",
            "BillingPeriodStart": charge_start.replace(day=1),
            "BillingPeriodEnd": (charge_start.replace(day=1) + timedelta(days=32)).replace(day=1),
            "ChargePeriodStart": charge_start,
            "ChargePeriodEnd": charge_end,
            "ChargeCategory": random.choice(CHARGE_CATEGORIES),
            "ChargeClass": None,
            "ChargeDescription": f"{svc} usage",
            "ChargeFrequency": "Usage-Based",
            "ConsumedQuantity": qty,
            "ConsumedUnit": "Hours",
            "PricingQuantity": qty,
            "PricingUnit": "Hours",
            "ProviderName": "Microsoft",
            "PublisherName": "Microsoft",
            "InvoiceIssuerName": "Microsoft",
            "RegionId": region_id,
            "RegionName": region_name,
            "ResourceId": f"/subscriptions/{sub.subscription_id}/resourceGroups/{rg}/providers/{svc}/res-{random.randint(1, 50)}",
            "ResourceName": f"res-{random.randint(1, 50)}",
            "ResourceType": svc,
            "ServiceName": svc,
            "ServiceCategory": cat,
            "SkuId": f"sku-{random.randint(1000, 9999)}",
            "SkuPriceId": f"price-{random.randint(1000, 9999)}",
            "SubAccountId": sub.subscription_id,
            "SubAccountName": sub.subscription_key,
            "SubAccountType": "Subscription",
            "PricingCurrency": currency,
            "Tags": '{"env":"prod","team":"finops"}',
        }
    )
    return values


def _generate(sub: SubscriptionConfig, dataset: str, period: str, rows: int) -> list[dict]:
    year, month = (int(x) for x in period.split("-"))
    first = date(year, month, 1)
    next_month = (first + timedelta(days=32)).replace(day=1)
    days = (next_month - first).days

    out = []
    for _ in range(rows):
        if dataset == "daily":
            d = first + timedelta(days=random.randint(0, days - 1))
            cs = datetime(d.year, d.month, d.day)
            ce = cs + timedelta(days=1)
        else:  # monthly: charge period spans the whole month
            cs = datetime(first.year, first.month, first.day)
            ce = datetime(next_month.year, next_month.month, next_month.day)
        out.append(_row(sub, cs, ce))
    return out


def _write(con: duckdb.DuckDBPyConnection, sub, dataset, period, records) -> None:
    settings = get_settings()
    cols = FOCUS_1_3_COLUMNS
    con.register("tmp_sample", _as_relation(con, records, cols))
    location, n = write_partition(
        con,
        settings,
        dataset=dataset,
        cloud=sub.cloud,
        subscription_key=sub.subscription_key,
        period=period,
        select_sql="SELECT * FROM tmp_sample",
    )
    con.unregister("tmp_sample")
    print(f"  [ok] {sub.subscription_key} {dataset} {period}: {n} rows -> {location}")


def _as_relation(con, records, cols):
    import pyarrow as pa

    columns = {c: [r[c] for r in records] for c in cols}
    return pa.table(columns)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=["daily", "monthly"], required=True)
    parser.add_argument("--period", required=True, help="YYYY-MM")
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--subscription", help="subscriptionKey; default=all")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    settings = get_settings()
    con = get_connection()
    subs = settings.subscriptions
    if args.subscription:
        subs = [s for s in subs if s.subscription_key == args.subscription]
    if not subs:
        raise SystemExit("no matching subscriptions in config")

    for sub in subs:
        records = _generate(sub, args.dataset, args.period, args.rows)
        _write(con, sub, args.dataset, args.period, records)


if __name__ == "__main__":
    main()
