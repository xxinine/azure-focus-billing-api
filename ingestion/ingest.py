"""Ingestion: per-subscription Raw FOCUS Parquet -> normalized Curated Parquet.

Idempotent: each (dataset, subscription, period) partition is fully overwritten,
matching Azure export's month-to-date overwrite semantics.

Usage:
  python -m ingestion.ingest --dataset daily --period 2026-06
  python -m ingestion.ingest --dataset monthly --period 2026-06 --subscription china-fin-02
"""
from __future__ import annotations

import argparse

import duckdb

from app.config import SubscriptionConfig, get_settings
from app.db import get_connection, write_partition
from app.schema import build_normalization_select


def _raw_glob(settings, sub: SubscriptionConfig, dataset: str, period: str) -> str:
    prefix = sub.daily_prefix if dataset == "daily" else sub.monthly_prefix
    if settings.storage_backend == "local":
        base = f"{settings.local_data_root.rstrip('/')}/{prefix}"
    else:
        base = f"az://{settings.blob_container}/{prefix}"
    # Azure export run folders are named "YYYYMMDD-YYYYMMDD" per billing month;
    # scope the glob to the target month, then recurse through the run/partition.
    ym = period.replace("-", "")  # e.g. 2026-06 -> 202606
    return f"{base}/{ym}01-*/**/*.parquet"


def ingest_partition(
    con: duckdb.DuckDBPyConnection,
    sub: SubscriptionConfig,
    dataset: str,
    period: str,
) -> dict:
    settings = get_settings()
    raw_glob = _raw_glob(settings, sub, dataset, period)

    try:
        desc = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true)",
            [raw_glob],
        ).fetchall()
    except duckdb.IOException as exc:
        # Only a genuine "no files match the glob" is a legitimate skip.
        # Auth/permission/network errors (e.g. AuthorizationPermissionMismatch)
        # must surface instead of being silently reported as missing data.
        if "no files found" not in str(exc).lower():
            raise
        print(f"  [skip] no raw files: {raw_glob}")
        return {
            "subscriptionKey": sub.subscription_key,
            "cloud": sub.cloud,
            "dataset": dataset,
            "period": period,
            "rows": 0,
            "status": "skipped",
            "detail": "no raw files for period",
        }

    source_cols = [r[0] for r in desc]
    select_cols = build_normalization_select(source_cols)
    select_sql = (
        f"SELECT\n  {select_cols}\n"
        f"FROM read_parquet('{raw_glob}', union_by_name=true)"
    )

    location, count = write_partition(
        con,
        settings,
        dataset=dataset,
        cloud=sub.cloud,
        subscription_key=sub.subscription_key,
        period=period,
        select_sql=select_sql,
    )
    print(f"  [ok] {sub.subscription_key} {dataset} {period}: {count} rows -> {location}")
    return {
        "subscriptionKey": sub.subscription_key,
        "cloud": sub.cloud,
        "dataset": dataset,
        "period": period,
        "rows": int(count),
        "status": "ok",
        "location": location,
    }


def run_ingest(
    dataset: str, period: str, subscription: str | None = None
) -> list[dict]:
    """Reusable ingestion entrypoint for CLI, API trigger, and scheduler.

    dataset: "daily" | "monthly"; period: "YYYY-MM"; subscription: subscriptionKey.
    Returns one result dict per processed subscription.
    """
    settings = get_settings()
    con = get_connection()
    subs = settings.subscriptions
    if subscription:
        subs = [s for s in subs if s.subscription_key == subscription]
    if not subs:
        raise ValueError("no matching subscriptions in config")

    return [ingest_partition(con, sub, dataset, period) for sub in subs]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=["daily", "monthly"], required=True)
    parser.add_argument("--period", required=True, help="YYYY-MM")
    parser.add_argument("--subscription", help="subscriptionKey; default=all")
    args = parser.parse_args()

    try:
        results = run_ingest(args.dataset, args.period, args.subscription)
    except ValueError as e:
        raise SystemExit(str(e))
    total = sum(r["rows"] for r in results)
    print(f"done: {total} rows ingested")


if __name__ == "__main__":
    main()
