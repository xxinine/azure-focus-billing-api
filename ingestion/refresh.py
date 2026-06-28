"""Daily refresh: pull the latest daily (current month) and monthly (last
completed month) billing for all configured subscriptions.

Idempotent — safe to run repeatedly / backfill. Reuses run_ingest().

Usage:
  python -m ingestion.refresh
  python -m ingestion.refresh --now 2026-06-28   # simulate a date
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone

from ingestion.ingest import run_ingest


def compute_periods(now: datetime | None = None) -> tuple[str, str]:
    """Return (daily_period, monthly_period) as YYYY-MM.

    daily   = current month (month-to-date export)
    monthly = last completed month (Azure monthly export = previous month)
    """
    now = now or datetime.now(timezone.utc)
    daily_period = f"{now.year:04d}-{now.month:02d}"
    last_month_end = date(now.year, now.month, 1) - timedelta(days=1)
    monthly_period = f"{last_month_end.year:04d}-{last_month_end.month:02d}"
    return daily_period, monthly_period


def run_refresh(now: datetime | None = None) -> dict:
    daily_period, monthly_period = compute_periods(now)
    started = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []

    for dataset, period in (("daily", daily_period), ("monthly", monthly_period)):
        try:
            results.extend(run_ingest(dataset, period))
        except Exception as e:  # keep going; record the failure
            results.append(
                {
                    "dataset": dataset,
                    "period": period,
                    "status": "error",
                    "rows": 0,
                    "detail": f"{type(e).__name__}: {e}",
                }
            )

    return {
        "startedAt": started,
        "finishedAt": datetime.now(timezone.utc).isoformat(),
        "dailyPeriod": daily_period,
        "monthlyPeriod": monthly_period,
        "totalRows": sum(r.get("rows", 0) for r in results),
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--now", help="ISO date to simulate, e.g. 2026-06-28")
    args = parser.parse_args()
    now = None
    if args.now:
        now = datetime.fromisoformat(args.now).replace(tzinfo=timezone.utc)
    summary = run_refresh(now)
    print(
        f"refresh done: daily={summary['dailyPeriod']} "
        f"monthly={summary['monthlyPeriod']} totalRows={summary['totalRows']}"
    )
    for r in summary["results"]:
        print(
            f"  {r.get('subscriptionKey', '-')} {r['dataset']} {r['period']}: "
            f"{r['status']} ({r.get('rows', 0)} rows)"
        )


if __name__ == "__main__":
    main()
