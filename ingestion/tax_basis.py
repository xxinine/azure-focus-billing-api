"""Build tax-basis summaries from existing curated detail partitions."""
from __future__ import annotations

import argparse

from app.config import get_settings
from app.db import get_connection, rebuild_tax_basis_partition


def run_tax_basis_backfill(
    dataset: str, period: str, subscription: str | None = None
) -> list[dict]:
    settings = get_settings()
    con = get_connection()
    subs = settings.subscriptions
    if subscription:
        subs = [sub for sub in subs if sub.subscription_key == subscription]
    if not subs:
        raise ValueError("no matching subscriptions in config")

    results = []
    for sub in subs:
        location, rows = rebuild_tax_basis_partition(
            con,
            settings,
            dataset=dataset,
            sub=sub,
            period=period,
        )
        results.append(
            {
                "subscriptionKey": sub.subscription_key,
                "dataset": dataset,
                "period": period,
                "rows": rows,
                "location": location,
            }
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=("daily", "monthly"), required=True)
    parser.add_argument("--period", required=True, help="YYYY-MM")
    parser.add_argument("--subscription", help="subscriptionKey; default=all")
    args = parser.parse_args()

    results = run_tax_basis_backfill(args.dataset, args.period, args.subscription)
    for result in results:
        print(
            f"[ok] {result['subscriptionKey']} {result['dataset']} "
            f"{result['period']}: {result['rows']} rows -> {result['location']}"
        )


if __name__ == "__main__":
    main()