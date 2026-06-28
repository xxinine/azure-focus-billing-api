"""Create/Update per-subscription Azure Cost Management FOCUS exports.

One export config per subscription (daily month-to-date + monthly last month),
format Parquet + Snappy, written to the configured Blob container.

Notes:
- FOCUS dataset version is set to the requested value; if a region/cloud does
  not yet support 1.3 it falls back per FALLBACK_VERSIONS and logs a warning.
- Azure China (21Vianet) uses a different ARM endpoint/credential scope; pass
  --cloud-endpoint accordingly.

Usage:
  python -m ingestion.export_setup --subscription global-prod-01 \
      --focus-version 1.3 --frequency daily
"""
from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request

from azure.identity import DefaultAzureCredential

from app.config import SubscriptionConfig, get_settings

API_VERSION = "2025-03-01"
FALLBACK_VERSIONS = ["1.3", "1.2-preview", "1.0r2"]

# ARM endpoints + token scopes per cloud.
CLOUD_ARM = {
    "global": ("https://management.azure.com", "https://management.azure.com/.default"),
    "china": ("https://management.chinacloudapi.cn", "https://management.chinacloudapi.cn/.default"),
}


def _recurrence(frequency: str) -> str:
    return "Daily" if frequency == "daily" else "Monthly"


def _schedule(frequency: str) -> dict:
    # Daily = month-to-date; Monthly = last completed month.
    return {
        "status": "Active",
        "recurrence": _recurrence(frequency),
        "recurrencePeriod": {"from": "2024-01-01T00:00:00Z", "to": "2030-12-31T00:00:00Z"},
    }


def _export_payload(
    sub: SubscriptionConfig, settings, frequency: str, focus_version: str
) -> dict:
    prefix = sub.daily_prefix if frequency == "daily" else sub.monthly_prefix
    timeframe = "MonthToDate" if frequency == "daily" else "TheLastMonth"
    account_id = settings.blob_account_url.split("//", 1)[-1].split(".", 1)[0]
    return {
        "properties": {
            "schedule": _schedule(frequency),
            "format": "Parquet",
            "partitionData": True,
            "compressionMode": "Snappy",
            "dataOverwriteBehavior": "OverwritePreviousReport",
            "deliveryInfo": {
                "destination": {
                    "resourceId": (
                        f"/subscriptions/{sub.subscription_id}/resourceGroups/"
                        f"<rg>/providers/Microsoft.Storage/storageAccounts/{account_id}"
                    ),
                    "container": settings.blob_container,
                    "rootFolderPath": prefix,
                }
            },
            "definition": {
                "type": "FocusCost",
                "timeframe": timeframe,
                "dataSet": {"configuration": {"dataVersion": focus_version}},
            },
        }
    }


def _put(url: str, token: str, payload: dict) -> tuple[int, str]:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        method="PUT",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def create_export(sub: SubscriptionConfig, frequency: str, focus_version: str) -> None:
    settings = get_settings()
    arm, scope = CLOUD_ARM[sub.cloud]
    cred = DefaultAzureCredential()
    token = cred.get_token(scope).token

    export_name = f"focus-{frequency}-{sub.subscription_key}"
    scope_id = f"subscriptions/{sub.subscription_id}"
    url = (
        f"{arm}/{scope_id}/providers/Microsoft.CostManagement/"
        f"exports/{export_name}?api-version={API_VERSION}"
    )

    versions = [focus_version] + [v for v in FALLBACK_VERSIONS if v != focus_version]
    for ver in versions:
        payload = _export_payload(sub, settings, frequency, ver)
        status, body = _put(url, token, payload)
        if status in (200, 201):
            print(f"[ok] {export_name} created with FOCUS {ver}")
            return
        if "dataVersion" in body or "version" in body.lower():
            print(f"[warn] FOCUS {ver} not supported here, trying fallback...")
            continue
        raise SystemExit(f"[error] {export_name}: HTTP {status} {body}")
    raise SystemExit(f"[error] {export_name}: no supported FOCUS version among {versions}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--subscription", help="subscriptionKey; default=all")
    parser.add_argument("--frequency", choices=["daily", "monthly"], default="daily")
    parser.add_argument("--focus-version", default="1.3")
    args = parser.parse_args()

    settings = get_settings()
    subs = settings.subscriptions
    if args.subscription:
        subs = [s for s in subs if s.subscription_key == args.subscription]
    if not subs:
        raise SystemExit("no matching subscriptions in config")

    for sub in subs:
        create_export(sub, args.frequency, args.focus_version)


if __name__ == "__main__":
    main()
