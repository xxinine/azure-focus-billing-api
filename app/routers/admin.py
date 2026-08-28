"""Admin endpoints: manually trigger ingestion (used by the debug UI)."""
from __future__ import annotations

import logging
from time import perf_counter
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..config import get_settings
from ..scheduler import run_now, scheduler_status
from ingestion.ingest import run_ingest

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])
logger = logging.getLogger("uvicorn.error")


class IngestRequest(BaseModel):
    dataset: Literal["daily", "monthly"]
    period: str  # YYYY-MM
    subscription: str | None = None


@router.post("/ingest")
def trigger_ingest(req: IngestRequest) -> dict:
    import re

    started = perf_counter()
    if not re.match(r"^\d{4}-\d{2}$", req.period):
        raise HTTPException(400, "period must be YYYY-MM")
    try:
        results = run_ingest(req.dataset, req.period, req.subscription)
    except ValueError as e:
        _log_ingest(req, started, "error")
        raise HTTPException(404, str(e))
    except Exception as e:  # surface ingestion errors to the UI
        _log_ingest(req, started, "error")
        raise HTTPException(500, f"{type(e).__name__}: {e}")
    elapsed_ms = _elapsed_ms(started)
    total_rows = sum(r["rows"] for r in results)
    logger.info(
        "billing_ingest status=ok dataset=%s period=%s subscription=%s "
        "rows=%s elapsed_ms=%.2f",
        req.dataset, req.period, req.subscription or "all", total_rows, elapsed_ms,
    )
    return {"results": results, "totalRows": total_rows, "elapsedMs": elapsed_ms}


@router.get("/meta")
def meta() -> dict:
    settings = get_settings()
    subs = settings.subscriptions
    return {
        "clouds": sorted({s.cloud for s in subs}),
        "subscriptions": [
            {"subscriptionKey": s.subscription_key, "subscriptionId": s.subscription_id, "cloud": s.cloud}
            for s in subs
        ],
        "defaultPageSize": settings.default_page_size,
        "maxPageSize": settings.max_page_size,
    }


@router.post("/refresh")
def trigger_refresh() -> dict:
    """Run the daily refresh now (daily current month + monthly last month)."""
    started = perf_counter()
    try:
        result = run_now()
    except Exception as e:
        logger.info("billing_refresh status=error elapsed_ms=%.2f", _elapsed_ms(started))
        raise HTTPException(500, f"{type(e).__name__}: {e}")
    elapsed_ms = _elapsed_ms(started)
    result["elapsedMs"] = elapsed_ms
    logger.info(
        "billing_refresh status=ok rows=%s elapsed_ms=%.2f",
        result.get("totalRows", 0), elapsed_ms,
    )
    return result


@router.get("/refresh/status")
def refresh_status() -> dict:
    return scheduler_status()


def _elapsed_ms(started: float) -> float:
    return round((perf_counter() - started) * 1000, 2)


def _log_ingest(req: IngestRequest, started: float, status: str) -> None:
    logger.info(
        "billing_ingest status=%s dataset=%s period=%s subscription=%s elapsed_ms=%.2f",
        status,
        req.dataset,
        req.period,
        req.subscription or "all",
        _elapsed_ms(started),
    )
