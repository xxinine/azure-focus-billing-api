"""Admin endpoints: manually trigger ingestion (used by the debug UI)."""
from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..config import get_settings
from ..scheduler import run_now, scheduler_status
from ingestion.ingest import run_ingest

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


class IngestRequest(BaseModel):
    dataset: Literal["daily", "monthly"]
    period: str  # YYYY-MM
    subscription: str | None = None


@router.post("/ingest")
def trigger_ingest(req: IngestRequest) -> dict:
    import re

    if not re.match(r"^\d{4}-\d{2}$", req.period):
        raise HTTPException(400, "period must be YYYY-MM")
    try:
        results = run_ingest(req.dataset, req.period, req.subscription)
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:  # surface ingestion errors to the UI
        raise HTTPException(500, f"{type(e).__name__}: {e}")
    return {"results": results, "totalRows": sum(r["rows"] for r in results)}


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
    try:
        return run_now()
    except Exception as e:
        raise HTTPException(500, f"{type(e).__name__}: {e}")


@router.get("/refresh/status")
def refresh_status() -> dict:
    return scheduler_status()
