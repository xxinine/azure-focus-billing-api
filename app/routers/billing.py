"""Daily / monthly billing query endpoints.

`cloud` unifies multiple independent subscriptions at the query layer only.
"""
from __future__ import annotations

import json
import logging
import math
import re
from time import perf_counter
from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from ..config import get_settings
from ..db import BillingDataQualityError, query_billing
from ..models import BillingResponse, BillingSummary, Pagination

router = APIRouter(prefix="/api/v1/billing", tags=["billing"])
logger = logging.getLogger("uvicorn.error")

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


def _paginate_args(page: int, page_size: int) -> tuple[int, int]:
    settings = get_settings()
    if page < 1:
        raise HTTPException(400, "page must be >= 1")
    if page_size < 1:
        raise HTTPException(400, "pageSize must be >= 1")
    if page_size > settings.max_page_size:
        raise HTTPException(400, f"pageSize must be <= {settings.max_page_size}")
    return page, page_size


@router.get("/daily", response_model=BillingResponse)
def get_daily(
    cloud: Literal["china", "global"],
    date: str = Query(..., description="YYYY-MM-DD"),
    subscriptionId: str | None = None,
    page: int = 1,
    pageSize: int | None = None,
    includeTax: bool = True,
) -> BillingResponse:
    started = perf_counter()
    if not _DATE_RE.match(date):
        raise HTTPException(400, "date must be YYYY-MM-DD")
    settings = get_settings()
    page, page_size = _paginate_args(page, pageSize or settings.default_page_size)

    subs = settings.subscriptions_for(cloud, subscriptionId)
    if not subs:
        raise HTTPException(404, f"no subscriptions configured for cloud={cloud}")

    period = date[:7]
    where = "CAST(\"ChargePeriodStart\" AS DATE) = CAST(? AS DATE)"
    try:
        rows, total, summary = _query_or_data_quality_error(
            dataset="daily",
            subs=subs,
            period=period,
            where_sql=where,
            where_params=[date],
            page=page,
            page_size=page_size,
            include_tax=includeTax,
        )
    except Exception:
        elapsed_ms = _elapsed_ms(started)
        _log_query(
            status="error",
            dataset="daily",
            cloud=cloud,
            period=date,
            page=page,
            page_size=page_size,
            include_tax=includeTax,
            elapsed_ms=elapsed_ms,
        )
        raise
    elapsed_ms = _elapsed_ms(started)
    _log_query(
        status="ok",
        dataset="daily",
        cloud=cloud,
        period=date,
        page=page,
        page_size=page_size,
        include_tax=includeTax,
        rows=len(rows),
        total=total,
        summary=summary,
        elapsed_ms=elapsed_ms,
    )
    return _build_response(rows, total, summary, page, page_size, elapsed_ms)


@router.get("/monthly", response_model=BillingResponse)
def get_monthly(
    cloud: Literal["china", "global"],
    month: str = Query(..., description="YYYY-MM"),
    subscriptionId: str | None = None,
    page: int = 1,
    pageSize: int | None = None,
    includeTax: bool = True,
) -> BillingResponse:
    started = perf_counter()
    if not _MONTH_RE.match(month):
        raise HTTPException(400, "month must be YYYY-MM")
    settings = get_settings()
    page, page_size = _paginate_args(page, pageSize or settings.default_page_size)

    subs = settings.subscriptions_for(cloud, subscriptionId)
    if not subs:
        raise HTTPException(404, f"no subscriptions configured for cloud={cloud}")

    where = "strftime(\"BillingPeriodStart\", '%Y-%m') = ?"
    try:
        rows, total, summary = _query_or_data_quality_error(
            dataset="monthly",
            subs=subs,
            period=month,
            where_sql=where,
            where_params=[month],
            page=page,
            page_size=page_size,
            include_tax=includeTax,
        )
    except Exception:
        elapsed_ms = _elapsed_ms(started)
        _log_query(
            status="error",
            dataset="monthly",
            cloud=cloud,
            period=month,
            page=page,
            page_size=page_size,
            include_tax=includeTax,
            elapsed_ms=elapsed_ms,
        )
        raise
    elapsed_ms = _elapsed_ms(started)
    _log_query(
        status="ok",
        dataset="monthly",
        cloud=cloud,
        period=month,
        page=page,
        page_size=page_size,
        include_tax=includeTax,
        rows=len(rows),
        total=total,
        summary=summary,
        elapsed_ms=elapsed_ms,
    )
    return _build_response(rows, total, summary, page, page_size, elapsed_ms)


def _query_or_data_quality_error(**kwargs):
    try:
        return query_billing(**kwargs)
    except BillingDataQualityError as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "BILLING_DATA_QUALITY_ERROR",
                "message": str(exc),
                "violations": exc.violations,
            },
        ) from exc


def _elapsed_ms(started: float) -> float:
    return round((perf_counter() - started) * 1000, 2)


def _log_query(**fields: object) -> None:
    logger.info(
        "billing_query\n%s",
        json.dumps(fields, ensure_ascii=False, indent=2, default=str),
    )


def _build_response(rows, total, summary, page, page_size, elapsed_ms) -> BillingResponse:
    return BillingResponse(
        data=rows,
        pagination=Pagination(
            page=page,
            pageSize=page_size,
            total=total,
            totalPages=math.ceil(total / page_size) if page_size else 0,
        ),
        summary=BillingSummary.model_validate(summary),
        elapsedMs=elapsed_ms,
    )
