"""Daily / monthly billing query endpoints.

`cloud` unifies multiple independent subscriptions at the query layer only.
"""
from __future__ import annotations

import math
import re
from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from ..config import get_settings
from ..db import query_billing
from ..models import BillingResponse, Pagination

router = APIRouter(prefix="/api/v1/billing", tags=["billing"])

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
) -> BillingResponse:
    if not _DATE_RE.match(date):
        raise HTTPException(400, "date must be YYYY-MM-DD")
    settings = get_settings()
    page, page_size = _paginate_args(page, pageSize or settings.default_page_size)

    subs = settings.subscriptions_for(cloud, subscriptionId)
    if not subs:
        raise HTTPException(404, f"no subscriptions configured for cloud={cloud}")

    period = date[:7]
    where = "CAST(\"ChargePeriodStart\" AS DATE) = CAST(? AS DATE)"
    rows, total = query_billing(
        dataset="daily",
        subs=subs,
        period=period,
        where_sql=where,
        where_params=[date],
        page=page,
        page_size=page_size,
    )
    return _build_response(rows, total, page, page_size)


@router.get("/monthly", response_model=BillingResponse)
def get_monthly(
    cloud: Literal["china", "global"],
    month: str = Query(..., description="YYYY-MM"),
    subscriptionId: str | None = None,
    page: int = 1,
    pageSize: int | None = None,
) -> BillingResponse:
    if not _MONTH_RE.match(month):
        raise HTTPException(400, "month must be YYYY-MM")
    settings = get_settings()
    page, page_size = _paginate_args(page, pageSize or settings.default_page_size)

    subs = settings.subscriptions_for(cloud, subscriptionId)
    if not subs:
        raise HTTPException(404, f"no subscriptions configured for cloud={cloud}")

    where = "strftime(\"BillingPeriodStart\", '%Y-%m') = ?"
    rows, total = query_billing(
        dataset="monthly",
        subs=subs,
        period=month,
        where_sql=where,
        where_params=[month],
        page=page,
        page_size=page_size,
    )
    return _build_response(rows, total, page, page_size)


def _build_response(rows, total, page, page_size) -> BillingResponse:
    return BillingResponse(
        data=rows,
        pagination=Pagination(
            page=page,
            pageSize=page_size,
            total=total,
            totalPages=math.ceil(total / page_size) if page_size else 0,
        ),
    )
