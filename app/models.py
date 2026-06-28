"""API response models."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class Pagination(BaseModel):
    page: int
    pageSize: int
    total: int
    totalPages: int


class BillingResponse(BaseModel):
    data: list[dict[str, Any]]
    pagination: Pagination
