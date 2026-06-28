"""In-process daily scheduler (APScheduler), enabled via env.

Runs the daily refresh at a fixed UTC time when SCHEDULER_ENABLED=true. This is
convenient for single-instance deployments. For multi-instance or external
scheduling, disable it and drive `python -m ingestion.refresh` via cron/systemd.
"""
from __future__ import annotations

import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import get_settings

_scheduler: AsyncIOScheduler | None = None
last_run: dict | None = None

JOB_ID = "daily_refresh"


def run_now() -> dict:
    """Run a refresh synchronously and record it as the last run."""
    global last_run
    from ingestion.refresh import run_refresh

    last_run = run_refresh()
    return last_run


async def _async_job() -> None:
    await asyncio.to_thread(run_now)


def start_scheduler() -> AsyncIOScheduler | None:
    global _scheduler
    settings = get_settings()
    if not settings.scheduler_enabled:
        return None
    _scheduler = AsyncIOScheduler(timezone="UTC")
    _scheduler.add_job(
        _async_job,
        CronTrigger(hour=settings.scheduler_hour, minute=settings.scheduler_minute, timezone="UTC"),
        id=JOB_ID,
        replace_existing=True,
        misfire_grace_time=3600,
    )
    _scheduler.start()
    return _scheduler


def shutdown_scheduler() -> None:
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        _scheduler = None


def scheduler_status() -> dict:
    settings = get_settings()
    info = {
        "enabled": settings.scheduler_enabled,
        "scheduleUtc": f"{settings.scheduler_hour:02d}:{settings.scheduler_minute:02d}",
        "nextRun": None,
        "lastRun": last_run,
    }
    if _scheduler:
        job = _scheduler.get_job(JOB_ID)
        if job and job.next_run_time:
            info["nextRun"] = job.next_run_time.isoformat()
    return info
