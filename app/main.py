from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from .config import get_settings
from .routers import admin, billing
from .scheduler import shutdown_scheduler, start_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_scheduler()
    try:
        yield
    finally:
        shutdown_scheduler()


app = FastAPI(title="Azure FOCUS Billing Query API", version="0.1.0", lifespan=lifespan)
app.include_router(billing.router)
app.include_router(admin.router)

_static_dir = Path(__file__).parent / "static"
app.mount("/ui", StaticFiles(directory=str(_static_dir), html=True), name="ui")


@app.get("/")
def root() -> RedirectResponse:
    return RedirectResponse(url="/ui/")


@app.get("/health")
def health() -> dict:
    settings = get_settings()
    return {
        "status": "ok",
        "storageBackend": settings.storage_backend,
        "subscriptions": [
            {"subscriptionKey": s.subscription_key, "cloud": s.cloud}
            for s in settings.subscriptions
        ],
    }
