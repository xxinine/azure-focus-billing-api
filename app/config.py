"""Application configuration: env-driven, multi-subscription aware.

`cloud` (china/global) is only an aggregation label used at the query layer.
Export and ingestion are configured per subscription.
"""
from __future__ import annotations

import json
from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

CloudName = Literal["china", "global"]


class SubscriptionConfig(BaseModel):
    """One independent configuration set per subscription."""

    subscription_key: str = Field(alias="subscriptionKey")
    subscription_id: str = Field(alias="subscriptionId")
    cloud: CloudName
    daily_prefix: str = Field(alias="dailyPrefix")
    monthly_prefix: str = Field(alias="monthlyPrefix")

    model_config = SettingsConfigDict(populate_by_name=True)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Raw export storage (where Cost Management exports land).
    blob_account_url: str = "https://<your-storage-account>.blob.core.windows.net"
    blob_container: str = "report"

    # Curated storage (normalized FOCUS 1.3 parquet). Defaults to the raw
    # account/container when not set, so users may keep them the same.
    curated_account_url: str | None = None
    curated_container: str | None = None
    curated_prefix: str = "curated/focus"

    azure_storage_auth_mode: Literal[
        "service_principal", "managed_identity", "sas", "connection_string"
    ] = "service_principal"
    azure_storage_connection_string: str | None = None
    azure_storage_sas_token: str | None = None

    # Service principal (app registration) credentials.
    azure_tenant_id: str | None = None
    azure_client_id: str | None = None
    azure_client_secret: str | None = None
    azure_client_certificate_path: str | None = None

    storage_backend: Literal["azure_blob", "local"] = "azure_blob"
    local_data_root: str = "./data"

    # Subscriptions can be configured EITHER via a YAML/JSON file (recommended,
    # readable, supports comments) OR inline JSON (handy for containers). If both
    # are set, the inline JSON takes precedence as an override.
    focus_subscriptions_config_file: str | None = None
    focus_subscriptions_config_json: str = "[]"

    default_page_size: int = 100
    max_page_size: int = 1000

    # In-process daily scheduler (APScheduler). Times are UTC.
    scheduler_enabled: bool = False
    scheduler_hour: int = 2
    scheduler_minute: int = 30

    @field_validator("focus_subscriptions_config_json")
    @classmethod
    def _validate_json(cls, v: str) -> str:
        json.loads(v)  # fail fast on malformed config
        return v

    @property
    def curated_account_url_effective(self) -> str:
        return self.curated_account_url or self.blob_account_url

    @property
    def curated_container_effective(self) -> str:
        return self.curated_container or self.blob_container

    @staticmethod
    def _load_config_file(path: str) -> list[dict]:
        from pathlib import Path

        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"subscriptions config file not found: {path}")
        text = p.read_text(encoding="utf-8")
        if p.suffix.lower() in (".yaml", ".yml"):
            import yaml

            data = yaml.safe_load(text) or []
        else:
            data = json.loads(text)
        # Accept either a top-level list or {"subscriptions": [...]}.
        if isinstance(data, dict):
            data = data.get("subscriptions", [])
        return data

    @property
    def subscriptions(self) -> list[SubscriptionConfig]:
        inline = self.focus_subscriptions_config_json.strip()
        if inline and inline != "[]":
            raw = json.loads(inline)  # inline JSON overrides the file
        elif self.focus_subscriptions_config_file:
            raw = self._load_config_file(self.focus_subscriptions_config_file)
        else:
            raw = json.loads(inline or "[]")
        if isinstance(raw, dict):
            raw = raw.get("subscriptions", [])
        return [SubscriptionConfig.model_validate(item) for item in raw]

    def subscriptions_for(
        self, cloud: CloudName, subscription_id: str | None = None
    ) -> list[SubscriptionConfig]:
        """Map a cloud (and optional subscription id) to subscription configs."""
        subs = [s for s in self.subscriptions if s.cloud == cloud]
        if subscription_id:
            subs = [s for s in subs if s.subscription_id == subscription_id]
        return subs


@lru_cache
def get_settings() -> Settings:
    return Settings()
