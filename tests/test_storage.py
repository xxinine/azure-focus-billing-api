from __future__ import annotations

import os
from dataclasses import dataclass
from types import SimpleNamespace

from app.config import Settings
from app.storage import _prune_blob_cache, cache_azure_parquet_globs


@dataclass
class FakeBlob:
    name: str
    etag: str
    size: int


class FakeDownload:
    def __init__(self, content: bytes):
        self.content = content

    def readinto(self, stream) -> None:
        stream.write(self.content)


class FakeBlobClient:
    def __init__(self, service, blob_name: str):
        self.service = service
        self.blob_name = blob_name

    def download_blob(self, **kwargs):
        self.service.downloads += 1
        assert kwargs["etag"] == self.service.blob.etag
        return FakeDownload(self.service.content)


class FakeContainerClient:
    def __init__(self, service):
        self.service = service

    def list_blobs(self, *, name_starts_with: str):
        if self.service.blob.name.startswith(name_starts_with):
            return [self.service.blob]
        return []


class FakeBlobService:
    def __init__(self, blob: FakeBlob, content: bytes):
        self.blob = blob
        self.content = content
        self.downloads = 0

    def get_container_client(self, container: str):
        assert container == "report"
        return FakeContainerClient(self)

    def get_blob_client(self, container: str, blob_name: str):
        assert container == "report"
        return FakeBlobClient(self, blob_name)


def test_blob_cache_reuses_etag_and_refreshes_changed_blob(tmp_path, monkeypatch):
    blob_name = "curated/focus/dataset=daily/period=2026-08/data.parquet"
    service = FakeBlobService(FakeBlob(blob_name, '"v1"', 8), b"version1")
    monkeypatch.setattr("app.storage.blob_service", lambda settings, url: service)
    settings = Settings(
        _env_file=None,
        blob_cache_dir=str(tmp_path),
        focus_subscriptions_config_json="[]",
    )
    globs = ["az://report/curated/focus/dataset=daily/period=2026-08/*.parquet"]

    first = cache_azure_parquet_globs(settings, "https://example.test", globs)
    second = cache_azure_parquet_globs(settings, "https://example.test", globs)

    assert first == second
    assert service.downloads == 1
    assert "dataset=daily/period=2026-08" in first[0]
    assert open(first[0], "rb").read() == b"version1"

    service.blob = FakeBlob(blob_name, '"v2"', 8)
    service.content = b"version2"
    refreshed = cache_azure_parquet_globs(settings, "https://example.test", globs)

    assert refreshed == first
    assert service.downloads == 2
    assert open(refreshed[0], "rb").read() == b"version2"


def test_blob_cache_returns_no_files_when_glob_has_no_match(tmp_path, monkeypatch):
    blob = FakeBlob("curated/other/data.parquet", '"v1"', 4)
    service = FakeBlobService(blob, b"data")
    monkeypatch.setattr("app.storage.blob_service", lambda settings, url: service)
    settings = Settings(
        _env_file=None,
        blob_cache_dir=str(tmp_path),
        focus_subscriptions_config_json="[]",
    )

    files = cache_azure_parquet_globs(
        settings,
        "https://example.test",
        ["az://report/curated/focus/*.parquet"],
    )

    assert files == []
    assert service.downloads == 0


def test_blob_cache_evicts_least_recently_used_file(tmp_path):
    settings = Settings(
        _env_file=None,
        blob_cache_dir=str(tmp_path),
        blob_cache_max_size_mb=1,
        blob_cache_min_free_space_mb=0,
        focus_subscriptions_config_json="[]",
    )
    old_data = tmp_path / "old.parquet"
    old_metadata = tmp_path / ".old.parquet.etag.json"
    recent_data = tmp_path / "recent.parquet"
    recent_metadata = tmp_path / ".recent.parquet.etag.json"
    old_data.write_bytes(b"x" * 700_000)
    old_metadata.write_text("{}", encoding="utf-8")
    recent_data.write_bytes(b"y" * 700_000)
    recent_metadata.write_text("{}", encoding="utf-8")
    old_data.touch()
    recent_data.touch()
    old_mtime = old_data.stat().st_mtime - 60
    os.utime(old_data, (old_mtime, old_mtime))

    can_fit = _prune_blob_cache(
        settings,
        tmp_path,
        protected_paths={recent_data},
    )

    assert can_fit is True
    assert not old_data.exists()
    assert not old_metadata.exists()
    assert recent_data.exists()


def test_blob_cache_bypasses_download_when_blob_exceeds_limit(tmp_path, monkeypatch):
    blob_name = "curated/focus/dataset=daily/large.parquet"
    blob_size = 1024 * 1024 + 1
    service = FakeBlobService(FakeBlob(blob_name, '"v1"', blob_size), b"")
    monkeypatch.setattr("app.storage.blob_service", lambda settings, url: service)
    settings = Settings(
        _env_file=None,
        blob_cache_dir=str(tmp_path),
        blob_cache_max_size_mb=1,
        blob_cache_min_free_space_mb=0,
        focus_subscriptions_config_json="[]",
    )

    files = cache_azure_parquet_globs(
        settings,
        "https://example.test",
        ["az://report/curated/focus/dataset=daily/*.parquet"],
    )

    assert files == ["az://report/curated/focus/dataset=daily/large.parquet"]
    assert service.downloads == 0


def test_blob_cache_prune_removes_orphaned_data(tmp_path):
    settings = Settings(
        _env_file=None,
        blob_cache_min_free_space_mb=0,
        focus_subscriptions_config_json="[]",
    )
    orphan = tmp_path / "orphan.parquet"
    orphan.write_bytes(b"incomplete")

    can_fit = _prune_blob_cache(settings, tmp_path, protected_paths=set())

    assert can_fit is True
    assert not orphan.exists()


def test_blob_cache_rejects_reservation_below_free_space_floor(tmp_path, monkeypatch):
    settings = Settings(
        _env_file=None,
        blob_cache_min_free_space_mb=2,
        focus_subscriptions_config_json="[]",
    )
    monkeypatch.setattr(
        "app.storage.shutil.disk_usage",
        lambda path: SimpleNamespace(free=2 * 1024 * 1024),
    )

    can_fit = _prune_blob_cache(
        settings,
        tmp_path,
        protected_paths=set(),
        required_bytes=1,
    )

    assert can_fit is False


def test_blob_cache_expires_unused_file_below_capacity(tmp_path):
    settings = Settings(
        _env_file=None,
        blob_cache_max_unused_days=1,
        blob_cache_min_free_space_mb=0,
        focus_subscriptions_config_json="[]",
    )
    data = tmp_path / "expired.parquet"
    metadata = tmp_path / ".expired.parquet.etag.json"
    data.write_bytes(b"old")
    metadata.write_text("{}", encoding="utf-8")
    old_mtime = data.stat().st_mtime - 2 * 24 * 60 * 60
    os.utime(data, (old_mtime, old_mtime))

    can_fit = _prune_blob_cache(settings, tmp_path, protected_paths=set())

    assert can_fit is True
    assert not data.exists()
    assert not metadata.exists()