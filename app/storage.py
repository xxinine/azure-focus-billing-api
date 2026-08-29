"""Azure Blob write helpers.

DuckDB's azure extension can READ az:// but cannot WRITE to containers, so
curated parquet is written locally by DuckDB and uploaded with the Azure SDK
using the same credential model as the rest of the app.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import logging
import os
import shutil
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

from azure.core import MatchConditions
from azure.core.exceptions import ResourceModifiedError
from azure.storage.blob import BlobServiceClient

from .config import Settings

logger = logging.getLogger("uvicorn.error")
_cache_locks = [threading.Lock() for _ in range(64)]
_blob_services_guard = threading.Lock()
_blob_services: dict[tuple[object, ...], BlobServiceClient] = {}
_cache_maintenance_guard = threading.Lock()
_cache_last_maintenance: dict[str, float] = {}

_MB = 1024 * 1024
_CACHE_MAINTENANCE_INTERVAL_SECONDS = 300
_STALE_DOWNLOAD_SECONDS = 24 * 60 * 60


def _cache_lock_index(key: str) -> int:
    return int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % len(_cache_locks)


def _authority(account_url: str) -> str | None:
    """AAD authority for the sovereign cloud the account lives in.
    None means the azure-identity default (Azure public cloud)."""
    if ".chinacloudapi.cn" in account_url:
        return "https://login.chinacloudapi.cn"
    return None


def _credential(settings: Settings):
    from azure.identity import (
        CertificateCredential,
        ClientSecretCredential,
        DefaultAzureCredential,
    )

    authority = _authority(settings.blob_account_url)
    if settings.azure_storage_auth_mode == "service_principal":
        if settings.azure_client_secret:
            return ClientSecretCredential(
                settings.azure_tenant_id,
                settings.azure_client_id,
                settings.azure_client_secret,
                authority=authority,
            )
        return CertificateCredential(
            settings.azure_tenant_id,
            settings.azure_client_id,
            certificate_path=settings.azure_client_certificate_path,
            authority=authority,
        )
    # managed_identity / az cli dev
    return DefaultAzureCredential(authority=authority) if authority else DefaultAzureCredential()


def blob_service(settings: Settings, account_url: str) -> BlobServiceClient:
    mode = settings.azure_storage_auth_mode
    key = (
        account_url,
        mode,
        settings.azure_tenant_id,
        settings.azure_client_id,
        settings.azure_client_secret,
        settings.azure_client_certificate_path,
        settings.azure_storage_sas_token,
        settings.azure_storage_connection_string,
    )
    with _blob_services_guard:
        existing = _blob_services.get(key)
        if existing:
            return existing
        if mode == "connection_string":
            service = BlobServiceClient.from_connection_string(
                settings.azure_storage_connection_string
            )
        elif mode == "sas":
            service = BlobServiceClient(
                account_url, credential=settings.azure_storage_sas_token
            )
        else:
            service = BlobServiceClient(account_url, credential=_credential(settings))
        _blob_services[key] = service
        return service


def cache_azure_parquet_globs(
    settings: Settings,
    account_url: str,
    globs: list[str],
) -> list[str]:
    """Resolve az:// parquet globs to ETag-validated local cache files."""
    if not settings.blob_cache_enabled:
        return globs

    cache_root = Path(settings.blob_cache_dir).expanduser().resolve()
    cache_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    cache_root.chmod(0o700)
    service = blob_service(settings, account_url)
    _maybe_prune_blob_cache(settings, cache_root)

    for attempt in range(2):
        try:
            files: list[str] = []
            seen: set[tuple[str, str]] = set()
            protected_paths: set[Path] = set()
            for uri_glob in globs:
                parsed = urlparse(uri_glob)
                if parsed.scheme != "az" or not parsed.netloc:
                    raise ValueError(f"expected az:// parquet glob: {uri_glob}")
                container = parsed.netloc
                pattern = unquote(parsed.path.lstrip("/"))
                prefix = _glob_static_prefix(pattern)
                container_client = service.get_container_client(container)
                for blob in container_client.list_blobs(name_starts_with=prefix):
                    if not fnmatch.fnmatchcase(blob.name, pattern):
                        continue
                    identity = (container, blob.name)
                    if identity in seen:
                        continue
                    seen.add(identity)
                    resolved = _cache_blob(
                        settings,
                        service,
                        cache_root,
                        account_url,
                        container,
                        blob.name,
                        str(blob.etag),
                        blob.size,
                        protected_paths,
                    )
                    files.append(resolved)
                    if not resolved.startswith("az://"):
                        protected_paths.add(Path(resolved))
            return files
        except ResourceModifiedError:
            if attempt:
                raise
            logger.info("blob_cache_retry reason=etag_changed")
    return []


def _glob_static_prefix(pattern: str) -> str:
    wildcard_positions = [
        position for token in ("*", "?", "[") if (position := pattern.find(token)) >= 0
    ]
    if not wildcard_positions:
        return pattern
    static = pattern[: min(wildcard_positions)]
    return static[: static.rfind("/") + 1]


def _cache_blob(
    settings: Settings,
    service: BlobServiceClient,
    cache_root: Path,
    account_url: str,
    container: str,
    blob_name: str,
    etag: str,
    size: int | None,
    protected_paths: set[Path],
) -> str:
    identity = f"{account_url}\n{container}\n{blob_name}"
    key = hashlib.sha256(identity.encode()).hexdigest()
    blob_parts = blob_name.split("/")
    if container in (".", "..") or any(part in ("", ".", "..") for part in blob_parts):
        raise ValueError(f"unsafe blob cache path: {container}/{blob_name}")
    account_key = hashlib.sha256(account_url.encode()).hexdigest()[:16]
    data_path = cache_root.joinpath(account_key, container, *blob_parts)
    data_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path = data_path.with_name(f".{data_path.name}.etag.json")
    lock_path = cache_root / ".locks" / f"{_cache_lock_index(key):02x}.lock"
    reservation_path = cache_root / ".reservations" / f"{key}.json"
    remote_uri = f"az://{container}/{quote(blob_name, safe='/=:@-._~')}"

    with _cache_lock(key, lock_path):
        if _cache_hit(data_path, metadata_path, etag, size):
            os.utime(data_path, None)
            logger.debug("blob_cache_hit blob=%s", blob_name)
            return str(data_path)

        if size is None or not _reserve_cache_space(
            settings,
            cache_root,
            protected_paths,
            reservation_path,
            data_path,
            size,
        ):
            logger.warning(
                "blob_cache_bypass blob=%s bytes=%s reason=capacity",
                blob_name,
                size,
            )
            return remote_uri

        blob_client = service.get_blob_client(container, blob_name)
        temp_path: str | None = None
        metadata_temp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=data_path.parent,
                prefix=f".{data_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as stream:
                temp_path = stream.name
                blob_client.download_blob(
                    etag=etag,
                    match_condition=MatchConditions.IfNotModified,
                    max_concurrency=settings.blob_cache_download_concurrency,
                ).readinto(stream)
                stream.flush()
                os.fsync(stream.fileno())
            downloaded_size = os.path.getsize(temp_path)
            if size is not None and downloaded_size != size:
                raise IOError(
                    f"cached blob size mismatch for {blob_name}: "
                    f"expected {size}, got {downloaded_size}"
                )
            os.replace(temp_path, data_path)
            temp_path = None

            metadata = {"etag": etag, "size": downloaded_size}
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=data_path.parent,
                prefix=f".{data_path.name}.",
                suffix=".json.tmp",
                delete=False,
            ) as metadata_stream:
                metadata_temp_path = metadata_stream.name
                json.dump(metadata, metadata_stream)
                metadata_stream.flush()
                os.fsync(metadata_stream.fileno())
            os.replace(metadata_temp_path, metadata_path)
            metadata_temp_path = None
            logger.info(
                "blob_cache_miss blob=%s bytes=%s etag=%s",
                blob_name,
                downloaded_size,
                etag,
            )
            return str(data_path)
        finally:
            reservation_path.unlink(missing_ok=True)
            for leftover in (temp_path, metadata_temp_path):
                if leftover:
                    Path(leftover).unlink(missing_ok=True)


def _maybe_prune_blob_cache(settings: Settings, cache_root: Path) -> None:
    cache_key = str(cache_root)
    now = time.monotonic()
    with _cache_maintenance_guard:
        last_run = _cache_last_maintenance.get(cache_key, 0)
        if now - last_run < _CACHE_MAINTENANCE_INTERVAL_SECONDS:
            return
        _cache_last_maintenance[cache_key] = now
    _prune_blob_cache(settings, cache_root, protected_paths=set())


def _prune_blob_cache(
    settings: Settings,
    cache_root: Path,
    protected_paths: set[Path],
    required_bytes: int = 0,
) -> bool:
    """Evict expired/LRU entries and report whether a new file can fit safely."""
    maintenance_lock = cache_root / ".maintenance.lock"
    with _cache_lock(f"maintenance:{cache_root}", maintenance_lock):
        return _prune_blob_cache_locked(
            settings,
            cache_root,
            protected_paths,
            required_bytes,
        )


def _reserve_cache_space(
    settings: Settings,
    cache_root: Path,
    protected_paths: set[Path],
    reservation_path: Path,
    data_path: Path,
    required_bytes: int,
) -> bool:
    maintenance_lock = cache_root / ".maintenance.lock"
    with _cache_lock(f"maintenance:{cache_root}", maintenance_lock):
        if not _prune_blob_cache_locked(
            settings,
            cache_root,
            protected_paths,
            required_bytes,
        ):
            return False
        reservation_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        reservation_path.write_text(
            json.dumps(
                {
                    "created": time.time(),
                    "size": required_bytes,
                    "data_path": str(data_path.relative_to(cache_root)),
                }
            ),
            encoding="utf-8",
        )
        return True


def _prune_blob_cache_locked(
    settings: Settings,
    cache_root: Path,
    protected_paths: set[Path],
    required_bytes: int = 0,
) -> bool:
    max_bytes = settings.blob_cache_max_size_mb * _MB
    min_free_bytes = settings.blob_cache_min_free_space_mb * _MB
    max_idle_seconds = settings.blob_cache_max_unused_days * 24 * 60 * 60
    now = time.time()
    reserved_bytes, reserved_paths = _active_cache_reservations(cache_root, now)
    all_protected_paths = protected_paths | reserved_paths
    entries: list[tuple[float, int, Path, Path]] = []
    total_bytes = 0
    data_paths: set[Path] = set()
    for data_path in cache_root.rglob("*.parquet"):
        if not data_path.is_file():
            continue
        metadata_path = data_path.with_name(f".{data_path.name}.etag.json")
        if not metadata_path.is_file() and data_path not in all_protected_paths:
            data_path.unlink(missing_ok=True)
            continue
        try:
            stat = data_path.stat()
        except FileNotFoundError:
            continue
        data_paths.add(data_path)
        total_bytes += stat.st_size
        entries.append((stat.st_mtime, stat.st_size, data_path, metadata_path))

    for metadata_path in cache_root.rglob(".*.etag.json"):
        data_name = metadata_path.name[1 : -len(".etag.json")]
        if metadata_path.with_name(data_name) not in data_paths:
            metadata_path.unlink(missing_ok=True)
    for partial_path in cache_root.rglob("*.tmp"):
        try:
            if now - partial_path.stat().st_mtime > _STALE_DOWNLOAD_SECONDS:
                partial_path.unlink(missing_ok=True)
        except FileNotFoundError:
            pass

    evicted_files = 0
    evicted_bytes = 0
    retained: list[tuple[float, int, Path, Path]] = []
    for entry in entries:
        last_accessed, entry_size, data_path, metadata_path = entry
        if data_path not in all_protected_paths and now - last_accessed > max_idle_seconds:
            if _remove_cache_entry(data_path, metadata_path):
                total_bytes -= entry_size
                evicted_files += 1
                evicted_bytes += entry_size
        else:
            retained.append(entry)

    free_bytes = shutil.disk_usage(cache_root).free
    bytes_to_free = max(
        0,
        total_bytes + reserved_bytes + required_bytes - max_bytes,
        min_free_bytes + reserved_bytes + required_bytes - free_bytes,
    )
    if bytes_to_free:
        for _, entry_size, data_path, metadata_path in sorted(retained):
            if data_path in all_protected_paths:
                continue
            if _remove_cache_entry(data_path, metadata_path):
                total_bytes -= entry_size
                free_bytes += entry_size
                bytes_to_free -= entry_size
                evicted_files += 1
                evicted_bytes += entry_size
                if bytes_to_free <= 0:
                    break

    if evicted_files:
        logger.info(
            "blob_cache_prune files=%s bytes=%s",
            evicted_files,
            evicted_bytes,
        )
    return (
        total_bytes + reserved_bytes + required_bytes <= max_bytes
        and free_bytes >= min_free_bytes + reserved_bytes + required_bytes
    )


def _active_cache_reservations(cache_root: Path, now: float) -> tuple[int, set[Path]]:
    reserved_bytes = 0
    reserved_paths: set[Path] = set()
    reservation_root = cache_root / ".reservations"
    for reservation_path in reservation_root.glob("*.json"):
        try:
            reservation = json.loads(reservation_path.read_text(encoding="utf-8"))
            if now - float(reservation["created"]) > _STALE_DOWNLOAD_SECONDS:
                reservation_path.unlink(missing_ok=True)
                continue
            reserved_bytes += int(reservation["size"])
            reserved_paths.add(cache_root / reservation["data_path"])
        except (KeyError, TypeError, ValueError, OSError, json.JSONDecodeError):
            reservation_path.unlink(missing_ok=True)
    return reserved_bytes, reserved_paths


def _remove_cache_entry(data_path: Path, metadata_path: Path) -> bool:
    try:
        data_path.unlink()
    except FileNotFoundError:
        metadata_path.unlink(missing_ok=True)
        return False
    metadata_path.unlink(missing_ok=True)
    return True


def _cache_hit(
    data_path: Path,
    metadata_path: Path,
    etag: str,
    size: int | None,
) -> bool:
    if not data_path.is_file() or not metadata_path.is_file():
        return False
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    actual_size = data_path.stat().st_size
    return (
        metadata.get("etag") == etag
        and metadata.get("size") == actual_size
        and (size is None or actual_size == size)
    )


@contextmanager
def _cache_lock(key: str, lock_path: Path):
    thread_lock = _cache_locks[_cache_lock_index(key)]
    with thread_lock:
        lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(descriptor, "r+") as lock_file:
            try:
                import fcntl
            except ImportError:
                fcntl = None
            if fcntl:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if fcntl:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def delete_prefix(settings: Settings, account_url: str, container: str, prefix: str) -> int:
    """Delete all blobs under a prefix (partition overwrite). Returns count."""
    svc = blob_service(settings, account_url)
    cc = svc.get_container_client(container)
    n = 0
    for b in cc.list_blobs(name_starts_with=prefix):
        cc.delete_blob(b.name)
        n += 1
    return n


def upload_file(
    settings: Settings, account_url: str, container: str, blob_name: str, local_path: str
) -> None:
    svc = blob_service(settings, account_url)
    bc = svc.get_blob_client(container, blob_name)
    with open(local_path, "rb") as f:
        bc.upload_blob(f, overwrite=True)
