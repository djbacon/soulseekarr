from __future__ import annotations

import json
import os
import stat
import tempfile
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from threading import RLock
from urllib.parse import urlsplit, urlunsplit

from cryptography.fernet import Fernet, InvalidToken


def normalize_service_url(value: str) -> str:
    """Validate and normalize a Sonarr/slskd base URL.

    These URLs are intentionally allowed to point at private/LAN services, but
    only ordinary HTTP(S) origins are accepted. User-info, query strings,
    fragments, control characters and malformed ports are rejected so a saved
    setting cannot become an alternate URL/credential injection primitive.
    """
    raw = str(value or "").strip()
    if not raw:
        return ""
    if len(raw) > 2048 or any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in raw):
        raise ValueError("Service URL is invalid.")
    try:
        parsed = urlsplit(raw)
        # Accessing .port also validates malformed/out-of-range ports.
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("Service URL is invalid.") from exc
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Service URL must use http:// or https:// and include a host.")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Service URL must not contain embedded credentials.")
    if parsed.query or parsed.fragment:
        raise ValueError("Service URL must not contain a query string or fragment.")
    # urlsplit keeps the original host spelling/IPv6 brackets in netloc. Keep it
    # intact while normalizing only the scheme and trailing slash/path.
    path = (parsed.path or "").rstrip("/")
    return urlunsplit((parsed.scheme.lower(), parsed.netloc, path, "", ""))


def normalize_api_key(value: str, *, field_name: str = "API key") -> str:
    """Validate an HTTP header credential before it reaches httpx."""
    key = str(value or "").strip()
    if len(key) > 512 or any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in key):
        raise ValueError(f"{field_name} is invalid.")
    return key


def normalize_container_root(value: str, *, field_name: str = "Path") -> str:
    """Validate a configured container/filesystem root.

    Local download/staging roots participate in file discovery and cleanup, so
    they must be explicit absolute paths and may not be the filesystem root.
    Lexical dot-segments are collapsed before the value is persisted.
    """
    import posixpath

    raw = str(value or "").strip().replace("\\", "/")
    if not raw or len(raw) > 4096 or any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in raw):
        raise ValueError(f"{field_name} is invalid.")
    if not raw.startswith("/"):
        raise ValueError(f"{field_name} must be an absolute container path.")
    normalized = "/" + posixpath.normpath(raw).lstrip("/")
    normalized = normalized.rstrip("/") or "/"
    if normalized == "/":
        raise ValueError(f"{field_name} may not be the filesystem root (/).")
    return normalized


def _bounded_int(value, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def _safe_bool(value, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    return bool(default)


def _best_effort_private(path: Path, mode: int) -> None:
    try:
        path.chmod(mode)
    except OSError:
        pass


def _read_regular_file(path: Path, *, max_bytes: int) -> bytes:
    """Read a small private file without following symlinks."""
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise RuntimeError(f"Refusing unsafe or unreadable config file: {path.name}") from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError(f"Refusing non-regular config file: {path.name}")
        if info.st_size > max_bytes:
            raise RuntimeError(f"Config file is unexpectedly large: {path.name}")
        try:
            os.fchmod(fd, 0o600)
        except OSError:
            pass
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) > max_bytes:
            raise RuntimeError(f"Config file is unexpectedly large: {path.name}")
        return data
    finally:
        os.close(fd)


@dataclass
class Settings:
    sonarr_url: str = ""
    sonarr_api_key: str = ""
    radarr_url: str = ""
    radarr_api_key: str = ""
    radarr_import_path: str = "/downloads"
    slskd_url: str = ""
    slskd_api_key: str = ""
    sonarr_import_path: str = "/downloads"
    slskd_downloads_path: str = "/slskd-downloads"
    folder_mappings: list[dict[str, str]] = field(default_factory=list)
    scan_interval_minutes: int = 2
    search_timeout_seconds: int = 15
    auto_search_gap_seconds: int = 15
    repeat_search_cooldown_hours: int = 24
    missing_episode_delay_hours: int = 48
    history_retention_hours: int = 48
    max_search_results: int = 200
    min_file_size_mb: int = 30
    max_file_size_mb: int = 25000
    download_timeout_minutes: int = 360
    sonarr_import_timeout_minutes: int = 120
    prefer_hardlinks: bool = True
    max_auto_per_cycle: int = 3
    max_active_downloads_for_search: int = 100
    dynamic_active_download_limit_enabled: bool = False
    max_actual_downloading_for_search: int = 5
    peer_failure_strike_limit: int = 3
    peer_failure_cooldown_days: int = 7
    search_order: str = "sequential"
    auto_search_enabled: bool = True
    auto_download: bool = False
    new_series_download_enabled: bool = False
    monitored_only: bool = True


class SettingsStore:
    SECRET_FIELDS = {"sonarr_api_key", "radarr_api_key", "slskd_api_key"}

    def __init__(self) -> None:
        config_dir = Path(os.getenv("SOULSEEKARR_CONFIG_DIR", "/config"))
        if config_dir.is_symlink():
            raise RuntimeError("SOULSEEKARR_CONFIG_DIR may not be a symlink.")
        config_dir.mkdir(parents=True, exist_ok=True)
        if config_dir.is_symlink():
            raise RuntimeError("SOULSEEKARR_CONFIG_DIR may not be a symlink.")
        _best_effort_private(config_dir, 0o700)
        self.path = config_dir / "settings.json"
        self.key_path = config_dir / "secret.key"
        self._lock = RLock()
        self._fernet = Fernet(self._load_or_create_key())

    def _load_or_create_key(self) -> bytes:
        if self.key_path.exists():
            return _read_regular_file(self.key_path, max_bytes=4096).strip()

        key = Fernet.generate_key()
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(self.key_path, flags, 0o600)
        except FileExistsError:
            return _read_regular_file(self.key_path, max_bytes=4096).strip()
        with os.fdopen(fd, "wb") as handle:
            handle.write(key + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        return key

    def load(self) -> Settings:
        with self._lock:
            if not self.path.exists():
                return Settings()
            try:
                payload = _read_regular_file(self.path, max_bytes=2 * 1024 * 1024)
                raw = json.loads(payload.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise RuntimeError("settings.json is not valid UTF-8 JSON.") from exc
            if not isinstance(raw, dict):
                raise RuntimeError("settings.json must contain a JSON object.")
            allowed = {f.name for f in fields(Settings)}
            data = {k: v for k, v in raw.items() if k in allowed}
            for key in self.SECRET_FIELDS:
                if data.get(key):
                    try:
                        encrypted = data[key]
                        if not isinstance(encrypted, str):
                            raise ValueError("Encrypted secret must be text.")
                        token = encrypted.encode("ascii")
                        plaintext = self._fernet.decrypt(token).decode("utf-8")
                        data[key] = normalize_api_key(plaintext, field_name=key)
                    except (InvalidToken, UnicodeError, ValueError):
                        data[key] = ""
            settings = Settings(**data)
            # Existing configuration is treated as untrusted input too. Invalid
            # service URLs fail closed instead of reaching the HTTP client.
            try:
                settings.sonarr_url = normalize_service_url(settings.sonarr_url)
            except ValueError:
                settings.sonarr_url = ""
            try:
                settings.slskd_url = normalize_service_url(settings.slskd_url)
            except ValueError:
                settings.slskd_url = ""
            try:
                settings.radarr_url = normalize_service_url(settings.radarr_url)
            except ValueError:
                settings.radarr_url = ""
            try:
                settings.radarr_import_path = normalize_container_root(settings.radarr_import_path, field_name="Radarr import path")
            except ValueError:
                settings.radarr_import_path = "/downloads"
            # Clamp every persisted numeric control before it reaches loops, API
            # pagination, sleeps or filesystem size filters. settings.json lives
            # on a bind mount and must not be able to create extreme resource use.
            settings.scan_interval_minutes = _bounded_int(settings.scan_interval_minutes, 2, 1, 10080)
            settings.search_timeout_seconds = _bounded_int(settings.search_timeout_seconds, 15, 5, 60)
            settings.auto_search_gap_seconds = _bounded_int(settings.auto_search_gap_seconds, 15, 1, 300)
            settings.repeat_search_cooldown_hours = _bounded_int(settings.repeat_search_cooldown_hours, 24, 0, 720)
            settings.missing_episode_delay_hours = _bounded_int(settings.missing_episode_delay_hours, 48, 0, 720)
            settings.history_retention_hours = _bounded_int(settings.history_retention_hours, 48, 1, 8760)
            settings.max_search_results = _bounded_int(settings.max_search_results, 200, 10, 1000)
            settings.min_file_size_mb = _bounded_int(settings.min_file_size_mb, 30, 1, 1_000_000)
            settings.max_file_size_mb = max(
                settings.min_file_size_mb,
                _bounded_int(settings.max_file_size_mb, 25000, 1, 1_000_000),
            )
            settings.download_timeout_minutes = _bounded_int(settings.download_timeout_minutes, 360, 10, 10080)
            settings.sonarr_import_timeout_minutes = _bounded_int(settings.sonarr_import_timeout_minutes, 120, 60, 1440)
            settings.max_auto_per_cycle = _bounded_int(settings.max_auto_per_cycle, 3, 1, 20)
            settings.max_active_downloads_for_search = _bounded_int(settings.max_active_downloads_for_search, 100, 1, 10000)
            settings.max_actual_downloading_for_search = _bounded_int(settings.max_actual_downloading_for_search, 5, 0, 1000)
            settings.peer_failure_strike_limit = _bounded_int(settings.peer_failure_strike_limit, 3, 1, 100)
            settings.peer_failure_cooldown_days = _bounded_int(settings.peer_failure_cooldown_days, 7, 1, 365)
            settings.search_order = "random" if str(settings.search_order).strip().lower() == "random" else "sequential"
            settings.prefer_hardlinks = _safe_bool(settings.prefer_hardlinks, True)
            settings.dynamic_active_download_limit_enabled = _safe_bool(
                settings.dynamic_active_download_limit_enabled, False
            )
            settings.auto_search_enabled = _safe_bool(settings.auto_search_enabled, True)
            settings.auto_download = _safe_bool(settings.auto_download, False)
            settings.new_series_download_enabled = _safe_bool(settings.new_series_download_enabled, False)
            settings.monitored_only = _safe_bool(settings.monitored_only, True)

            # Treat path settings loaded from the bind-mounted JSON as untrusted.
            # Invalid roots fall back to safe defaults instead of broadening file
            # discovery/cleanup to relative paths or filesystem root.
            try:
                settings.sonarr_import_path = normalize_container_root(
                    settings.sonarr_import_path, field_name="Sonarr import path"
                )
            except ValueError:
                settings.sonarr_import_path = "/downloads"
            try:
                settings.slskd_downloads_path = normalize_container_root(
                    settings.slskd_downloads_path, field_name="slskd downloads path"
                )
            except ValueError:
                settings.slskd_downloads_path = "/slskd-downloads"

            # Persisted mappings are also bounded so a tampered settings file
            # cannot create an unbounded mapping scan. clean_mappings performs
            # containment-oriented path normalization and duplicate removal.
            from .pathmap import clean_mappings
            settings.folder_mappings = clean_mappings(
                settings.folder_mappings if isinstance(settings.folder_mappings, list) else []
            )[:64]
            return settings

    def save(self, settings: Settings) -> None:
        with self._lock:
            data = asdict(settings)
            data["sonarr_url"] = normalize_service_url(data.get("sonarr_url", ""))
            data["radarr_url"] = normalize_service_url(data.get("radarr_url", ""))
            data["radarr_import_path"] = normalize_container_root(data.get("radarr_import_path", "/downloads"), field_name="Radarr import path")
            data["slskd_url"] = normalize_service_url(data.get("slskd_url", ""))
            data["sonarr_import_path"] = normalize_container_root(
                data.get("sonarr_import_path", "/downloads"), field_name="Sonarr import path"
            )
            data["slskd_downloads_path"] = normalize_container_root(
                data.get("slskd_downloads_path", "/slskd-downloads"), field_name="slskd downloads path"
            )
            from .pathmap import clean_mappings
            data["folder_mappings"] = clean_mappings(data.get("folder_mappings") or [])[:64]
            for key in self.SECRET_FIELDS:
                value = normalize_api_key(data.get(key, ""), field_name=key)
                if value:
                    data[key] = self._fernet.encrypt(value.encode("utf-8")).decode("ascii")
            fd, tmp_name = tempfile.mkstemp(prefix=".settings-", suffix=".tmp", dir=self.path.parent)
            tmp = Path(tmp_name)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(data, handle, indent=2)
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                _best_effort_private(tmp, 0o600)
                os.replace(tmp, self.path)
                _best_effort_private(self.path, 0o600)
            finally:
                try:
                    tmp.unlink()
                except FileNotFoundError:
                    pass
