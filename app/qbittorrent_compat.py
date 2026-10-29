from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any


DEFAULT_CATEGORY = "soulseekarr"
WEBAPI_VERSION = "2.11.0"
CLIENT_VERSION = "5.0.0"


def job_hash(job_id: int) -> str:
    """Return a stable 40-character torrent-like hash for a Soulseekarr job."""
    return hashlib.sha1(f"soulseekarr-job:{int(job_id)}".encode("utf-8")).hexdigest()


def _basename(value: Any) -> str:
    """Return a filename from either POSIX or Soulseek/Windows-style paths."""
    text = str(value or "").strip().replace("\\", "/")
    return text.rsplit("/", 1)[-1].strip() if text else ""


def queue_title(row: dict[str, Any]) -> str:
    """Build the qBittorrent item name Sonarr parses for release metadata.

    Sonarr calculates queue-side quality/custom-format information from the
    download client's item title. Soulseekarr therefore exposes the exact
    normalized filename it plans to hand to Sonarr *before* the network transfer
    starts. Scoring the peer's raw Soulseek name while downloading and a different
    normalized name at import time can change Custom Format matches and waste the
    download. Older jobs without a persisted plan fall back to their available
    prepared/remote names.
    """
    for field in ("prepare_target_name", "sonarr_file", "import_file", "remote_filename"):
        name = _basename(row.get(field))
        if name:
            return name

    series = str(row.get("series_title") or "").strip()
    episode_code = str(row.get("episode_code") or "").strip()
    episode_title = str(row.get("episode_title") or "").strip()
    parts = [part for part in (series, episode_code, episode_title) if part]
    return " - ".join(parts) or f"Soulseekarr job {row.get('id', '')}".strip()


def torrent_row(
    row: dict[str, Any],
    *,
    category: str = DEFAULT_CATEGORY,
    save_path: str = "/downloads",
) -> dict[str, Any]:
    """Translate an active Soulseekarr row to qBittorrent Web API v2 JSON.

    Soulseekarr intentionally never reports these compatibility items as a
    completed/seeding torrent. Sonarr therefore displays them in Activity but
    does not invoke Completed Download Handling; Soulseekarr remains the sole
    owner of candidate evaluation, import and cleanup.
    """
    size = max(0, int(row.get("size") or 0))
    try:
        progress = float(row.get("progressPercent") or 0.0) / 100.0
    except (TypeError, ValueError):
        progress = 0.0
    progress = max(0.0, min(1.0, progress))

    status = str(row.get("status") or "").lower()
    is_queued = bool(row.get("isQueued")) or status == "queued"
    if is_queued:
        state = "queuedDL"
        eta = 8640000
    else:
        # Keep all post-download processing in a downloading-like state. This
        # prevents Sonarr from attempting to import a fake qBittorrent item.
        state = "moving" if status == "copying" else "downloading"
        raw_eta = row.get("remainingSeconds")
        try:
            eta = max(0, int(raw_eta)) if raw_eta is not None else (0 if progress >= 1.0 else 8640000)
        except (TypeError, ValueError):
            eta = 0 if progress >= 1.0 else 8640000

    if status in {"copying", "ready", "evaluating", "importing"}:
        progress = 1.0
        eta = 0

    speed = row.get("averageSpeed")
    try:
        dl_speed = max(0, int(float(speed or 0)))
    except (TypeError, ValueError):
        dl_speed = 0

    normalized_save_path = str(save_path or "/downloads").rstrip("/") or "/"
    return {
        "hash": job_hash(int(row.get("id") or 0)),
        "name": queue_title(row),
        "size": size,
        "progress": progress,
        "eta": eta,
        "state": state,
        "category": category or DEFAULT_CATEGORY,
        "save_path": normalized_save_path,
        # Keep content_path equal to save_path. These compatibility items never
        # enter qBittorrent's completed states, so Sonarr does not use it for CDH.
        "content_path": normalized_save_path,
        "ratio": 0.0,
        "ratio_limit": -1,
        "seeding_time_limit": -1,
        "inactive_seeding_time_limit": -1,
        "last_activity": 0,
        # Not consumed by Sonarr's qBittorrent model today, but useful to other
        # clients and when inspecting the endpoint manually.
        "dlspeed": dl_speed,
        "num_seeds": 0,
        "num_leechs": 0,
    }
