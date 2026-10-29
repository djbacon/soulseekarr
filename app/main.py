from __future__ import annotations

import asyncio
import inspect
import json
import os
import re
import secrets
import shutil
import time
from datetime import datetime, timedelta, timezone

import httpx
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any, Annotated
from urllib.parse import parse_qs, urlsplit

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from .config import Settings, SettingsStore, normalize_api_key, normalize_container_root, normalize_service_url
from .import_cleanup import cleanup_import_directory
from .jobs import JobStore, peer_file_key
from .models import MissingEpisode, SearchCandidate, is_movie_id
from .media import media_client, media_configured
from .radarr import RadarrClient
from .pathmap import clean_mappings, remote_transfer_fallback_key, remote_transfer_key
from .qbittorrent_compat import CLIENT_VERSION as QBIT_CLIENT_VERSION, DEFAULT_CATEGORY as QBIT_DEFAULT_CATEGORY, WEBAPI_VERSION as QBIT_WEBAPI_VERSION, job_hash as qbit_job_hash, torrent_row as qbit_torrent_row
from .queue_policy import (
    QUEUE_STRIKE_INTERVAL_SECONDS,
    QUEUE_STRIKE_LIMIT,
    QUEUE_STRIKE_MAX_PROBES_PER_PASS,
    parse_utc,
    transfer_has_started,
    transfer_is_actively_downloading,
    transfer_is_queued,
    transfer_queue_position,
    transfer_queue_started_at,
)
from .release_name import build_import_filename, extract_quality
from .runner import JobRunner
from .search_state import SearchStateStore
from .service import adaptive_search_delay_hours, episode_search_available_at, episode_search_eligible, order_round_robin_episodes, order_season_folder_episodes, order_upgrade_round_robin_episodes, order_upgrade_search_episodes, rank_candidates, search_queries, upgrade_priority_tiers
from .slskd import SlskdClient, SlskdPeerDirectoryUnavailable
from .sonarr import SonarrClient


BASE_DIR = Path(__file__).resolve().parent
APP_VERSION = "0.8.4"
_BROWSER_CSRF_TOKEN = secrets.token_urlsafe(32)
IMPORT_DIR = Path(
    normalize_container_root(
        os.getenv("SOULSEEKARR_IMPORT_DIR", "/downloads"),
        field_name="SOULSEEKARR_IMPORT_DIR",
    )
)
IMPORT_DIR.mkdir(parents=True, exist_ok=True)
settings_store = SettingsStore()
jobs = JobStore()
runner = JobRunner(settings_store, jobs, IMPORT_DIR)
search_state = SearchStateStore()

PAGE_SIZE = 25

# TV-show UI readers never contact Sonarr directly. A background worker keeps a
# persistent SQLite snapshot fresh; pagination/filter endpoints read only that
# saved state. The event lets the Refresh button request an early background
# sync without making the browser wait for Sonarr.
_SERIES_REFRESH_EVENT = asyncio.Event()
_SERIES_SNAPSHOT_INTERVAL_SECONDS = 30.0

# Upgrade lists can span thousands of episodes and require one Sonarr episode
# request per enabled show. Keep a short-lived shared catalog so asynchronous
# pagination/prefetch does not rescan the entire library for every page.
_UPGRADE_CATALOG_TTL_SECONDS = 30.0
_UPGRADE_CATALOG_LOCK = asyncio.Lock()
_UPGRADE_CATALOG_CACHE: dict[str, Any] = {
    "signature": None,
    "expires_at": 0.0,
    "episodes": [],
    "errors": [],
}

# Dashboard's missing count is exact, but building it can require Sonarr plus
# supplemental specials lookups. Cache it briefly so the 5-second Dashboard
# refresh can keep downloads/search activity live without hammering Sonarr.
_DASHBOARD_MISSING_CACHE_TTL_SECONDS = 30.0
_DASHBOARD_MISSING_CACHE_LOCK = asyncio.Lock()
_DASHBOARD_MISSING_CACHE: dict[str, Any] = {
    "signature": None,
    "expires_at": 0.0,
    "count": 0,
    "error": None,
}

# Explicit per-episode searches launched from the UI run independently of the
# current page request. This lets a search finish and notify the user even when
# they navigate from Missing/Upgrade to another Soulseekarr section.
_TARGETED_SEARCH_TASKS: dict[int, asyncio.Task[Any]] = {}
_MANUAL_CYCLE_TASK: asyncio.Task[Any] | None = None

# qBittorrent compatibility categories are intentionally lightweight. Sonarr
# may create the configured category while testing the download client. The
# fixed default is always present again after a restart.
QBIT_RADARR_CATEGORY = "soulseekarr-radarr"
_QBIT_CATEGORIES: set[str] = {QBIT_DEFAULT_CATEGORY, QBIT_RADARR_CATEGORY}


def _format_disk_bytes(value: int) -> str:
    amount = float(max(0, int(value or 0)))
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    unit = units[0]
    for candidate in units:
        unit = candidate
        if amount < 1024.0 or candidate == units[-1]:
            break
        amount /= 1024.0
    if unit == "B":
        return f"{int(amount)} {unit}"
    if amount >= 100:
        return f"{amount:.0f} {unit}"
    if amount >= 10:
        return f"{amount:.1f} {unit}"
    return f"{amount:.2f} {unit}"


def _filesystem_space(path: Path) -> dict[str, Any]:
    resolved = Path(path)
    try:
        usage = shutil.disk_usage(resolved)
    except OSError as exc:
        return {
            "available": False,
            "path": str(resolved),
            "error": str(exc),
            "total": None,
            "free": None,
            "totalText": None,
            "freeText": None,
        }
    return {
        "available": True,
        "path": str(resolved),
        "error": None,
        "total": int(usage.total),
        "free": int(usage.free),
        "totalText": _format_disk_bytes(usage.total),
        "freeText": _format_disk_bytes(usage.free),
    }


def _settings_storage_space(settings: Settings) -> dict[str, dict[str, Any]]:
    # sonarr_import_path is the path Sonarr sees. Soulseekarr writes fallback
    # staging data through SOULSEEKARR_IMPORT_DIR, so that local backing mount
    # is the filesystem whose capacity is meaningful from this container.
    return {
        "sonarr_import": _filesystem_space(IMPORT_DIR),
        "slskd_downloads": _filesystem_space(Path(settings.slskd_downloads_path)),
    }


def _pagination_meta(total: int, page: int) -> dict[str, int]:
    total = max(0, int(total))
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(int(page or 1), total_pages))
    offset = (page - 1) * PAGE_SIZE
    start = offset + 1 if total else 0
    end = min(offset + PAGE_SIZE, total)
    return {
        "page": page,
        "pageSize": PAGE_SIZE,
        "total": total,
        "totalPages": total_pages,
        "offset": offset,
        "start": start,
        "end": end,
    }


def _paginate_rows(rows: list[Any], page: int) -> tuple[list[Any], dict[str, int]]:
    pagination = _pagination_meta(len(rows), page)
    offset = pagination["offset"]
    return rows[offset:offset + PAGE_SIZE], pagination


def _auto_search_download_limit(settings: Settings) -> int:
    return max(1, min(10000, int(getattr(settings, "max_active_downloads_for_search", 100) or 100)))


def _dynamic_actual_download_limit(settings: Settings) -> int:
    return max(0, min(1000, int(getattr(settings, "max_actual_downloading_for_search", 5) or 0)))


def _actually_downloading_count() -> int:
    """Count distinct slskd transfers that are doing network I/O *now*.

    Restored partial downloads are a subtle case: slskd can report historical
    ``startedAt``/``bytesTransferred`` values while the transfer is currently
    ``Queued, Remotely``.  Those rows are not active network transfers.  Likewise
    completed rows can lack ``endedAt`` in some payloads.  Use the current slskd
    state as the primary signal and deduplicate by live transfer id so duplicate
    Soulseekarr rows can never inflate the scheduler guard.
    """
    snapshots, _state = jobs.active_transfer_snapshots()
    seen: set[tuple[Any, ...]] = set()
    count = 0
    for job in jobs.active():
        if str(getattr(job, "status", "")) != "downloading":
            continue
        transfer = snapshots.get(int(job.id))
        if not transfer or not transfer_is_actively_downloading(transfer):
            continue

        transfer_id = str(transfer.get("id") or transfer.get("Id") or "").strip()
        username = str(
            transfer.get("username") or transfer.get("Username") or getattr(job, "username", "") or ""
        )
        if transfer_id:
            identity: tuple[Any, ...] = ("id", username.casefold(), transfer_id)
        else:
            filename = str(
                transfer.get("filename")
                or transfer.get("Filename")
                or getattr(job, "remote_filename", "")
                or ""
            )
            size = int(_number(transfer, "size", "Size", default=float(getattr(job, "size", 0) or 0)))
            identity = ("key", *remote_transfer_key(username, filename, size))
        if identity in seen:
            continue
        seen.add(identity)
        count += 1
    return count


def _auto_search_download_limit_reached(settings: Settings) -> bool:
    if bool(getattr(settings, "dynamic_active_download_limit_enabled", False)):
        # Dynamic mode replaces the total-active-job guard with a network-active
        # transfer guard. Equality is allowed by design: with a max of 5, search
        # may continue while 0..5 transfers are actually downloading and pauses
        # only when the sixth transfer is active.
        return _actually_downloading_count() > _dynamic_actual_download_limit(settings)

    # Preserve the historical static semantics: a configured maximum of 100
    # allows 100 active jobs and pauses additional automatic searches at 101.
    return jobs.active_count() > _auto_search_download_limit(settings)


def _auto_search_limit_notice(settings: Settings) -> str:
    if bool(getattr(settings, "dynamic_active_download_limit_enabled", False)):
        maximum = _dynamic_actual_download_limit(settings)
        actual = _actually_downloading_count()
        return (
            f"No Soulseek search was run because {actual} transfers are currently downloading, "
            f"which is above the dynamic automatic-search limit of {maximum}. "
            "Queued/preparing/importing jobs do not count toward this dynamic limit."
        )

    limit = _auto_search_download_limit(settings)
    return (
        f"No Soulseek search was run because there are more than {limit} active Soulseekarr downloads "
        f"(configured automatic-search limit: {limit})."
    )


async def _language_safe_candidates(
    sonarr: Any,
    candidates: list[SearchCandidate],
    ep: MissingEpisode | None = None,
) -> list[SearchCandidate]:
    """Apply Sonarr-backed language eligibility when the client supports it.

    Prefer parsing Soulseekarr's planned final filename when an episode is known.
    This is essential for title-only specials: Sonarr may not parse the raw peer
    filename, but it can parse the normalized ``S00E##`` name Soulseekarr will
    actually hand to ManualImport. Older test/custom client stubs retain the
    original candidate-only fallback.
    """
    if ep is not None:
        method = getattr(sonarr, "eligible_language_candidates_for_episode", None)
        if callable(method):
            result = method(ep, candidates)
            if inspect.isawaitable(result):
                result = await result
            if isinstance(result, list):
                return result
    method = getattr(sonarr, "eligible_language_candidates", None)
    if not callable(method):
        return candidates
    result = method(candidates)
    if inspect.isawaitable(result):
        result = await result
    return result if isinstance(result, list) else candidates


async def _candidate_language_safe(
    sonarr: Any, candidate: SearchCandidate, ep: MissingEpisode | None = None
) -> bool:
    if ep is not None:
        method = getattr(sonarr, "candidate_language_allowed_for_episode", None)
        if callable(method):
            result = method(ep, candidate)
            if inspect.isawaitable(result):
                result = await result
            if isinstance(result, bool):
                return result
    method = getattr(sonarr, "candidate_language_allowed", None)
    if not callable(method):
        return True
    result = method(candidate)
    if inspect.isawaitable(result):
        result = await result
    return bool(result) if isinstance(result, bool) else True


def _peer_cooldown_detail(username: str, cooldown: dict[str, Any]) -> str:
    until = str(cooldown.get("cooldownUntil") or "").replace("+00:00", "Z")
    reason = str(cooldown.get("reason") or "remote queue limit")
    if str(cooldown.get("kind") or "") == "enqueue_failure":
        return (
            f"Soulseek peer {username} is on enqueue-failure cooldown after '{reason}'. "
            f"Soulseekarr will skip new files from this peer until {until}."
        )
    return (
        f"Soulseek peer {username} is temporarily paused after '{reason}'. "
        f"New files from this peer can be queued again after {until}."
    )


def _file_rejection_ban_detail(username: str, ban: dict[str, Any]) -> str:
    until = str(ban.get("cooldownUntil") or "").replace("+00:00", "Z")
    reason = str(ban.get("reason") or "remote file unavailable")
    return (
        f"This file from Soulseek peer {username} is temporarily blocked after '{reason}'. "
        f"Soulseekarr will allow this exact peer/file again after {until}."
    )


def _queueable_peer_candidates(candidates: list[SearchCandidate]) -> list[SearchCandidate]:
    checker = getattr(jobs, "peer_cooldown_active", None)
    if not callable(checker):
        return candidates
    return [item for item in candidates if not checker(item.username)]


async def _usable_search_candidates(
    sonarr: Any,
    ep: MissingEpisode,
    results: list[SearchCandidate],
    settings: Settings,
) -> tuple[dict[tuple[str, str, int], SearchCandidate], list[SearchCandidate]]:
    """Apply the complete pre-download eligibility pipeline to search results.

    Query rotation is based on this final list, not on raw Soulseek hits. A
    result therefore does not keep a query variant sticky merely because it was
    returned by slskd: previously downloaded/active files, normal ranking
    rejects, language rejects, and Sonarr upgrade score/quality rejects are all
    considered unusable.
    """
    unique = {(item.username, item.filename, item.size): item for item in results}
    rejection_bans = jobs.active_file_rejection_ban_keys()
    available = [
        item for item in unique.values()
        if not jobs.unavailable_file(item.filename, item.size)
        and peer_file_key(item.username, item.filename, item.size) not in rejection_bans
    ]
    ranked = rank_candidates(ep, available, settings)
    ranked = await _language_safe_candidates(sonarr, ranked, ep)

    if ep.has_file:
        method = getattr(sonarr, "eligible_upgrade_candidates", None)
        if callable(method):
            upgraded = method(ep, ranked)
            if inspect.isawaitable(upgraded):
                upgraded = await upgraded
            if isinstance(upgraded, list):
                ranked = upgraded

    return unique, ranked


def _active_network_jobs(episode_id: int) -> list[Any]:
    """Return queued/downloading Soulseekarr jobs for one episode."""
    method = getattr(jobs, "episode_jobs", None)
    if not callable(method):
        return []
    try:
        rows = method(int(episode_id), ("queued", "downloading"))
    except Exception:
        return []
    return list(rows or [])


def _active_soulseekarr_jobs(episode_id: int) -> list[Any]:
    """Return every active Soulseekarr download-list job for one episode.

    Sonarr can see started and post-download Soulseekarr work through the
    read-only qBittorrent compatibility API.  Upgrade-page searches must not
    mistake those compatibility rows for work owned by another download client.
    """
    method = getattr(jobs, "episode_jobs", None)
    if not callable(method):
        return []
    statuses = tuple(getattr(jobs, "ACTIVE_STATUSES", ())) or (
        "queued", "downloading", "copying", "ready", "evaluating", "importing"
    )
    try:
        rows = method(int(episode_id), statuses)
    except Exception:
        return []
    return list(rows or [])


def _active_soulseekarr_download_markers() -> tuple[set[int], set[str]]:
    """Return episode ids and qBittorrent compatibility ids for active local jobs."""
    try:
        active = list(jobs.active())
    except Exception:
        active = []
    episode_ids = {
        int(item.episode_id)
        for item in active
        if int(getattr(item, "episode_id", 0) or 0) > 0
    }
    download_ids = {
        qbit_job_hash(int(item.id))
        for item in active
        if int(getattr(item, "id", 0) or 0) > 0
    }
    return episode_ids, download_ids


async def _external_sonarr_queue_episode_ids(
    sonarr: Any,
    *,
    local_episode_ids: set[int] | None = None,
    local_download_ids: set[str] | None = None,
) -> set[int]:
    """Return Sonarr queue episode ids that are not Soulseekarr's own jobs."""
    method = getattr(sonarr, "queued_episode_ids_excluding_download_ids", None)
    if callable(method):
        result = method(set(local_download_ids or set()))
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, (set, list, tuple)):
            return {int(value) for value in result}

    method = getattr(sonarr, "queued_episode_ids", None)
    if not callable(method):
        return set()
    result = method()
    if inspect.isawaitable(result):
        result = await result
    if not isinstance(result, (set, list, tuple)):
        return set()
    # Compatibility fallback for test doubles/older clients that cannot expose
    # per-download ids. Avoid mislabeling obvious local-only rows as Sonarr work.
    return {int(value) for value in result} - set(local_episode_ids or set())


async def _episode_blocked_by_external_sonarr_queue(sonarr: Any, episode_id: int) -> bool:
    """Return True only when Sonarr has queue work not owned by Soulseekarr.

    Soulseekarr exposes its own active jobs through the qBittorrent compatibility
    API, so a plain Sonarr queue check would block parallel better candidates.
    Ignore those known download ids while still yielding to every other Sonarr
    queue item.
    """
    active = _active_soulseekarr_jobs(episode_id)
    if active:
        method = getattr(sonarr, "episode_in_queue_excluding_download_ids", None)
        if callable(method):
            ignored = {qbit_job_hash(item.id) for item in active}
            result = method(int(episode_id), ignored)
            if inspect.isawaitable(result):
                return bool(await result)
            if isinstance(result, bool):
                return result

    method = getattr(sonarr, "episode_in_queue", None)
    if callable(method):
        result = method(int(episode_id))
        if inspect.isawaitable(result):
            return bool(await result)
        if isinstance(result, bool):
            return result
    return False


def _planned_import_filename(ep: MissingEpisode, remote_filename: str) -> str:
    """Return the exact basename Soulseekarr will hand to Sonarr after download.

    Sonarr Custom Formats are release-name driven.  Scoring the raw Soulseek path
    before download and a normalized filename after download can produce different
    scores, wasting bandwidth on a release that is rejected at import time.  Build
    the final name once from the episode metadata and remote path, score that name,
    persist it on the job, and reuse it during preparation.
    """
    return build_import_filename(
        series_title=ep.series_title,
        series_year=ep.series_year,
        episode_code=ep.episode_code,
        episode_title=ep.title,
        remote_filename=remote_filename,
    )


async def _release_profile(
    sonarr: Any, ep: MissingEpisode, remote_filename: str
) -> tuple[int, int, int] | None:
    """Return ``(CF score, resolution, source rank)`` for one candidate.

    Detailed source-aware clients are preferred. Test doubles and older clients
    that only expose score/resolution are accepted with source rank zero.
    """
    final_filename = _planned_import_filename(ep, remote_filename)

    async def call(method_name: str) -> Any:
        method = getattr(sonarr, method_name, None)
        if not callable(method):
            return None
        try:
            result = method(ep, final_filename)
            if inspect.isawaitable(result):
                result = await result
            return result
        except Exception:
            return None

    result = await call("candidate_release_quality_profile")
    if isinstance(result, tuple) and len(result) == 3:
        try:
            return int(result[0]), int(result[1]), max(0, int(result[2]))
        except (TypeError, ValueError):
            pass

    result = await call("candidate_release_profile")
    if not isinstance(result, tuple) or len(result) != 2:
        return None
    try:
        return int(result[0]), int(result[1]), 0
    except (TypeError, ValueError):
        return None


async def _installed_profile(sonarr: Any, episode_id: int) -> tuple[int, int, int] | None:
    """Return installed ``(CF score, resolution, source rank)`` when known."""

    async def call(method_name: str) -> Any:
        method = getattr(sonarr, method_name, None)
        if not callable(method):
            return None
        try:
            result = method(int(episode_id))
            if inspect.isawaitable(result):
                result = await result
            return result
        except Exception:
            return None

    result = await call("installed_episode_quality_profile")
    if isinstance(result, tuple) and len(result) == 3:
        try:
            score, resolution, source_rank = int(result[0]), int(result[1]), max(0, int(result[2]))
        except (TypeError, ValueError):
            pass
        else:
            return (score, resolution, source_rank) if resolution > 0 else None

    result = await call("installed_episode_profile")
    if not isinstance(result, tuple) or len(result) != 2:
        return None
    try:
        score, resolution = int(result[0]), int(result[1])
    except (TypeError, ValueError):
        return None
    return (score, resolution, 0) if resolution > 0 else None


def _profile_is_strict_upgrade(
    candidate: tuple[int, int, int], installed: tuple[int, int, int]
) -> bool:
    """Return True when no quality axis regresses and at least one improves."""
    candidate_score, candidate_resolution, candidate_source = candidate
    installed_score, installed_resolution, installed_source = installed
    return (
        candidate_score >= installed_score
        and candidate_resolution >= installed_resolution
        and candidate_source >= installed_source
        and (
            candidate_score > installed_score
            or candidate_resolution > installed_resolution
            or candidate_source > installed_source
        )
    )


async def _better_than_active_candidates(
    sonarr: Any,
    ep: MissingEpisode,
    candidates: list[SearchCandidate],
) -> list[SearchCandidate]:
    """Apply active-transfer upgrade gating only when Sonarr already has a file.

    A queued/downloading Soulseekarr transfer is *not* an installed episode file.
    For a genuinely missing episode (``has_file == False``), another eligible
    release may therefore be queued when the normal round-robin returns to that
    episode. Exact active/downloaded files are still removed earlier by
    ``jobs.unavailable_file()``, so this does not enqueue the same basename+size
    repeatedly.

    For proactive upgrades, where Sonarr already has an installed file, active
    upgrade transfers still form a frontier so Soulseekarr does not spend
    bandwidth on an alternative that is no better than an upgrade already in
    flight.
    """
    if not bool(ep.has_file):
        return candidates
    return await _candidates_against_active_frontier(sonarr, ep, candidates)


async def _candidates_against_active_frontier(
    sonarr: Any,
    ep: MissingEpisode,
    candidates: list[SearchCandidate],
) -> list[SearchCandidate]:
    """Require a strict CF/resolution/source improvement over active transfers."""
    active = _active_network_jobs(ep.id)
    if not active:
        return candidates

    active_profiles: list[tuple[int, int, int]] = []
    for item in active:
        # Re-parse active releases when possible so source quality participates
        # even for jobs created before source-aware comparison existed.
        profile = await _release_profile(sonarr, ep, str(item.remote_filename))
        if profile is not None:
            active_profiles.append(profile)
            continue
        stored_score = getattr(item, "candidate_custom_format_score", None)
        stored_resolution = getattr(item, "candidate_resolution", None)
        try:
            if stored_score is not None and int(stored_resolution or 0) > 0:
                active_profiles.append((int(stored_score), int(stored_resolution), 0))
        except (TypeError, ValueError):
            pass

    # If Sonarr cannot score any already-active release, do not guess and
    # multiply bandwidth usage. A later search can retry after metadata improves.
    if not active_profiles:
        return []

    best_score = max(score for score, _resolution, _source in active_profiles)
    best_resolution = max(resolution for _score, resolution, _source in active_profiles)
    best_source = max(source for _score, _resolution, source in active_profiles)
    eligible: list[tuple[int, int, int, float, SearchCandidate]] = []
    for candidate in candidates:
        profile = await _release_profile(sonarr, ep, candidate.filename)
        if profile is None:
            continue
        score, resolution, source_rank = profile
        if score < best_score or resolution < best_resolution or source_rank < best_source:
            continue
        if score == best_score and resolution == best_resolution and source_rank == best_source:
            continue
        eligible.append((score, resolution, source_rank, float(candidate.score), candidate))

    eligible.sort(key=lambda row: (row[0], row[1], row[2], row[3]), reverse=True)
    return [row[4] for row in eligible]


def _finish_search_variant(
    ep: MissingEpisode,
    variant_index: int,
    variant_count: int,
    *,
    usable_count: int,
    search_errors: list[str],
) -> int:
    """Persist which query spelling should be used on the next search.

    Successful searches stay on the same spelling while at least one candidate
    survives all eligibility checks. Only a successful but exhausted search
    advances to the next spelling. Upstream errors keep the current spelling so
    a transient slskd/Soulseek failure cannot churn through every query variant.
    """
    if search_errors or usable_count > 0:
        return variant_index
    return search_state.advance_variant(
        ep.id, variant_count, expected_index=variant_index
    )

def _series_download_enabled(series_id: int, sonarr_monitored: bool, overrides: dict[int, bool] | None = None) -> bool:
    choices = overrides if overrides is not None else jobs.series_download_overrides()
    if int(series_id) in choices:
        return bool(choices[int(series_id)])
    return bool(sonarr_monitored)


def _filter_download_enabled_episodes(episodes: list[MissingEpisode]) -> list[MissingEpisode]:
    overrides = jobs.series_download_overrides()
    return [
        ep for ep in episodes
        if ep.episode_monitored
        and ep.season_monitored
        and _series_download_enabled(ep.series_id, ep.series_monitored, overrides)
    ]


def _download_enabled_series_rows(series_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return Sonarr series rows that Soulseekarr is allowed to search.

    This is primarily used before the supplemental Season 00 scan so Soulseekarr
    does not issue one episode request for every disabled TV show merely to find
    undated specials that Sonarr omits from Wanted/Missing.
    """
    overrides = jobs.series_download_overrides()
    selected: list[dict[str, Any]] = []
    for row in series_rows:
        if not isinstance(row, dict):
            continue
        try:
            series_id = int(row.get("id", row.get("Id")))
        except (TypeError, ValueError):
            continue
        monitored = bool(row.get("monitored", row.get("Monitored", False)))
        if _series_download_enabled(series_id, monitored, overrides):
            selected.append(row)
    return selected


async def _all_enabled_episode_catalog(
    sonarr: SonarrClient,
    series_rows: list[dict[str, Any]],
    *,
    max_concurrency: int = 6,
) -> tuple[list[MissingEpisode], list[str]]:
    """Return all searchable episodes for Soulseekarr-enabled TV shows.

    Unlike Wanted/Missing this includes episodes that already have Sonarr files,
    so the Upgrade page can expose an explicit search action for every monitored
    episode in an enabled show. Existing files remain protected by Sonarr-backed
    strict-upgrade validation later in the candidate pipeline.
    """
    enabled_series = _download_enabled_series_rows(series_rows)
    if not enabled_series:
        return [], []

    semaphore = asyncio.Semaphore(max(1, min(16, int(max_concurrency or 1))))

    async def load(row: dict[str, Any]) -> list[MissingEpisode]:
        series_id = int(row.get("id", row.get("Id")))
        title = str(row.get("title", row.get("Title")) or f"Series {series_id}")
        try:
            year = int(row.get("year", row.get("Year")) or 0) or None
        except (TypeError, ValueError):
            year = None
        async with semaphore:
            # Upgrade is intentionally broader than Missing: the per-series
            # Soulseekarr Download toggle is authoritative, and all episodes in
            # an enabled show are listed even when an old Sonarr season/episode
            # is unmonitored. This matches proactive automatic upgrade discovery.
            return await sonarr.series_episodes_all(
                series_id, title, year, monitored_only=False
            )

    loaded = await asyncio.gather(
        *(load(row) for row in enabled_series), return_exceptions=True
    )
    episodes: list[MissingEpisode] = []
    errors: list[str] = []
    for row, result in zip(enabled_series, loaded):
        if isinstance(result, Exception):
            title = str(row.get("title", row.get("Title")) or "Unknown series")
            errors.append(f"{title}: {result}")
            continue
        episodes.extend(item for item in result if isinstance(item, MissingEpisode))

    episodes.sort(
        key=lambda ep: (
            ep.series_title.casefold(),
            int(ep.season_number),
            int(ep.episode_number),
            int(ep.id),
        )
    )
    return episodes, errors


def _upgrade_catalog_signature(
    sonarr: SonarrClient, series_rows: list[dict[str, Any]]
) -> tuple[Any, ...]:
    enabled = _download_enabled_series_rows(series_rows)
    rows: list[tuple[int, str, int | None]] = []
    for row in enabled:
        try:
            series_id = int(row.get("id", row.get("Id")))
        except (TypeError, ValueError):
            continue
        title = str(row.get("title", row.get("Title")) or "")
        try:
            year = int(row.get("year", row.get("Year")) or 0) or None
        except (TypeError, ValueError):
            year = None
        rows.append((series_id, title, year))
    rows.sort()
    return (str(getattr(sonarr, "base_url", "")), tuple(rows))


async def _cached_upgrade_episode_catalog(
    sonarr: SonarrClient,
    series_rows: list[dict[str, Any]],
    *,
    force: bool = False,
) -> tuple[list[MissingEpisode], list[str]]:
    signature = _upgrade_catalog_signature(sonarr, series_rows)
    now = time.monotonic()
    if (
        not force
        and _UPGRADE_CATALOG_CACHE.get("signature") == signature
        and float(_UPGRADE_CATALOG_CACHE.get("expires_at") or 0.0) > now
    ):
        return (
            list(_UPGRADE_CATALOG_CACHE.get("episodes") or []),
            list(_UPGRADE_CATALOG_CACHE.get("errors") or []),
        )

    async with _UPGRADE_CATALOG_LOCK:
        now = time.monotonic()
        if (
            not force
            and _UPGRADE_CATALOG_CACHE.get("signature") == signature
            and float(_UPGRADE_CATALOG_CACHE.get("expires_at") or 0.0) > now
        ):
            return (
                list(_UPGRADE_CATALOG_CACHE.get("episodes") or []),
                list(_UPGRADE_CATALOG_CACHE.get("errors") or []),
            )

        episodes, errors = await _all_enabled_episode_catalog(sonarr, series_rows)
        _UPGRADE_CATALOG_CACHE.update({
            "signature": signature,
            "expires_at": time.monotonic() + _UPGRADE_CATALOG_TTL_SECONDS,
            "episodes": list(episodes),
            "errors": list(errors),
        })
        return episodes, errors


def _merge_missing_episode_rows(
    wanted_rows: list[MissingEpisode],
    special_rows: list[MissingEpisode],
) -> list[MissingEpisode]:
    """Merge Wanted/Missing with direct Season 00 discovery by Sonarr id."""
    combined = {int(ep.id): ep for ep in wanted_rows}
    # Prefer the direct episode-resource copy for specials. It carries the exact
    # current monitor/file state even when Wanted/Missing omitted or lagged it.
    combined.update({int(ep.id): ep for ep in special_rows})
    return sorted(
        combined.values(),
        key=lambda ep: (
            ep.air_date_utc or "",
            ep.series_title.casefold(),
            int(ep.season_number),
            int(ep.episode_number),
            int(ep.id),
        ),
    )


async def _missing_catalog_with_specials(
    sonarr: SonarrClient,
    series_rows: list[dict[str, Any]],
    wanted_rows: list[MissingEpisode] | None = None,
) -> list[MissingEpisode]:
    """Return the complete missing catalog used by UI and automatic search.

    Sonarr's ``/wanted/missing`` can omit two useful classes of genuinely
    missing episodes: undated Season 00 specials and episodes whose download
    client item only just disappeared.  The Season 00 scan handles the former.
    For the latter, recent terminal Soulseekarr jobs are used only as bounded
    hints and Sonarr's direct episode endpoint is authoritative about ``hasFile``.
    This keeps recovery precise without scanning every episode in the library.
    """
    if wanted_rows is None:
        wanted_rows = await sonarr.missing_episodes(False)

    special_rows: list[MissingEpisode] = []
    special_method = getattr(sonarr, "missing_special_episodes", None)
    if callable(special_method):
        result = special_method(_download_enabled_series_rows(series_rows))
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, list):
            special_rows = [item for item in result if isinstance(item, MissingEpisode)]

    merged = _merge_missing_episode_rows(list(wanted_rows), special_rows)

    # A transfer can disappear from Sonarr's download-client queue before
    # Wanted/Missing catches up.  This is especially visible after an slskd
    # restart or after the user removes an orphaned transfer manually.  Ask
    # Sonarr directly about only the recent terminal Soulseekarr episode ids
    # that are not already represented above.  Direct episode state wins over
    # the stale queue/wanted views, but monitor/download filters are still
    # applied by callers exactly as for ordinary Wanted/Missing rows.
    recover_method = getattr(sonarr, "missing_episode_resources", None)
    if callable(recover_method):
        represented = {int(ep.id) for ep in merged}
        recovery_ids = [
            int(episode_id)
            for episode_id in jobs.recoverable_missing_episode_ids(128)
            if int(episode_id) not in represented
        ]
        if recovery_ids:
            result = recover_method(recovery_ids, series_rows)
            if inspect.isawaitable(result):
                result = await result
            if isinstance(result, list):
                recovered = [item for item in result if isinstance(item, MissingEpisode)]
                if recovered:
                    merged = _merge_missing_episode_rows(merged, recovered)

    return merged


async def _resolve_missing_episode_target(
    sonarr: SonarrClient,
    settings: Settings,
    episode_id: int,
    *,
    series_rows: list[dict[str, Any]] | None = None,
    wanted_rows: list[MissingEpisode] | None = None,
) -> MissingEpisode | None:
    """Resolve one missing target even when Sonarr Wanted/Missing omits it.

    Manual Search/Download receives a concrete Sonarr episode id.  First use
    Wanted/Missing; if that view is stale, ask Sonarr's direct episode endpoint
    for just this id.  This covers both undated specials and ordinary episodes
    whose old slskd/download-client item has just disappeared.
    """
    if wanted_rows is None:
        wanted_rows = await sonarr.missing_episodes(False)
    filtered = _filter_download_enabled_episodes(list(wanted_rows))
    found = next((ep for ep in filtered if int(ep.id) == int(episode_id)), None)
    if found is not None:
        return found

    if series_rows is None:
        series_rows = await sonarr.series()

    # Newer Soulseekarr/SonarrClient: resolve exactly one episode directly.
    # Sonarr's direct resource is authoritative for hasFile and avoids waiting
    # for /wanted/missing to catch up after a download-client item disappears.
    direct_method = getattr(sonarr, "missing_episode_resources", None)
    if callable(direct_method):
        result = direct_method([int(episode_id)], series_rows)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, list):
            resolved = next(
                (ep for ep in result if isinstance(ep, MissingEpisode) and int(ep.id) == int(episode_id)),
                None,
            )
            if resolved is not None:
                allowed = _filter_download_enabled_episodes([resolved])
                return allowed[0] if allowed else None
            return None

    # Compatibility fallback for test doubles/older client implementations:
    # preserve the previous direct Season 00 resolver.
    raw = await sonarr.episode(int(episode_id))
    if not isinstance(raw, dict) or not raw:
        return None
    try:
        season_number = int(raw.get("seasonNumber", raw.get("SeasonNumber")) or 0)
        series_id = int(raw.get("seriesId", raw.get("SeriesId")) or 0)
    except (TypeError, ValueError):
        return None
    if season_number != 0 or series_id <= 0:
        return None
    if bool(raw.get("hasFile", raw.get("HasFile", False))):
        return None

    series_row = None
    for row in series_rows:
        if not isinstance(row, dict):
            continue
        try:
            row_id = int(row.get("id", row.get("Id")))
        except (TypeError, ValueError):
            continue
        if row_id == series_id:
            series_row = row
            break
    if series_row is None:
        return None

    enabled_rows = _download_enabled_series_rows([series_row])
    if not enabled_rows:
        return None

    title = str(series_row.get("title", series_row.get("Title")) or f"Series {series_id}")
    try:
        year = int(series_row.get("year", series_row.get("Year")) or 0) or None
    except (TypeError, ValueError):
        year = None
    specials = await sonarr.season_episodes(
        series_id, 0, title, year, monitored_only=False
    )
    resolved = next(
        (ep for ep in specials if int(ep.id) == int(episode_id) and not ep.has_file),
        None,
    )
    if resolved is None:
        return None
    allowed = _filter_download_enabled_episodes([resolved])
    return allowed[0] if allowed else None


async def _resolve_enabled_episode_target(
    sonarr: SonarrClient,
    settings: Settings,
    episode_id: int,
    *,
    series_rows: list[dict[str, Any]] | None = None,
) -> MissingEpisode | None:
    """Resolve one monitored episode from a Soulseekarr-enabled TV show.

    This is the targeted-search resolver used by the Upgrade page. The
    per-series Soulseekarr Download choice is authoritative here, matching the
    proactive automatic-upgrade catalog even for old unmonitored seasons.
    """
    if series_rows is None:
        series_rows = await sonarr.series()

    raw = await sonarr.episode(int(episode_id))
    if not isinstance(raw, dict) or not raw:
        return None
    try:
        series_id = int(raw.get("seriesId", raw.get("SeriesId")) or 0)
    except (TypeError, ValueError):
        return None
    if series_id <= 0:
        return None

    series_row = None
    for row in series_rows:
        if not isinstance(row, dict):
            continue
        try:
            row_id = int(row.get("id", row.get("Id")))
        except (TypeError, ValueError):
            continue
        if row_id == series_id:
            series_row = row
            break
    if series_row is None:
        return None

    monitored = bool(series_row.get("monitored", series_row.get("Monitored", False)))
    if not _series_download_enabled(series_id, monitored):
        return None

    title = str(series_row.get("title", series_row.get("Title")) or f"Series {series_id}")
    try:
        year = int(series_row.get("year", series_row.get("Year")) or 0) or None
    except (TypeError, ValueError):
        year = None

    rows = await sonarr.series_episodes_all(
        series_id, title, year, monitored_only=False
    )
    resolved = next(
        (ep for ep in rows if isinstance(ep, MissingEpisode) and int(ep.id) == int(episode_id)),
        None,
    )
    return resolved


async def _resolve_search_episode_target(
    sonarr: SonarrClient,
    settings: Settings,
    episode_id: int,
    *,
    allow_existing: bool,
    series_rows: list[dict[str, Any]] | None = None,
    wanted_rows: list[MissingEpisode] | None = None,
) -> MissingEpisode | None:
    if allow_existing:
        return await _resolve_enabled_episode_target(
            sonarr, settings, episode_id, series_rows=series_rows
        )
    return await _resolve_missing_episode_target(
        sonarr,
        settings,
        episode_id,
        series_rows=series_rows,
        wanted_rows=wanted_rows,
    )


def _target_search_eligible(ep: MissingEpisode, settings: Settings) -> bool:
    # The missing-episode delay exists to avoid grabbing a just-aired missing
    # release too early. An already-installed episode is a proactive upgrade
    # target and is not suppressed by that delay, matching the automatic cycle.
    if ep.has_file:
        return True
    return episode_search_eligible(
        ep, delay_hours=settings.missing_episode_delay_hours
    )


def _series_options(series_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    overrides = jobs.series_download_overrides()
    search_modes = jobs.series_search_modes()
    result: list[dict[str, Any]] = []
    for item in series_rows:
        try:
            series_id = int(item.get("id"))
        except (TypeError, ValueError):
            continue
        monitored = bool(item.get("monitored", False))
        result.append({
            "id": series_id,
            "title": str(item.get("title") or f"Series {series_id}"),
            "year": int(item.get("year") or 0) or None,
            "monitored": monitored,
            "download_enabled": _series_download_enabled(series_id, monitored, overrides),
            "overridden": series_id in overrides,
            "search_mode": search_modes.get(series_id, "current"),
            "media_type": "movie" if is_movie_id(series_id) else "series",
        })
    result.sort(key=lambda row: (row["title"].casefold(), row["year"] or 0))
    return result

def _saved_sonarr_series_rows() -> tuple[list[dict[str, Any]], dict[str, str | None]]:
    """Return the last persisted Sonarr series snapshot without network I/O."""
    return jobs.sonarr_series_snapshot()


async def refresh_sonarr_series_snapshot(settings: Settings) -> tuple[list[dict[str, Any]], str | None]:
    """Refresh the Sonarr series snapshot in background-owned code only."""
    if not media_configured(settings):
        jobs.save_sonarr_series_snapshot([])
        return [], None

    try:
        sonarr = media_client(settings, SonarrClient)
        rows = await sonarr.series()
        jobs.sync_series_preferences(rows, settings.new_series_download_enabled)
        jobs.save_sonarr_series_snapshot(rows)
        return list(rows), None
    except Exception as exc:
        error = _upstream_error_message(exc)
        jobs.record_sonarr_series_snapshot_error(error)
        return jobs.sonarr_series_snapshot()[0], error


async def series_snapshot_loop() -> None:
    """Continuously refresh TV-show state independently of browser traffic."""
    while True:
        try:
            await refresh_sonarr_series_snapshot(settings_store.load())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            try:
                jobs.record_sonarr_series_snapshot_error(_upstream_error_message(exc))
            except Exception:
                pass

        try:
            await asyncio.wait_for(_SERIES_REFRESH_EVENT.wait(), timeout=_SERIES_SNAPSHOT_INTERVAL_SECONDS)
            _SERIES_REFRESH_EVENT.clear()
        except asyncio.TimeoutError:
            pass


def _filter_series_options(
    all_series_options: list[dict[str, Any]], query: str
) -> list[dict[str, Any]]:
    query = str(query or "").strip()
    if not query:
        return all_series_options
    needle = query.casefold()
    return [
        item for item in all_series_options
        if needle in str(item.get("title") or "").casefold()
        or needle in str(item.get("year") or "").casefold()
    ]


async def _series_page_payload(
    settings: Settings, page: int = 1, q: str = "", *, request_refresh: bool = False
) -> dict[str, Any]:
    # This path is intentionally SQLite-only. Browser navigation, filtering,
    # pagination and prefetching must never wait on Sonarr.
    series_rows, snapshot_state = _saved_sonarr_series_rows()
    if request_refresh:
        _SERIES_REFRESH_EVENT.set()
    all_series_options = _series_options(series_rows)
    series_query = str(q or "").strip()
    filtered_series = _filter_series_options(all_series_options, series_query)
    visible_series, pagination = _paginate_rows(filtered_series, page)
    return {
        "seriesOptions": visible_series,
        "seriesEnabledCount": sum(1 for item in all_series_options if item["download_enabled"]),
        "seriesTotalCount": len(all_series_options),
        "seriesQuery": series_query,
        "pagination": pagination,
        "configured": configured(settings),
        "error": snapshot_state.get("error"),
        "snapshotRefreshedAt": snapshot_state.get("refreshedAt"),
        "snapshotErrorAt": snapshot_state.get("errorAt"),
        "refreshRequested": bool(request_refresh),
    }

async def _sync_series_preferences_from_sonarr(sonarr: SonarrClient, settings: Settings) -> list[dict[str, Any]]:
    """Refresh persisted per-series choices and the shared UI snapshot.

    Automatic/manual search paths may already need a live Sonarr series call.
    Whenever they succeed, reuse that response to refresh the persisted TV-show
    snapshot too. Failure remains best-effort and never blocks episode search.
    """
    try:
        series_rows = await sonarr.series()
    except Exception as exc:
        jobs.record_sonarr_series_snapshot_error(_upstream_error_message(exc))
        return []
    jobs.sync_series_preferences(series_rows, settings.new_series_download_enabled)
    jobs.save_sonarr_series_snapshot(series_rows)
    return series_rows

# Automatic episode-search bundles are intentionally serialized. A
# scheduler-triggered bundle and a manual "Run auto cycle" request must never
# select targets at the same time. _AUTO_SEARCH_NEXT_AT is only the short gap
# between alternate query spellings inside one episode bundle.
_AUTO_CYCLE_RUNNING = False
_AUTO_SEARCH_NEXT_AT = 0.0
# Monotonic completion timestamp of the last scheduler-triggered episode
# bundle. The next primary episode search is scheduled from completion so time
# spent waiting for search responses/fallback variants never shortens the
# configured inter-episode interval.
_AUTO_SCHEDULER_LAST_RUN_AT = 0.0


async def _wait_for_auto_search_slot(min_gap_seconds: float) -> None:
    loop = asyncio.get_running_loop()
    wait_for = _AUTO_SEARCH_NEXT_AT - loop.time()
    if wait_for > 0:
        await asyncio.sleep(wait_for)


def _mark_auto_search_complete(min_gap_seconds: float) -> None:
    global _AUTO_SEARCH_NEXT_AT
    loop = asyncio.get_running_loop()
    _AUTO_SEARCH_NEXT_AT = loop.time() + max(0.0, float(min_gap_seconds))




def _auto_search_schedule_status(settings: Settings) -> dict[str, Any]:
    """Return the current scheduler state for the Settings UI/API."""
    if not configured(settings):
        return {
            "enabled": False,
            "running": bool(_AUTO_CYCLE_RUNNING),
            "state": "unconfigured",
            "nextRunAt": None,
            "secondsUntil": None,
            "message": "Automatic search is waiting for Sonarr and slskd configuration.",
        }
    if not settings.auto_search_enabled:
        return {
            "enabled": False,
            "running": bool(_AUTO_CYCLE_RUNNING),
            "state": "disabled",
            "nextRunAt": None,
            "secondsUntil": None,
            "message": "Automatic search is disabled.",
        }
    if not settings.auto_download:
        return {
            "enabled": False,
            "running": bool(_AUTO_CYCLE_RUNNING),
            "state": "paused",
            "nextRunAt": None,
            "secondsUntil": None,
            "message": "Automatic search cycles are paused because Automatic downloads is disabled.",
        }
    if _AUTO_CYCLE_RUNNING:
        return {
            "enabled": True,
            "running": True,
            "state": "running",
            "nextRunAt": None,
            "secondsUntil": 0,
            "message": "Automatic search cycle is running now.",
        }

    interval = max(60, int(settings.scan_interval_minutes) * 60)
    loop = asyncio.get_running_loop()
    if _AUTO_SCHEDULER_LAST_RUN_AT == 0.0:
        seconds_until = 0.0
    else:
        seconds_until = max(0.0, (_AUTO_SCHEDULER_LAST_RUN_AT + interval) - loop.time())
    next_run = datetime.now(timezone.utc) + timedelta(seconds=seconds_until)
    return {
        "enabled": True,
        "running": False,
        "state": "due" if seconds_until <= 0.5 else "scheduled",
        "nextRunAt": next_run.isoformat().replace("+00:00", "Z"),
        "secondsUntil": int(round(seconds_until)),
        "message": "Automatic search is due now." if seconds_until <= 0.5 else "Automatic search is scheduled.",
    }


def _upstream_error_message(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status == 429:
            return "slskd rate-limited a Soulseek search (HTTP 429) after retries."
        return f"slskd search request failed with HTTP {status}."
    return str(exc) or exc.__class__.__name__


async def collect_search_results(
    slskd: SlskdClient,
    ep: Any,
    settings: Settings,
    *,
    variant_index: int | None = None,
    record_cooldown: bool = True,
    persist_variant: bool = True,
) -> tuple[list[SearchCandidate], list[str], str, int, int]:
    """Run exactly one query variant without rotating it pre-emptively.

    ``variant_index`` is used by manual searches and automatic query bundles.
    Explicit manual choices are persisted by default. Automatic bundles can set
    ``persist_variant=False`` so trying every enabled spelling does not rewrite
    the manual-search cursor.
    """
    queries = search_queries(ep, jobs.series_search_mode(ep.series_id))
    if variant_index is None:
        variant_index = search_state.current_variant(ep.id, len(queries))
    else:
        if variant_index < 0 or variant_index >= len(queries):
            raise ValueError("variant_index is outside the available search variants")
        if persist_variant:
            variant_index = search_state.set_variant(ep.id, len(queries), variant_index)
    query = queries[variant_index]

    def remember_search(search_id: str) -> None:
        search_state.record_completed_search(search_id, settings.slskd_url, retention_seconds=3600)

    try:
        results = await slskd.search(
            query,
            settings.search_timeout_seconds,
            settings.max_search_results,
            on_finished=remember_search,
        )
        if record_cooldown:
            search_state.record_search_success(ep.id)
        return results, [], query, variant_index, len(queries)
    except Exception as exc:
        return [], [_upstream_error_message(exc)], query, variant_index, len(queries)


async def _automatic_search_candidates(
    slskd: SlskdClient,
    sonarr: SonarrClient,
    ep: MissingEpisode,
    settings: Settings,
    *,
    pace: bool = False,
) -> dict[str, Any]:
    """Run one logical automatic episode-search bundle.

    Every query form enabled for the episode is tried exactly once, in the
    preferred order returned by ``search_queries``. Ordinary ``both`` mode
    therefore runs ``S01E02`` followed by ``1x02``; specials with a title run
    ``series + episode title`` followed by ``S00E02``. The shorter
    ``auto_search_gap_seconds`` delay applies only between those related query
    forms. A transient upstream error stops the bundle so Soulseekarr does not
    turn one failure into a burst of retries.

    Candidates from all successful variants are merged and ranked together, so
    the second query can still improve on a usable result from the first.
    """
    queries = search_queries(ep, jobs.series_search_mode(ep.series_id))
    variant_count = max(1, len(queries))
    attempted_variants: list[int] = []
    attempted_queries: list[str] = []
    all_errors: list[str] = []
    raw_unique: dict[tuple[str, str, int], SearchCandidate] = {}
    usable_unique: dict[tuple[str, str, int], SearchCandidate] = {}

    for variant_index in range(variant_count):
        if pace and attempted_variants:
            await _wait_for_auto_search_slot(settings.auto_search_gap_seconds)
        try:
            results, errors, query, selected_index, reported_count = await collect_search_results(
                slskd,
                ep,
                settings,
                variant_index=variant_index,
                record_cooldown=False,
                persist_variant=False,
            )
        finally:
            if pace:
                _mark_auto_search_complete(settings.auto_search_gap_seconds)

        variant_count = max(1, int(reported_count))
        attempted_variants.append(int(selected_index))
        attempted_queries.append(str(query))
        all_errors.extend(str(error) for error in errors)

        for item in results:
            raw_unique[(item.username, item.filename, int(item.size))] = item

        _unique, ranked = await _usable_search_candidates(sonarr, ep, results, settings)
        ranked = await _better_than_active_candidates(sonarr, ep, ranked)
        for item in ranked:
            key = (item.username, item.filename, int(item.size))
            previous = usable_unique.get(key)
            if previous is None or float(item.score) > float(previous.score):
                usable_unique[key] = item

        # A network/API failure is not evidence that the next spelling has no
        # results. Stop here and let adaptive error backoff retry later.
        if errors:
            break

    selected_ranked = sorted(
        usable_unique.values(), key=lambda item: float(item.score), reverse=True
    )
    if ep.is_movie:
        selected_ranked = await _language_safe_candidates(sonarr, selected_ranked, ep)
    return {
        "ranked": selected_ranked,
        "usable_count": len(selected_ranked),
        "raw_result_count": len(raw_unique),
        "errors": all_errors,
        "query": " | ".join(dict.fromkeys(attempted_queries)),
        "attempted_queries": attempted_queries,
        "attempted_variants": attempted_variants,
        "variant_count": int(variant_count),
    }


def _adaptive_outcome_delay(
    ep: MissingEpisode,
    target_type: str,
    outcome: str,
    settings: Settings,
) -> float:
    """Persist an adaptive automatic-search cooldown and return its hours."""
    next_streak = search_state.outcome_streak(ep.id, target_type, outcome) + 1
    delay = adaptive_search_delay_hours(
        target_type, outcome, next_streak, settings.repeat_search_cooldown_hours
    )
    search_state.record_search_outcome(
        ep.id, target_type, outcome, delay
    )
    return delay




async def cleanup_search_history(settings: Settings) -> None:
    """Delete Soulseekarr-created slskd searches one hour after collection."""
    if not settings.slskd_url or not settings.slskd_api_key:
        return
    due = search_state.due_searches(settings.slskd_url, limit=100)
    if not due:
        return
    slskd = SlskdClient(settings.slskd_url, settings.slskd_api_key, timeout=20)
    for search_id in due:
        try:
            await slskd.delete_search(search_id)
        except Exception:
            # Leave it persisted so a later cleanup pass can retry.
            continue
        search_state.forget_search(search_id)


def cleanup_database_history(settings: Settings) -> dict[str, int]:
    """Prune expired Soulseekarr history according to the configured retention."""
    return jobs.prune_history(settings.history_retention_hours)


async def cleanup_slskd_download_history(settings: Settings) -> None:
    """Keep slskd's Downloads view limited to active transfers.

    slskd classifies succeeded, errored, cancelled, timed-out, rejected, and
    aborted transfers as completed. Its cleanup endpoint only soft-removes those
    terminal records; queued/requested/initializing/in-progress transfers and the
    downloaded files themselves are not deleted. Soulseekarr keeps its own job
    and import history in SQLite, so clearing slskd's terminal rows loses no
    Soulseekarr history.
    """
    if not settings.slskd_url or not settings.slskd_api_key:
        return
    slskd = SlskdClient(settings.slskd_url, settings.slskd_api_key, timeout=20)
    await slskd.clear_completed_downloads()


async def maintain_queued_download_health(settings: Settings) -> None:
    """Track remote queue progress and remove queues that never improve.

    This maintenance path deliberately does **not** launch Soulseek searches.
    Automatic episode discovery is owned exclusively by :func:`auto_cycle`, so
    queue maintenance can never bypass the persisted TV-show -> season ->
    episode round-robin. A queued episode may still be searched later when the
    normal automatic rotation reaches it and its repeat-search cooldown is due.

    About once per hour, each queued Soulseekarr transfer is asked for its
    remote queue place. A lower number than the previous known place resets
    strikes to zero; same/worse or missing position adds one strike. 168
    consecutive non-improving hourly observations (about seven days)
    cancels/removes the transfer.
    """
    if not configured(settings):
        return

    slskd = SlskdClient(settings.slskd_url, settings.slskd_api_key, timeout=20)
    payload = await slskd.all_downloads()
    transfer_rows = slskd.flatten_downloads(payload)
    queue_check_candidates: list[tuple[float, Any, dict[str, Any]]] = []
    delete_candidates: list[tuple[Any, dict[str, Any]]] = []

    for transfer in transfer_rows:
        username = str(transfer.get("username") or transfer.get("Username") or "")
        filename = str(transfer.get("filename") or transfer.get("Filename") or "")
        try:
            size = int(transfer.get("size", transfer.get("Size")) or 0)
        except (TypeError, ValueError):
            size = 0
        if not username or not filename or size <= 0:
            continue
        job = jobs.latest_for_transfer(username, filename, size)
        if job is None or job.downloaded_at is not None:
            # Never manage arbitrary/manual slskd transfers. Only rows with a
            # matching Soulseekarr job are eligible.
            continue
        if not transfer_is_queued(transfer):
            if transfer_has_started(transfer):
                if (
                    int(getattr(job, "queue_strikes", 0) or 0) > 0
                    or getattr(job, "queue_last_position", None) is not None
                    or getattr(job, "queue_last_checked_at", None)
                ):
                    jobs.reset_queue_checks(job.id)
            continue

        # A restored partial transfer can be queued again while retaining old
        # startedAt/bytesTransferred values.  Current queue state wins, so it
        # remains under queue-health tracking rather than being treated as live
        # network I/O.

        # The first strike evaluation is due one hour after the transfer was
        # queued. Subsequent evaluations use the persisted last-check time, so
        # scheduler restarts cannot accelerate the strike counter.
        last_checked = parse_utc(getattr(job, "queue_last_checked_at", None))
        baseline = last_checked or transfer_queue_started_at(transfer, job.created_at)
        if baseline is None:
            due_age = QUEUE_STRIKE_INTERVAL_SECONDS
        else:
            due_age = max(0.0, (datetime.now(timezone.utc) - baseline).total_seconds())
        if due_age >= QUEUE_STRIKE_INTERVAL_SECONDS:
            due_order = baseline.timestamp() if baseline is not None else 0.0
            queue_check_candidates.append((due_order, job, transfer))

    queue_check_candidates.sort(key=lambda item: (item[0], int(item[1].id)))
    for _due_order, job, transfer in queue_check_candidates[:QUEUE_STRIKE_MAX_PROBES_PER_PASS]:
        transfer_id = str(transfer.get("id") or transfer.get("Id") or "").strip()
        transfer_username = str(
            transfer.get("username") or transfer.get("Username") or job.username
        ).strip()
        refreshed: dict[str, Any] | None = None
        if transfer_id and transfer_username:
            try:
                refreshed = await slskd.queue_position(
                    transfer_username,
                    transfer_id,
                    wait_for_slot=True,
                    return_cached_on_error=False,
                )
            except Exception:
                # An attempted hourly check with no reported place is itself a
                # non-improving observation.
                refreshed = None

        position = transfer_queue_position(refreshed)
        checked = jobs.record_queue_check(job.id, position)
        if int(getattr(checked, "queue_strikes", 0) or 0) >= QUEUE_STRIKE_LIMIT:
            delete_candidates.append((checked, transfer))

    # Re-read each strike-limit candidate immediately before deletion so a
    # transfer that started between the hourly probe and cleanup is never cancelled.
    for job, _transfer in delete_candidates:
        try:
            current = await slskd.find_download(job.username, job.remote_filename, job.size)
            if not current or not transfer_is_queued(current):
                if current and transfer_has_started(current):
                    jobs.reset_queue_checks(job.id)
                continue
            transfer_id = str(current.get("id") or current.get("Id") or "").strip()
            if not transfer_id:
                continue
            await slskd.cancel_download(job.username, transfer_id, remove=True)
            jobs.update(
                job.id,
                status="cancelled",
                message=(
                    f"Removed from slskd after {QUEUE_STRIKE_LIMIT} hourly queue strikes "
                    "without queue-position improvement (about 7 days)."
                ),
            )
        except Exception:
            # Leave it active so a later maintenance pass can safely retry.
            continue


async def queue_candidate_with_season_folder(
    ep: MissingEpisode,
    candidate: SearchCandidate,
    settings: Settings,
    sonarr: SonarrClient,
    slskd: SlskdClient | None = None,
    *,
    expand_season_folder: bool = True,
    retry_parent_job_id: int | None = None,
    transfer_retry_count: int = 0,
) -> tuple[list[Any], str | None]:
    """Queue a selected file and matching episodes beside it in the same peer folder.

    The selected episode is always queued first. Folder expansion is best-effort:
    if the peer goes offline or refuses a directory listing, the selected file
    continues normally. Other files are only added when they map cleanly to an
    episode from the same Sonarr series/season and pass the normal Soulseekarr
    file/size/series checks. Existing Sonarr files are intentionally included;
    v0.3.26+ lets Sonarr score them as possible Custom Format upgrades after the
    downloads finish.
    """
    created: list[Any] = []

    cooldown = jobs.peer_cooldown(candidate.username)
    if cooldown is not None:
        return created, _peer_cooldown_detail(candidate.username, cooldown)

    rejection_ban = jobs.file_rejection_ban(
        candidate.username, candidate.filename, candidate.size
    )
    if rejection_ban is not None:
        return created, _file_rejection_ban_detail(candidate.username, rejection_ban)

    candidate_profile = await _release_profile(sonarr, ep, candidate.filename)
    if ep.is_movie and candidate_profile is None:
        return created, "Radarr could not recognize and score this movie release. It was not queued."
    installed_profile = await _installed_profile(sonarr, ep.id)
    if installed_profile is not None:
        if candidate_profile is None:
            return created, (
                "Sonarr already has an episode file, but the selected Soulseek release could not "
                "be scored reliably as an upgrade. It was not queued."
            )
        if not _profile_is_strict_upgrade(candidate_profile, installed_profile):
            candidate_score, candidate_resolution, candidate_source = candidate_profile
            installed_score, installed_resolution, installed_source = installed_profile
            return created, (
                "Not queued: Sonarr already has an equal or better episode file "
                f"(CF score {installed_score}, {installed_resolution}p, source rank {installed_source}); "
                f"selected Soulseek candidate is CF score {candidate_score}, {candidate_resolution}p, "
                f"source rank {candidate_source}."
            )

    selected_target_name = _planned_import_filename(ep, candidate.filename)
    selected_job = jobs.create(
        episode_id=ep.id,
        series_title=ep.series_title,
        episode_code=ep.episode_code,
        episode_title=ep.title,
        username=candidate.username,
        remote_filename=candidate.filename,
        size=candidate.size,
        prepare_target_name=selected_target_name,
        candidate_custom_format_score=(candidate_profile[0] if candidate_profile is not None else None),
        candidate_resolution=(candidate_profile[1] if candidate_profile is not None else None),
        retry_parent_job_id=retry_parent_job_id,
        transfer_retry_count=transfer_retry_count,
    )
    created.append(selected_job)
    runner.start(selected_job.id)

    if ep.is_movie or not expand_season_folder:
        return created, None

    client = slskd or SlskdClient(settings.slskd_url, settings.slskd_api_key, timeout=20)
    try:
        season_episodes = await sonarr.season_episodes(
            ep.series_id,
            ep.season_number,
            ep.series_title,
            ep.series_year,
            True,
        )
        season_episodes = order_season_folder_episodes(season_episodes)
        if len(season_episodes) <= 1:
            return created, None

        folder_candidates = await client.directory_candidates(candidate.username, candidate.filename)
        if not folder_candidates:
            return created, None

        try:
            sonarr_queue_ids = await sonarr.queued_episode_ids()
        except Exception:
            # The selected job already passed the queue check. If the broad queue
            # lookup fails, do not risk bulk-queueing siblings Sonarr may own.
            return created, "Could not verify Sonarr queue for season-folder expansion; only the selected file was queued."

        used_files = {(candidate.filename.casefold(), int(candidate.size))}
        rejection_bans = jobs.active_file_rejection_ban_keys()
        for sibling_ep in season_episodes:
            if sibling_ep.id == ep.id:
                continue
            # Sonarr episode monitoring remains authoritative even when the
            # series itself is opted into Soulseekarr. This also respects
            # unmonitored seasons, because their episodes are unmonitored.
            if not sibling_ep.episode_monitored or not sibling_ep.season_monitored:
                continue
            # Season-folder expansion must obey the same release-age rule as
            # normal searches; otherwise a full-season folder could bypass the
            # configured missing-episode delay for future/just-aired episodes.
            if not episode_search_eligible(
                sibling_ep, delay_hours=settings.missing_episode_delay_hours
            ):
                continue
            active_sibling_jobs = _active_network_jobs(sibling_ep.id)
            if sibling_ep.id in sonarr_queue_ids:
                if not active_sibling_jobs:
                    continue
                if await _episode_blocked_by_external_sonarr_queue(sonarr, sibling_ep.id):
                    continue

            available = [
                item for item in folder_candidates
                if (item.filename.casefold(), int(item.size)) not in used_files
                and not jobs.unavailable_file(item.filename, item.size)
                and peer_file_key(item.username, item.filename, item.size) not in rejection_bans
            ]
            ranked = rank_candidates(sibling_ep, available, settings)
            ranked = await _language_safe_candidates(sonarr, ranked, sibling_ep)
            if not ranked:
                continue

            # Existing Sonarr files are upgrade candidates, not ordinary missing
            # downloads. Before spending bandwidth, let Sonarr parse every
            # candidate and keep only releases that are no worse in Custom
            # Format score, resolution, or source quality and strictly better in
            # at least one. Sonarr's CF score remains the primary ordering.
            if sibling_ep.has_file:
                ranked = await sonarr.eligible_upgrade_candidates(sibling_ep, ranked)
                if not ranked:
                    continue

            if active_sibling_jobs:
                ranked = await _better_than_active_candidates(sonarr, sibling_ep, ranked)
                if not ranked:
                    continue

            best = ranked[0]
            # Recheck immediately before insertion because an earlier sibling or
            # concurrent manual action may have claimed the same file.
            if jobs.unavailable_file(best.filename, best.size):
                continue
            sibling_profile = await _release_profile(sonarr, sibling_ep, best.filename)
            sibling_target_name = _planned_import_filename(sibling_ep, best.filename)
            sibling_job = jobs.create(
                episode_id=sibling_ep.id,
                series_title=sibling_ep.series_title,
                episode_code=sibling_ep.episode_code,
                episode_title=sibling_ep.title,
                username=best.username,
                remote_filename=best.filename,
                size=best.size,
                prepare_target_name=sibling_target_name,
                candidate_custom_format_score=(sibling_profile[0] if sibling_profile is not None else None),
                candidate_resolution=(sibling_profile[1] if sibling_profile is not None else None),
            )
            created.append(sibling_job)
            used_files.add((best.filename.casefold(), int(best.size)))
            runner.start(sibling_job.id)

        return created, None
    except SlskdPeerDirectoryUnavailable as exc:
        # A peer may answer the Soulseek search and disappear before slskd can
        # open a direct connection for GetDirectoryContents. The selected file
        # is already queued and remains valid; only the folder expansion is
        # skipped for this attempt.
        return created, f"Season-folder expansion skipped: {exc}"
    except Exception as exc:
        # Season expansion is an optimization. Never sacrifice the explicitly
        # selected download when the peer's directory API is unavailable.
        return created, f"Season-folder expansion failed: {type(exc).__name__}: {exc}"




def _select_priority_probe(
    targets: list[MissingEpisode],
    settings: Settings,
    covered_seasons: set[tuple[int, int]],
    *,
    universe: list[MissingEpisode] | None = None,
) -> MissingEpisode | None:
    """Pick one globally prioritized season probe with round-robin tie breaking.

    The scheduler first decides *which priority pool* is allowed to run. Inside
    that pool, older/never-searched episodes win. Persistent series -> season ->
    episode cursors break ties and prevent one show from monopolizing equally old
    work. Only one probe per series is considered at a time; after a real search
    moves the series cursor, the next selection naturally offers another show.
    """
    remaining = [
        ep for ep in targets
        if (int(ep.series_id), int(ep.season_number)) not in covered_seasons
    ]
    if not remaining:
        return None

    last_times = search_state.last_search_times([int(ep.id) for ep in remaining])
    by_series: dict[int, list[MissingEpisode]] = {}
    for ep in remaining:
        by_series.setdefault(int(ep.series_id), []).append(ep)
    universe_by_series: dict[int, list[MissingEpisode]] = {}
    for ep in (universe if universe is not None else remaining):
        if (int(ep.series_id), int(ep.season_number)) in covered_seasons:
            continue
        universe_by_series.setdefault(int(ep.series_id), []).append(ep)

    series_order = search_state.auto_series_order(list(by_series))
    series_rank = {int(series_id): index for index, series_id in enumerate(series_order)}
    series_probes: list[tuple[float, int, MissingEpisode]] = []

    for series_id in series_order:
        rows = by_series.get(int(series_id), [])
        universe_rows = universe_by_series.get(int(series_id), rows)
        season_numbers = sorted({int(ep.season_number) for ep in universe_rows})
        season_order = search_state.auto_season_order(int(series_id), season_numbers)
        season_probes: list[tuple[float, int, MissingEpisode]] = []

        for season_rank, season_number in enumerate(season_order):
            season_rows = [
                ep for ep in rows if int(ep.season_number) == int(season_number)
            ]
            if not season_rows:
                continue
            season_universe = [
                ep for ep in universe_rows if int(ep.season_number) == int(season_number)
            ] or season_rows
            ring = order_round_robin_episodes(
                season_universe, settings.search_order, int(series_id), int(season_number)
            )
            episode_order = search_state.auto_episode_order(
                int(series_id), int(season_number), [int(ep.id) for ep in ring]
            )
            episode_rank = {
                int(episode_id): index for index, episode_id in enumerate(episode_order)
            }
            selected = min(
                season_rows,
                key=lambda item: (
                    -1.0
                    if last_times.get(int(item.id)) is None
                    else float(last_times[int(item.id)]),
                    episode_rank.get(int(item.id), len(episode_rank)),
                    int(item.id),
                ),
            )
            last = last_times.get(int(selected.id))
            # Never-searched episodes are oldest of all.
            age_key = -1.0 if last is None else float(last)
            season_probes.append((age_key, season_rank, selected))

        if not season_probes:
            continue
        season_probes.sort(key=lambda row: (row[0], row[1], int(row[2].id)))
        _, _, probe = season_probes[0]
        last = last_times.get(int(probe.id))
        age_key = -1.0 if last is None else float(last)
        series_probes.append((age_key, series_rank.get(int(series_id), 0), probe))

    if not series_probes:
        return None
    series_probes.sort(key=lambda row: (row[0], row[1], int(row[2].id)))
    return series_probes[0][2]


async def auto_cycle(cycle_source: str = "automatic", max_episode_targets: int | None = None) -> bool:
    """Run one priority-based automatic search/download pass.

    The scheduler and manual Run-auto-cycle route call this with
    ``max_episode_targets=1`` so each user-visible automatic search window
    contains exactly one logical episode target (plus its enabled alternate
    query spellings). Normal priority is global rather than per-show, with one
    explicit exception:

    0. A TV show just toggled from disabled -> enabled gets one next-cycle turn.
    1. Missing episodes.
    2. Installed episodes Sonarr reports below quality-profile cutoff.
    3. Installed episodes weaker than peers in the same season by known CF score,
       resolution, or source quality.
    4. Exploratory upgrades for everything else.

    After the one-time enable-priority search runs, normal global scheduling
    resumes. Within a normal priority pool, the oldest search wins and the persisted show/season/
    episode round-robin is used as a fairness tie breaker. A season is probed at
    most once per cycle, because same-peer season-folder expansion can turn one
    Soulseek query into several useful downloads. Automatic searches try all
    enabled query spellings in the same turn before adaptive backoff begins.
    The next episode target is deferred to the next scheduler window.
    """
    global _AUTO_CYCLE_RUNNING

    if _AUTO_CYCLE_RUNNING:
        return False
    _AUTO_CYCLE_RUNNING = True
    # Snapshot newly-enabled show priority at the instant the cycle starts. A
    # toggle that happens while this cycle is already running is deliberately
    # left for the next cycle rather than joining a partially built queue.
    cycle_started_at = time.time()
    cycle_source = "manual" if str(cycle_source or "").strip().lower() == "manual" else "automatic"
    searches_run = 0
    episode_targets_run = 0

    def record_cycle_notice(message: str, *, status: str = "skipped") -> None:
        try:
            jobs.record_search_activity(
                cycle_source=cycle_source,
                status=status,
                message=message,
                search_target_type="cycle",
            )
        except Exception:
            pass

    try:
        settings = settings_store.load()
        if not settings.auto_search_enabled:
            record_cycle_notice("No Soulseek search was run because Automatic search is disabled.")
            return True
        if not settings.auto_download:
            record_cycle_notice("No Soulseek search was run because Automatic downloads is disabled.")
            return True
        if not configured(settings):
            record_cycle_notice("No Soulseek search was run because Sonarr/Radarr or slskd is not fully configured.")
            return True
        if _auto_search_download_limit_reached(settings):
            record_cycle_notice(_auto_search_limit_notice(settings))
            return True

        sonarr = media_client(settings, SonarrClient)
        series_rows = await _sync_series_preferences_from_sonarr(sonarr, settings)

        # A TV show explicitly toggled from disabled -> enabled gets one
        # next-cycle priority turn. Load only those shows up front so stale
        # per-episode adaptive backoff can be reset before normal due lists are
        # calculated. The flag survives Sonarr/API failures and external queue
        # blocks until a real Soulseek search attempt actually runs.
        priority_snapshot = search_state.pending_series_enable_priorities(
            before_ts=cycle_started_at
        )
        series_by_id: dict[int, dict[str, Any]] = {}
        for row in series_rows:
            if not isinstance(row, dict):
                continue
            try:
                series_by_id[int(row.get("id", row.get("Id")))] = row
            except (TypeError, ValueError):
                continue
        series_overrides = jobs.series_download_overrides()
        priority_series: list[tuple[int, bool]] = []
        priority_catalogs: dict[int, list[MissingEpisode]] = {}
        priority_catalog_errors: dict[int, list[str]] = {}
        for series_id, cooldowns_reset in priority_snapshot:
            row = series_by_id.get(int(series_id))
            if row is None:
                # Keep the priority flag. An empty/partial series response may
                # be a transient Sonarr API failure; losing the user's explicit
                # enable priority would be worse than retaining a stale row for
                # a show that was actually removed.
                continue
            monitored = bool(row.get("monitored", row.get("Monitored", False)))
            if not _series_download_enabled(series_id, monitored, series_overrides):
                # It was disabled again before the next cycle.
                search_state.clear_series_enable_priority(series_id)
                continue
            priority_series.append((int(series_id), bool(cooldowns_reset)))
            catalog, errors = await _all_enabled_episode_catalog(
                sonarr, [row], max_concurrency=1
            )
            if errors:
                priority_catalog_errors[int(series_id)] = list(errors)
                continue
            priority_catalogs[int(series_id)] = list(catalog)
            if not cooldowns_reset:
                search_state.reset_adaptive_cooldowns([int(ep.id) for ep in catalog])
                search_state.mark_series_enable_cooldowns_reset(series_id)

        wanted_missing = await sonarr.missing_episodes(False)
        missing_all = await _missing_catalog_with_specials(sonarr, series_rows, wanted_missing)
        missing_all = _filter_download_enabled_episodes(missing_all)
        missing_searchable = [
            ep for ep in missing_all
            if episode_search_eligible(ep, delay_hours=settings.missing_episode_delay_hours)
        ]
        missing_not_yet_searchable = max(0, len(missing_all) - len(missing_searchable))
        missing_due = [
            ep for ep in missing_searchable
            if search_state.search_due(ep.id, settings.repeat_search_cooldown_hours)
        ]
        missing_on_cooldown = max(0, len(missing_searchable) - len(missing_due))

        # Upgrade data is intentionally lazy. As long as global Missing work is
        # due, no full-library installed-episode scan is needed.
        upgrade_loaded = False
        upgrade_tiers: dict[str, list[MissingEpisode]] = {
            "upgrade-cutoff": [],
            "upgrade-weak": [],
            "upgrade-quality": [],
            "upgrade-exploratory": [],
        }
        upgrade_all_tiers: dict[str, list[MissingEpisode]] = {key: [] for key in upgrade_tiers}
        upgrade_existing_seen = 0
        upgrade_due_seen = 0
        upgrade_on_cooldown = 0
        upgrade_series_errors = 0
        cutoff_signal_error = False
        external_queue_blocks = 0
        cutoff_ids_cache: set[int] | None = None
        priority_completed_series: set[int] = set()
        priority_unavailable_series: set[int] = set()
        priority_excluded_episode_ids: set[int] = set()

        async def load_cutoff_ids() -> set[int]:
            nonlocal cutoff_ids_cache, cutoff_signal_error
            if cutoff_ids_cache is not None:
                return cutoff_ids_cache
            cutoff_ids_cache = set()
            cutoff_method = getattr(sonarr, "cutoff_unmet_episode_ids", None)
            if callable(cutoff_method):
                try:
                    result = cutoff_method()
                    if inspect.isawaitable(result):
                        result = await result
                    if isinstance(result, (set, list, tuple)):
                        cutoff_ids_cache = {int(value) for value in result}
                except Exception:
                    # Sonarr versions without /wanted/cutoff still get weak and
                    # exploratory upgrade scheduling.
                    cutoff_signal_error = True
            return cutoff_ids_cache

        async def load_upgrade_tiers() -> None:
            nonlocal upgrade_loaded, upgrade_existing_seen, upgrade_due_seen
            nonlocal upgrade_on_cooldown, upgrade_series_errors
            if upgrade_loaded:
                return
            upgrade_loaded = True

            catalog, errors = await _all_enabled_episode_catalog(sonarr, series_rows)
            upgrade_series_errors = len(errors)
            installed = [ep for ep in catalog if ep.has_file]
            upgrade_existing_seen = len(installed)

            cutoff_ids = await load_cutoff_ids()
            classified = upgrade_priority_tiers(installed, cutoff_ids)
            for tier_name, rows in classified.items():
                upgrade_all_tiers[tier_name] = [
                    ep for ep in rows if int(ep.id) not in priority_excluded_episode_ids
                ]
                for ep in upgrade_all_tiers[tier_name]:
                    if search_state.search_due(ep.id, settings.repeat_search_cooldown_hours):
                        upgrade_tiers[tier_name].append(ep)
                        upgrade_due_seen += 1
                    else:
                        upgrade_on_cooldown += 1

        async def select_newly_enabled_priority(
            covered_seasons: set[tuple[int, int]],
        ) -> tuple[str, MissingEpisode, int] | None:
            """Return the next one-time priority probe for a newly enabled show.

            Missing episodes win inside that show. If none are currently eligible,
            choose the show's best installed upgrade tier. Old adaptive cooldowns
            have already been reset, so the explicit enable action is not delayed
            by search state from before the show was disabled.
            """
            for series_id, _reset_done in priority_series:
                series_id = int(series_id)
                if series_id in priority_completed_series or series_id in priority_unavailable_series:
                    continue
                catalog = priority_catalogs.get(series_id)
                if catalog is None:
                    # Keep the persisted priority flag for a later cycle if the
                    # show catalog could not be loaded this time.
                    priority_unavailable_series.add(series_id)
                    continue

                missing_rows = [
                    ep for ep in missing_searchable
                    if int(ep.series_id) == series_id
                    and int(ep.id) not in priority_excluded_episode_ids
                ]
                if missing_rows:
                    candidate = _select_priority_probe(
                        missing_rows, settings, covered_seasons, universe=missing_rows
                    )
                    if candidate is not None:
                        return "missing", candidate, series_id

                installed = [
                    ep for ep in catalog
                    if ep.has_file and int(ep.id) not in priority_excluded_episode_ids
                ]
                if installed:
                    classified = upgrade_priority_tiers(installed, await load_cutoff_ids())
                    for tier_name in (
                        "upgrade-cutoff", "upgrade-weak", "upgrade-quality", "upgrade-exploratory"
                    ):
                        rows = classified.get(tier_name, [])
                        if not rows:
                            continue
                        candidate = _select_priority_probe(
                            list(rows), settings, covered_seasons, universe=list(rows)
                        )
                        if candidate is not None:
                            return tier_name, candidate, series_id

                # No currently eligible missing episode or installed upgrade.
                # Keep the flag so a future episode/air-date can consume it.
                priority_unavailable_series.add(series_id)
            return None

        def remove_target(pool: list[MissingEpisode], episode_id: int) -> None:
            pool[:] = [ep for ep in pool if int(ep.id) != int(episode_id)]

        covered_seasons: set[tuple[int, int]] = set()
        queued_total = 0

        while True:
            # User-visible automatic cycles pass max_episode_targets=1, so
            # alternate query spellings stay inside this bundle while the next
            # episode waits for another search window. Internal/tests may leave
            # this unset when exercising multi-target priority rotation.
            if max_episode_targets is not None and episode_targets_run >= max(0, int(max_episode_targets)):
                break
            if queued_total >= settings.max_auto_per_cycle:
                break
            if _auto_search_download_limit_reached(settings):
                break

            priority_series_id: int | None = None
            priority_pick = await select_newly_enabled_priority(covered_seasons)
            if priority_pick is not None:
                target_tier, ep, priority_series_id = priority_pick
                # Missing rows are also part of the normal global pool. Remove
                # this concrete probe there so an external Sonarr queue block
                # cannot make the same episode spin twice in one cycle.
                active_pool = missing_due if target_tier == "missing" else []
            else:
                target_tier = "missing"
                ep = _select_priority_probe(
                    missing_due, settings, covered_seasons, universe=missing_searchable
                )
                active_pool = missing_due

                if ep is None:
                    await load_upgrade_tiers()
                    active_pool = []
                    for tier_name in (
                        "upgrade-cutoff", "upgrade-weak", "upgrade-quality", "upgrade-exploratory"
                    ):
                        candidate = _select_priority_probe(
                            upgrade_tiers[tier_name], settings, covered_seasons,
                            universe=upgrade_all_tiers[tier_name],
                        )
                        if candidate is None:
                            continue
                        target_tier = tier_name
                        ep = candidate
                        active_pool = upgrade_tiers[tier_name]
                        break

            if ep is None:
                break

            remove_target(active_pool, ep.id)
            if await _episode_blocked_by_external_sonarr_queue(sonarr, ep.id):
                external_queue_blocks += 1
                if priority_series_id is not None:
                    # No real Soulseek search happened, so keep the show's
                    # one-time flag for the next cycle but avoid retrying this
                    # exact blocked episode again during the current cycle.
                    priority_excluded_episode_ids.add(int(ep.id))
                continue

            series_id = int(ep.series_id)
            season_number = int(ep.season_number)
            season_key = (series_id, season_number)
            search_state.record_auto_series_search(series_id)
            search_state.record_auto_season_search(series_id, season_number)
            search_state.record_auto_episode_search(series_id, season_number, ep.id)

            slskd = SlskdClient(
                settings.slskd_url,
                settings.slskd_api_key,
                timeout=settings.search_timeout_seconds + 10,
            )
            attempt = await _automatic_search_candidates(
                slskd, sonarr, ep, settings, pace=True
            )
            episode_targets_run += 1
            searches_run += max(1, len(attempt["attempted_queries"]))
            covered_seasons.add(season_key)
            if priority_series_id is not None:
                # The explicit enable request has now received its promised real
                # Soulseek search turn, regardless of whether it found/queued a
                # release. Subsequent work returns to the normal global scheduler.
                search_state.clear_series_enable_priority(priority_series_id)
                priority_completed_series.add(int(priority_series_id))

            ranked = list(attempt["ranked"])
            raw_result_count = int(attempt["raw_result_count"])
            usable_count = int(attempt["usable_count"])
            search_errors = list(attempt["errors"])
            search_query = str(attempt["query"])
            queueable = _queueable_peer_candidates(ranked)
            audit_target_type = "missing" if target_tier == "missing" else "upgrade"

            if not queueable:
                if search_errors:
                    outcome_code = "error"
                    outcome = "Search failed: " + "; ".join(str(item) for item in search_errors)
                    activity_status = "failed"
                elif usable_count > 0:
                    outcome_code = "not-queueable"
                    outcome = (
                        f"Found {usable_count} usable candidate(s), but none are queueable right now "
                        "(for example peer cooldown/limit)."
                    )
                    activity_status = "searched"
                elif raw_result_count > 0:
                    outcome_code = "no-usable"
                    outcome = (
                        f"Found {raw_result_count} result(s), but none passed "
                        "quality/score/repeat-download eligibility."
                    )
                    activity_status = "searched"
                else:
                    outcome_code = "no-results"
                    outcome = "Search returned no results."
                    activity_status = "searched"
                delay = _adaptive_outcome_delay(ep, target_tier, outcome_code, settings)
                if settings.repeat_search_cooldown_hours > 0:
                    outcome += f" Next automatic attempt in about {delay:g} hour(s)."
                try:
                    jobs.record_search_activity(
                        cycle_source=cycle_source,
                        status=activity_status,
                        episode_id=ep.id,
                        episode_code=ep.episode_code,
                        episode_title=f"{ep.series_title} · {ep.title}",
                        search_target_type=audit_target_type,
                        search_query=search_query,
                        result_count=raw_result_count,
                        usable_count=usable_count,
                        queued_count=0,
                        message=outcome,
                    )
                except Exception:
                    pass
                continue

            if await _episode_blocked_by_external_sonarr_queue(sonarr, ep.id):
                delay = _adaptive_outcome_delay(ep, target_tier, "external-queue", settings)
                message = (
                    "Usable candidate(s) were found, but Sonarr now has an external queue item "
                    "for this episode, so nothing was queued."
                )
                if settings.repeat_search_cooldown_hours > 0:
                    message += f" Next automatic attempt in about {delay:g} hour(s)."
                try:
                    jobs.record_search_activity(
                        cycle_source=cycle_source,
                        status="skipped",
                        episode_id=ep.id,
                        episode_code=ep.episode_code,
                        episode_title=f"{ep.series_title} · {ep.title}",
                        search_target_type=audit_target_type,
                        search_query=search_query,
                        result_count=raw_result_count,
                        usable_count=usable_count,
                        queued_count=0,
                        message=message,
                    )
                except Exception:
                    pass
                continue

            best = queueable[0]
            created_jobs, folder_warning = await queue_candidate_with_season_folder(
                ep, best, settings, sonarr, slskd
            )
            queued_count = len(created_jobs)
            queued_total += queued_count

            if queued_count > 0:
                delay = _adaptive_outcome_delay(ep, target_tier, "queued", settings)
                outcome = f"Queued {queued_count} download(s) from the selected peer/season folder."
                if folder_warning:
                    outcome += f" Folder expansion warning: {folder_warning}"
                activity_status = "searched"

                # Folder expansion can satisfy several episodes from one query.
                # Give every queued sibling a normal adaptive pause as well, so
                # the next cycle does not immediately spend another search on it.
                queued_ids = {int(getattr(job, "episode_id", -1)) for job in created_jobs}
                catalog_by_id = {int(item.id): item for item in missing_searchable}
                if upgrade_loaded:
                    for rows in upgrade_tiers.values():
                        for item in rows:
                            catalog_by_id[int(item.id)] = item
                for sibling_id in queued_ids:
                    if sibling_id == int(ep.id):
                        continue
                    sibling = catalog_by_id.get(sibling_id)
                    sibling_target = "missing" if sibling is not None and not sibling.has_file else "upgrade-weak"
                    sibling_streak = search_state.outcome_streak(sibling_id, sibling_target, "queued") + 1
                    sibling_delay = adaptive_search_delay_hours(
                        sibling_target,
                        "queued",
                        sibling_streak,
                        settings.repeat_search_cooldown_hours,
                    )
                    search_state.record_search_outcome(
                        sibling_id, sibling_target, "queued", sibling_delay
                    )
            else:
                delay = _adaptive_outcome_delay(ep, target_tier, "not-queueable", settings)
                outcome = folder_warning or "A candidate was selected, but no download was queued."
                if settings.repeat_search_cooldown_hours > 0:
                    outcome += f" Next automatic attempt in about {delay:g} hour(s)."
                activity_status = "skipped"

            try:
                jobs.record_search_activity(
                    cycle_source=cycle_source,
                    status=activity_status,
                    episode_id=ep.id,
                    episode_code=ep.episode_code,
                    episode_title=f"{ep.series_title} · {ep.title}",
                    search_target_type=audit_target_type,
                    search_query=search_query,
                    result_count=raw_result_count,
                    usable_count=usable_count,
                    queued_count=queued_count,
                    message=outcome,
                )
            except Exception:
                pass

        if searches_run == 0:
            diagnostic_parts = [
                (
                    f"Missing: {len(missing_due)} due, {missing_on_cooldown} on adaptive/repeat cooldown, "
                    f"{missing_not_yet_searchable} not yet past the "
                    f"{settings.missing_episode_delay_hours}-hour missing-episode delay."
                )
            ]
            if upgrade_loaded:
                diagnostic_parts.append(
                    f"Upgrades: {upgrade_due_seen} due from {upgrade_existing_seen} installed episode(s), "
                    f"{upgrade_on_cooldown} on adaptive/repeat cooldown."
                )
            if external_queue_blocks:
                diagnostic_parts.append(f"Sonarr queue blocked {external_queue_blocks} target(s).")
            if upgrade_series_errors:
                diagnostic_parts.append(
                    f"Sonarr upgrade metadata could not be loaded for {upgrade_series_errors} series."
                )
            if cutoff_signal_error:
                diagnostic_parts.append(
                    "Sonarr cutoff-unmet priority data was unavailable; weak/exploratory upgrade priority was used."
                )
            diagnostic_parts.append(
                f"Base repeat-search cooldown is {settings.repeat_search_cooldown_hours:g} hour(s); "
                "adaptive backoff may choose a shorter or longer delay by outcome."
            )
            record_cycle_notice("No Soulseek search was run. " + " ".join(diagnostic_parts))
        return True
    except Exception as exc:
        if searches_run == 0:
            record_cycle_notice(
                f"Cycle failed before a Soulseek search completed: {type(exc).__name__}: {exc}",
                status="failed",
            )
        else:
            record_cycle_notice(
                f"Cycle stopped after {searches_run} Soulseek search(es): {type(exc).__name__}: {exc}",
                status="failed",
            )
        raise
    finally:
        _AUTO_CYCLE_RUNNING = False


async def _run_scheduled_auto_search_window() -> bool:
    """Run one paced automatic episode window and timestamp its completion."""
    global _AUTO_SCHEDULER_LAST_RUN_AT
    try:
        return await auto_cycle(max_episode_targets=1)
    finally:
        # Completion-to-next-start spacing: query execution and any alternate
        # query gap never consume the configured inter-episode wait.
        _AUTO_SCHEDULER_LAST_RUN_AT = asyncio.get_running_loop().time()


async def scheduler_loop() -> None:
    global _AUTO_SCHEDULER_LAST_RUN_AT
    last_cleanup = 0.0
    last_import_cleanup = float("-inf")
    last_history_cleanup = 0.0
    last_transfer_cleanup = 0.0
    last_queue_maintenance = 0.0
    last_series_sync = 0.0
    last_transfer_reconcile = 0.0
    last_sonarr_supersede_reconcile = 0.0
    while True:
        try:
            settings = settings_store.load()
            interval = max(60, settings.scan_interval_minutes * 60)
            now = asyncio.get_running_loop().time()
            if now - last_import_cleanup >= 60 * 60:
                protected_import_paths = []
                for job in jobs.active():
                    protected_import_paths.append(IMPORT_DIR / f"soulseekarr-{job.id}")
                    for path in (job.source_file, job.import_file):
                        if path:
                            protected_import_paths.append(Path(path))
                await asyncio.to_thread(cleanup_import_directory, IMPORT_DIR, protected_import_paths)
                last_import_cleanup = now
            if configured(settings) and now - last_transfer_reconcile >= 30:
                last_transfer_reconcile = now
                await runner.reconcile_slskd_downloads()
            if configured(settings) and now - last_sonarr_supersede_reconcile >= 60:
                last_sonarr_supersede_reconcile = now
                await runner.reconcile_sonarr_superseded_downloads()
            if now - last_cleanup >= 60:
                last_cleanup = now
                await cleanup_search_history(settings)
            if now - last_history_cleanup >= 60:
                last_history_cleanup = now
                cleanup_database_history(settings)
            if now - last_transfer_cleanup >= 60:
                last_transfer_cleanup = now
                await cleanup_slskd_download_history(settings)
            if now - last_queue_maintenance >= 60:
                last_queue_maintenance = now
                await maintain_queued_download_health(settings)
            if configured(settings) and now - last_series_sync >= 300:
                last_series_sync = now
                await _sync_series_preferences_from_sonarr(
                    media_client(settings, SonarrClient), settings
                )
            if settings.auto_search_enabled and settings.auto_download and now - _AUTO_SCHEDULER_LAST_RUN_AT >= interval:
                # Space primary episode-search bundles from completion, not
                # from their start. A bundle may include one fallback query,
                # and its execution time must not eat into the configured gap
                # before the next episode is searched.
                await _run_scheduled_auto_search_window()
        except asyncio.CancelledError:
            raise
        except Exception:
            # Per-job errors are recorded separately; scheduler failures retry next interval.
            pass
        # Keep the short episode-search interval responsive. The guarded
        # maintenance jobs above still run only on their own 30/60/300-second
        # cadences; this only reduces scheduler timing jitter.
        await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Planned names are persisted on queued jobs. Repair names created by older
    # builds before Sonarr supersede reconciliation uses their cached profile.
    try:
        runner.repair_active_planned_filenames()
    except Exception:
        pass

    # Reconcile before normal recovery so an upgrade from an older build can
    # immediately revive jobs that were incorrectly terminalized during an
    # slskd restart. If slskd is still starting, ordinary recovery keeps active
    # jobs alive and the scheduler retries reconciliation every 30 seconds.
    try:
        await runner.reconcile_slskd_downloads()
    except Exception:
        pass
    try:
        await runner.reconcile_sonarr_superseded_downloads()
    except Exception:
        pass
    await runner.recover()
    # Recovery may revive legacy peer-limit failures into active retry jobs.
    # Prune only after that migration so pending work is never mistaken for
    # disposable history.
    try:
        cleanup_database_history(settings_store.load())
    except Exception:
        pass
    scheduler = asyncio.create_task(scheduler_loop())
    download_snapshots = asyncio.create_task(download_snapshot_loop())
    series_snapshots = asyncio.create_task(series_snapshot_loop())
    try:
        yield
    finally:
        scheduler.cancel()
        download_snapshots.cancel()
        series_snapshots.cancel()
        for task in runner.tasks.values():
            task.cancel()
        for task in list(_TARGETED_SEARCH_TASKS.values()):
            task.cancel()
        if _MANUAL_CYCLE_TASK is not None:
            _MANUAL_CYCLE_TASK.cancel()


app = FastAPI(title="Soulseekarr", version=APP_VERSION, lifespan=lifespan)


def _same_host_request_origin(request: Request, value: str) -> bool:
    """Return True only when Origin/Referer targets the request Host.

    Comparing Host rather than ASGI scheme keeps this compatible with normal
    HTTPS reverse proxies that preserve the public Host while terminating TLS.
    """
    try:
        parsed = urlsplit(str(value or ""))
    except ValueError:
        return False
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    request_host = str(request.headers.get("host") or "").strip().casefold()
    return bool(request_host) and parsed.netloc.casefold() == request_host


def _browser_csrf_token(request: Request, body: bytes) -> str:
    """Return the synchronizer token supplied by Soulseekarr's own UI.

    Browser JavaScript sends the token in a custom header. The Settings page
    uses a normal HTML form, so it sends the same token as a hidden urlencoded
    field. Server-to-server clients do not need or receive this token.
    """
    header_token = str(request.headers.get("x-soulseekarr-csrf") or "").strip()
    if header_token:
        return header_token

    content_type = str(request.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
    if content_type != "application/x-www-form-urlencoded" or not body:
        return ""
    try:
        fields = parse_qs(body.decode("utf-8"), keep_blank_values=True, max_num_fields=512)
    except (UnicodeDecodeError, ValueError):
        return ""
    values = fields.get("_csrf_token") or []
    return str(values[0]).strip() if values else ""


def _valid_browser_csrf_token(request: Request, body: bytes) -> bool:
    token = _browser_csrf_token(request, body)
    return bool(token) and len(token) <= 128 and secrets.compare_digest(token, _BROWSER_CSRF_TOKEN)


@app.middleware("http")
async def security_headers_and_request_guards(request: Request, call_next):
    """Apply browser hardening, CSRF-origin checks and a coarse body-size guard."""
    request.state.csp_nonce = secrets.token_urlsafe(18)

    if request.method.upper() in {"POST", "PUT", "PATCH", "DELETE"}:
        max_body = 2 * 1024 * 1024
        raw_length = request.headers.get("content-length")
        if raw_length:
            try:
                if int(raw_length) > max_body:
                    return JSONResponse({"detail": "Request body is too large."}, status_code=413)
            except ValueError:
                return JSONResponse({"detail": "Invalid Content-Length header."}, status_code=400)

        # Enforce the limit even for chunked/HTTP2 requests without Content-Length.
        # Starlette caches request.body(), so form/JSON parsing downstream still
        # sees exactly the same bytes after this guard.
        try:
            request_body = await request.body()
            if len(request_body) > max_body:
                return JSONResponse({"detail": "Request body is too large."}, status_code=413)
        except Exception:
            return JSONResponse({"detail": "Could not read request body."}, status_code=400)

        # Browser state-changing requests must come from Soulseekarr's own UI.
        # Fetch Metadata remains a useful first-line signal, but not every browser
        # sends Sec-Fetch-Site and reverse proxies can rewrite Host/port. To avoid
        # rejecting legitimate Settings saves in those cases, every rendered UI
        # also carries an unguessable per-process synchronizer token. Native forms
        # submit it as a hidden field and same-origin fetch() calls add it as a
        # custom header. Cross-site requests are still rejected unconditionally.
        # Older/stale pages without the token retain the strict Origin/Referer
        # fallback, while server-to-server clients such as Sonarr normally send
        # none of these browser headers and remain compatible.
        fetch_site = str(request.headers.get("sec-fetch-site") or "").strip().lower()
        if fetch_site == "cross-site":
            return JSONResponse({"detail": "Cross-site state-changing request rejected."}, status_code=403)

        if not _valid_browser_csrf_token(request, request_body) and fetch_site != "same-origin":
            origin = request.headers.get("origin")
            referer = request.headers.get("referer")
            if origin and not _same_host_request_origin(request, origin):
                return JSONResponse({"detail": "Cross-origin state-changing request rejected."}, status_code=403)
            if not origin and referer and not _same_host_request_origin(request, referer):
                return JSONResponse({"detail": "Cross-origin state-changing request rejected."}, status_code=403)

    response = await call_next(request)
    content_type = response.headers.get("content-type", "").lower()
    if content_type.startswith("text/html"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"

    nonce = request.state.csp_nonce
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        f"script-src 'self' 'nonce-{nonce}' https://cdn.jsdelivr.net; "
        "style-src 'self' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com; "
        "style-src-attr 'unsafe-inline'; "
        "font-src 'self' https://cdnjs.cloudflare.com data:; "
        "img-src 'self' data:; connect-src 'self'; object-src 'none'; "
        "base-uri 'self'; frame-ancestors 'none'; form-action 'self'"
    )
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
    response.headers["Cross-Origin-Resource-Policy"] = "same-origin"
    response.headers["X-Permitted-Cross-Domain-Policies"] = "none"
    return response


app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")
templates.env.globals["app_version"] = APP_VERSION
templates.env.globals["csrf_token"] = _BROWSER_CSRF_TOKEN
templates.env.globals["notification_latest_id"] = lambda: jobs.ui_notifications_after(0, 1)[1]


def configured(settings: Settings) -> bool:
    return bool(media_configured(settings) and settings.slskd_url and settings.slskd_api_key)



def _transfer_state(transfer: dict[str, Any] | None) -> str:
    if not transfer:
        return ""
    status_obj = transfer.get("status") if isinstance(transfer.get("status"), dict) else {}
    return str(transfer.get("state") or transfer.get("State") or status_obj.get("state") or "")


def _number(transfer: dict[str, Any] | None, *names: str, default: float = 0.0) -> float:
    if not transfer:
        return default
    for name in names:
        value = transfer.get(name)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return default


def _preparation_details(job: Any) -> tuple[str | None, int | None]:
    if str(getattr(job, "status", "")) != "copying":
        return None, None

    message = str(getattr(job, "message", "") or "").strip()
    step = message
    if step.lower().startswith("preparing:"):
        step = step.split(":", 1)[1].strip()
    step = step.rstrip(".") or "Preparing file"

    match = re.search(r"\((\d{1,3})%\)", step)
    progress = None
    if match:
        progress = max(0, min(100, int(match.group(1))))
        step = re.sub(r"\s*\(\d{1,3}%\)\s*$", "", step).rstrip(" .")

    return step, progress


def _active_download_row(job: Any, transfer: dict[str, Any] | None) -> dict[str, Any]:
    size = int(_number(transfer, "size", "Size", default=float(job.size))) or int(job.size)
    transferred = int(_number(transfer, "bytesTransferred", "BytesTransferred", default=0.0))
    speed = _number(transfer, "averageSpeed", "AverageSpeed", default=0.0)
    place_raw = None
    if transfer:
        place_raw = transfer.get("placeInQueue", transfer.get("PlaceInQueue"))
    try:
        place = int(place_raw) if place_raw is not None else None
    except (TypeError, ValueError):
        place = None

    if size > 0:
        percent = max(0.0, min(100.0, transferred / size * 100.0))
    else:
        percent = 0.0

    # Once slskd has finished, Soulseekarr may be copying/renaming or waiting on
    # Sonarr. Those are still active jobs, but the network transfer is complete.
    if job.status in {"copying", "importing"}:
        transferred = size
        percent = 100.0

    remaining_seconds = None
    if speed > 0 and size > transferred:
        remaining_seconds = max(0, int((size - transferred) / speed))

    state = _transfer_state(transfer)
    started_at = None
    if transfer:
        started_at = transfer.get("startedAt", transfer.get("StartedAt"))
    is_queued = (
        job.status == "queued"
        or (job.status == "downloading" and transferred <= 0 and not started_at)
        or "queue" in state.lower()
    )
    quality_source = job.import_file or job.remote_filename
    quality = extract_quality(quality_source)
    preparation_step, preparation_progress = _preparation_details(job)
    waiting_for_peer_limit = (
        job.status == "queued"
        and str(job.message or "").casefold().startswith("waiting to retry: remote peer limit")
    )

    live_transfer_id = None
    requested_at = enqueued_at = transfer_started_at = ended_at = transfer_exception = None
    attempts = None
    bytes_remaining = max(0, size - transferred)
    if transfer:
        live_transfer_id = transfer.get("id", transfer.get("Id"))
        requested_at = transfer.get("requestedAt", transfer.get("RequestedAt"))
        enqueued_at = transfer.get("enqueuedAt", transfer.get("EnqueuedAt"))
        transfer_started_at = transfer.get("startedAt", transfer.get("StartedAt"))
        ended_at = transfer.get("endedAt", transfer.get("EndedAt"))
        transfer_exception = transfer.get("exception", transfer.get("Exception"))
        attempts_raw = transfer.get("attempts", transfer.get("Attempts"))
        try:
            attempts = int(attempts_raw) if attempts_raw is not None else None
        except (TypeError, ValueError):
            attempts = None
        remaining_raw = transfer.get("bytesRemaining", transfer.get("BytesRemaining"))
        try:
            if remaining_raw is not None:
                bytes_remaining = max(0, int(remaining_raw))
        except (TypeError, ValueError):
            pass

    return {
        **asdict(job),
        "quality": quality,
        "transferFound": transfer is not None,
        "transferState": state,
        "bytesTransferred": transferred,
        "progressPercent": round(percent, 1),
        "averageSpeed": speed,
        "speedMbits": round(speed * 8.0 / 1_000_000.0, 2),
        "placeInQueue": place,
        "queueStrikeLimit": QUEUE_STRIKE_LIMIT,
        "remainingSeconds": remaining_seconds,
        "isQueued": is_queued,
        "waitingForPeerLimit": waiting_for_peer_limit,
        "preparationStep": preparation_step,
        "preparationProgress": preparation_progress,
        "liveTransferId": live_transfer_id,
        "requestedAt": requested_at,
        "enqueuedAt": enqueued_at,
        "startedAt": transfer_started_at,
        "endedAt": ended_at,
        "attempts": attempts,
        "transferException": transfer_exception,
        "bytesRemaining": bytes_remaining,
    }


def _sort_active_download_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Active work first, then queued work by remote queue place, peer-limit waits last."""
    def sort_key(row: dict[str, Any]) -> tuple[float, float, int]:
        if row.get("waitingForPeerLimit"):
            # Peer-limit cooldowns are not active remote queue positions and stay
            # below all ordinary queued work.
            bucket = 3.0
            secondary = float("inf")
        elif row.get("isQueued"):
            place_raw = row.get("placeInQueue")
            try:
                place = int(place_raw) if place_raw is not None else None
            except (TypeError, ValueError):
                place = None

            if place is not None and place > 0:
                # Smaller remote queue positions are closer to starting.
                bucket = 1.0
                secondary = float(place)
            else:
                # Unknown queue positions follow every known-position queue.
                bucket = 2.0
                secondary = float("inf")
        else:
            bucket = 0.0
            secondary = -float(row.get("progressPercent") or 0.0)
        return bucket, secondary, int(row.get("id") or 0)

    return sorted(rows, key=sort_key)


def saved_active_download_rows() -> tuple[list[dict[str, Any]], str | None]:
    """Build Active Downloads entirely from SQLite's last background snapshot.

    Neither this helper nor any Downloads/qBittorrent HTTP endpoint contacts
    slskd. The independent background worker owns all live polling and stores
    the latest successful transfer snapshot in SQLite.
    """
    snapshots, snapshot_state = jobs.active_transfer_snapshots()
    rows: list[dict[str, Any]] = []
    seen_transfer_keys: set[tuple[str, str, int]] = set()
    for job in jobs.active():
        job_transfer_key = remote_transfer_key(job.username, job.remote_filename, int(job.size))
        if job_transfer_key in seen_transfer_keys:
            continue
        seen_transfer_keys.add(job_transfer_key)

        has_snapshot = int(job.id) in snapshots
        transfer = snapshots.get(int(job.id)) if has_snapshot else None
        row = _active_download_row(job, transfer)
        row["livePending"] = not has_snapshot
        row["liveRefreshedAt"] = snapshot_state.get("refreshedAt")
        rows.append(row)

    return _sort_active_download_rows(rows), snapshot_state.get("error")


def local_active_download_rows() -> list[dict[str, Any]]:
    """Compatibility helper for fast local rendering; never performs network I/O."""
    rows, _error = saved_active_download_rows()
    return rows


async def active_download_rows(*, include_queue_positions: bool = True) -> tuple[list[dict[str, Any]], str | None]:
    """Return the last saved background transfer snapshot.

    ``include_queue_positions`` is retained for qBittorrent/test compatibility;
    queue positions are refreshed only by the background worker.
    """
    return saved_active_download_rows()


async def refresh_active_download_snapshot(*, include_queue_positions: bool = True) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch slskd live state in the background and persist one atomic snapshot."""
    active_jobs = jobs.active()
    if not active_jobs:
        jobs.save_active_transfer_snapshots({})
        return [], None

    settings = settings_store.load()
    if not (settings.slskd_url and settings.slskd_api_key):
        error = "slskd is not configured."
        jobs.record_active_transfer_snapshot_error(error)
        return saved_active_download_rows()[0], error

    client = SlskdClient(settings.slskd_url, settings.slskd_api_key, timeout=15)
    try:
        payload = await client.all_downloads()
    except Exception as exc:
        error = _upstream_error_message(exc)
        jobs.record_active_transfer_snapshot_error(error)
        return saved_active_download_rows()[0], error

    transfers: dict[tuple[str, str, int], dict[str, Any]] = {}
    transfers_by_id: dict[tuple[str, str], dict[str, Any]] = {}
    fallback_transfers: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for transfer in client.flatten_downloads(payload):
        transfer_username = str(transfer.get("username") or transfer.get("Username") or "")
        transfer_filename = str(transfer.get("filename") or transfer.get("Filename") or "")
        transfer_size = int(_number(transfer, "size", "Size", default=0.0))
        key = remote_transfer_key(transfer_username, transfer_filename, transfer_size)
        transfers[key] = transfer
        fallback_key = remote_transfer_fallback_key(
            transfer_username, transfer_filename, transfer_size
        )
        fallback_transfers.setdefault(fallback_key, []).append(transfer)
        transfer_id = str(transfer.get("id") or transfer.get("Id") or "").strip()
        if transfer_id:
            transfers_by_id[(transfer_username.casefold(), transfer_id)] = transfer

    snapshots: dict[int, dict[str, Any] | None] = {}
    unique_jobs: list[Any] = []
    seen_transfer_keys: set[tuple[str, str, int]] = set()
    for job in active_jobs:
        job_transfer_key = remote_transfer_key(job.username, job.remote_filename, int(job.size))
        if job_transfer_key in seen_transfer_keys:
            continue
        seen_transfer_keys.add(job_transfer_key)
        unique_jobs.append(job)
        transfer = None
        persisted_id = str(getattr(job, "transfer_id", None) or "").strip()
        if persisted_id:
            transfer = transfers_by_id.get((str(job.username or "").casefold(), persisted_id))
        if transfer is None:
            transfer = transfers.get(job_transfer_key)
        if transfer is None:
            fallback_key = remote_transfer_fallback_key(
                job.username, job.remote_filename, int(job.size)
            )
            fallback_matches = fallback_transfers.get(fallback_key, [])
            if len(fallback_matches) == 1:
                transfer = fallback_matches[0]
        snapshots[int(job.id)] = transfer

    # Persist the cheap bulk-transfer snapshot immediately. Queue-position
    # requests actively contact remote peers and may be slower; they must not
    # hold back fresh speed/progress data from local readers.
    jobs.save_active_transfer_snapshots(snapshots)

    queue_metadata_changed = False
    if include_queue_positions:
        for job in unique_jobs:
            transfer = snapshots.get(int(job.id))
            preliminary = _active_download_row(job, transfer)
            if transfer is None or not preliminary["isQueued"]:
                continue
            transfer_id = str(transfer.get("id") or transfer.get("Id") or "").strip()
            if not transfer_id:
                continue
            try:
                transfer_username = str(
                    transfer.get("username") or transfer.get("Username") or job.username
                )
                refreshed = await client.queue_position(transfer_username, transfer_id)
                if refreshed is not None:
                    snapshots[int(job.id)] = {**transfer, **refreshed}
                    queue_metadata_changed = True
            except Exception:
                # Queue position is optional snapshot metadata. The worker keeps
                # the already-saved transfer even when the peer cannot answer.
                pass

    if queue_metadata_changed:
        jobs.save_active_transfer_snapshots(snapshots)

    rows: list[dict[str, Any]] = []
    for job in unique_jobs:
        row = _active_download_row(job, snapshots.get(int(job.id)))
        row["livePending"] = False
        rows.append(row)
    return _sort_active_download_rows(rows), None


async def download_snapshot_loop() -> None:
    """Continuously refresh live transfer state independently of browser traffic."""
    while True:
        try:
            await refresh_active_download_snapshot()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Preserve the last successful snapshot and surface the error to the
            # Downloads page instead of letting the worker die.
            try:
                jobs.record_active_transfer_snapshot_error(_upstream_error_message(exc))
            except Exception:
                pass
        await asyncio.sleep(3)




@app.get("/api/app-version", response_class=PlainTextResponse)
async def current_app_version() -> PlainTextResponse:
    """Return the running Soulseekarr version without allowing browser/proxy caching."""
    return PlainTextResponse(
        APP_VERSION,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


# ---------------------------------------------------------------------------
# Read-only qBittorrent Web API compatibility for Sonarr Activity -> Queue.
# ---------------------------------------------------------------------------
# Sonarr has no generic external-queue API; it populates Activity by polling
# configured download clients. These endpoints emulate only the qBittorrent
# read/status surface Sonarr needs. Soulseekarr deliberately rejects torrent
# additions, so this compatibility client cannot become a second downloader.

@app.get("/api/v2/app/webapiVersion", response_class=PlainTextResponse)
async def qbit_webapi_version() -> str:
    return QBIT_WEBAPI_VERSION


@app.get("/api/v2/app/version", response_class=PlainTextResponse)
async def qbit_client_version() -> str:
    return QBIT_CLIENT_VERSION


@app.post("/api/v2/auth/login", response_class=PlainTextResponse)
async def qbit_login() -> PlainTextResponse:
    # Soulseekarr itself currently has no UI/API authentication boundary, so
    # this compatibility endpoint does not pretend to add one. Supporting the
    # login shape simply lets Sonarr connect if credentials were entered.
    return PlainTextResponse("Ok.", headers={"Set-Cookie": "SID=soulseekarr; Path=/; HttpOnly; SameSite=Strict"})


@app.get("/api/v2/app/preferences")
async def qbit_preferences() -> dict[str, Any]:
    settings = settings_store.load()
    save_path = (settings.sonarr_import_path or "/downloads").rstrip("/") or "/"
    return {
        "save_path": save_path,
        "max_ratio_enabled": False,
        "max_ratio": -1,
        "max_seeding_time_enabled": False,
        "max_seeding_time": -1,
        "max_inactive_seeding_time_enabled": False,
        "max_inactive_seeding_time": -1,
        "max_ratio_act": 0,
        "queueing_enabled": True,
        "dht": True,
    }


@app.get("/api/v2/torrents/categories")
async def qbit_categories() -> dict[str, dict[str, str]]:
    settings = settings_store.load()
    save_path = (settings.sonarr_import_path or "/downloads").rstrip("/") or "/"
    return {
        category: {"name": category, "savePath": settings.radarr_import_path if category == QBIT_RADARR_CATEGORY else save_path}
        for category in sorted(_QBIT_CATEGORIES)
    }


@app.post("/api/v2/torrents/createCategory", response_class=PlainTextResponse)
async def qbit_create_category(category: str = Form(...), savePath: str = Form("")) -> PlainTextResponse:
    value = str(category or "").strip()
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
        raise HTTPException(status_code=400, detail="Category name contains invalid control characters.")
    if len(value) > 128:
        raise HTTPException(status_code=400, detail="Category name is too long.")
    if value and value not in _QBIT_CATEGORIES and len(_QBIT_CATEGORIES) >= 64:
        raise HTTPException(status_code=429, detail="Too many compatibility categories.")
    if value:
        _QBIT_CATEGORIES.add(value)
    return PlainTextResponse("")


def _sonarr_visible_active_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return only Soulseekarr work that Sonarr should see as active.

    Soulseek queue entries are intentionally private to Soulseekarr.  Sonarr's
    qBittorrent compatibility queue begins only once slskd has actually started
    transferring bytes (or Soulseekarr is doing post-download preparation/import).
    """
    return [row for row in rows if not bool(row.get("isQueued"))]


async def _qbit_visible_row_by_hash(hash_value: str) -> dict[str, Any] | None:
    wanted = str(hash_value or "").strip().lower()
    rows, _slskd_error = await active_download_rows(include_queue_positions=False)
    for row in _sonarr_visible_active_rows(rows):
        if qbit_job_hash(int(row.get("id") or 0)) == wanted:
            return row
    return None


@app.get("/api/v2/torrents/info")
async def qbit_torrents_info(category: str | None = None) -> list[dict[str, Any]]:
    # Sonarr should see only transfers that have actually started.  Do not ask
    # remote peers for queue positions while servicing Sonarr's frequent polling;
    # queued transfers are visible only on Soulseekarr's own Downloads page.
    rows, _slskd_error = await active_download_rows(include_queue_positions=False)
    rows = _sonarr_visible_active_rows(rows)
    settings = settings_store.load()
    save_path = (settings.sonarr_import_path or "/downloads").rstrip("/") or "/"
    visible_category = str(category or QBIT_DEFAULT_CATEGORY).strip() or QBIT_DEFAULT_CATEGORY
    result = []
    for row in rows:
        movie = is_movie_id(int(row.get("episode_id") or 0))
        if category and movie != (visible_category == QBIT_RADARR_CATEGORY):
            continue
        result.append(qbit_torrent_row(
            row, category=QBIT_RADARR_CATEGORY if movie else visible_category,
            save_path=settings.radarr_import_path if movie else save_path,
        ))
    return result


@app.get("/api/v2/torrents/properties")
async def qbit_torrent_properties(hash: str) -> dict[str, Any]:
    row = await _qbit_visible_row_by_hash(hash)
    if row is None:
        raise HTTPException(status_code=404, detail="Active Soulseekarr download not found")
    settings = settings_store.load()
    save_path = (settings.sonarr_import_path or "/downloads").rstrip("/") or "/"
    if is_movie_id(int(row.get("episode_id") or 0)):
        save_path = settings.radarr_import_path
    return {"hash": qbit_job_hash(int(row.get("id") or 0)), "save_path": save_path, "seeding_time": 0}


@app.get("/api/v2/torrents/files")
async def qbit_torrent_files(hash: str) -> list[dict[str, str]]:
    row = await _qbit_visible_row_by_hash(hash)
    if row is None:
        raise HTTPException(status_code=404, detail="Active Soulseekarr download not found")
    # qBittorrent file rows are relative to save_path. The route is mainly here
    # for compatibility/debugging because Soulseekarr never reports a completed
    # fake torrent to Sonarr.
    value = row.get("sonarr_file") or row.get("import_file") or str(row.get("remote_filename") or "").replace("\\", "/")
    name = Path(str(value)).name
    return [{"name": name}]


@app.post("/api/v2/torrents/add")
async def qbit_add_rejected() -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": (
                "Soulseekarr's qBittorrent compatibility API is read-only. "
                "It exists only to expose Soulseekarr jobs in Sonarr Activity/Queue."
            )
        },
    )


@app.post("/api/v2/torrents/delete")
async def qbit_delete_rejected() -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": "Remove/cancel Soulseekarr jobs from Soulseekarr, not Sonarr."},
    )

@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


async def _missing_page_payload(settings: Settings, page: int = 1) -> dict[str, Any]:
    episodes: list[MissingEpisode] = []
    active_soulseekarr_episode_ids, local_download_ids = _active_soulseekarr_download_markers()
    sonarr_queue_episode_ids: set[int] = set()
    error = None
    if media_configured(settings):
        try:
            sonarr = media_client(settings, SonarrClient)
            series_rows, wanted_missing = await asyncio.gather(
                sonarr.series(),
                sonarr.missing_episodes(False),
            )
            jobs.sync_series_preferences(series_rows, settings.new_series_download_enabled)
            all_missing = await _missing_catalog_with_specials(
                sonarr, series_rows, wanted_missing
            )
            episodes = _filter_download_enabled_episodes(all_missing)
            try:
                sonarr_queue_episode_ids = await _external_sonarr_queue_episode_ids(
                    sonarr,
                    local_episode_ids=active_soulseekarr_episode_ids,
                    local_download_ids=local_download_ids,
                )
            except Exception:
                # Download-source display is informational. A queue lookup failure
                # should not hide the Missing page or its search controls.
                sonarr_queue_episode_ids = set()
        except Exception as exc:
            error = str(exc)

    visible_episodes, pagination = _paginate_rows(episodes, page)
    search_eligible_episode_ids = {
        ep.id for ep in visible_episodes
        if episode_search_eligible(ep, delay_hours=settings.missing_episode_delay_hours)
    }
    search_available_at: dict[int, str | None] = {}
    for ep in visible_episodes:
        available_at = episode_search_available_at(ep, settings.missing_episode_delay_hours)
        search_available_at[ep.id] = (
            available_at.isoformat().replace("+00:00", "Z") if available_at else None
        )

    serialized_episodes = []
    for ep in visible_episodes:
        row = asdict(ep)
        row["episode_code"] = ep.episode_code
        row["search_cooldown_until"] = search_state.search_cooldown_until(
            ep.id, settings.repeat_search_cooldown_hours
        )
        serialized_episodes.append(row)

    return {
        "episodes": serialized_episodes,
        "sonarrQueueEpisodeIds": sorted(sonarr_queue_episode_ids),
        "activeSoulseekarrEpisodeIds": sorted(active_soulseekarr_episode_ids),
        "searchEligibleEpisodeIds": sorted(search_eligible_episode_ids),
        "searchAvailableAt": search_available_at,
        "missingEpisodeDelayHours": settings.missing_episode_delay_hours,
        "configured": configured(settings),
        "error": error,
        "pagination": pagination,
    }


def _dashboard_search_status(job: Any) -> tuple[str, str]:
    """Return a concise Dashboard label and Bootstrap-ish severity for a search."""
    status = str(getattr(job, "status", "") or "").strip().lower()
    result_count = max(0, int(getattr(job, "search_result_count", 0) or 0))
    usable_count = max(0, int(getattr(job, "search_usable_count", 0) or 0))
    queued_count = max(0, int(getattr(job, "search_queued_count", 0) or 0))
    if queued_count > 0:
        return f"{queued_count} queued", "success"
    if status == "failed":
        return "Failed", "danger"
    if status == "skipped":
        return "Skipped", "secondary"
    if result_count <= 0:
        return "Nothing found", "secondary"
    if usable_count <= 0:
        return "No eligible match", "warning"
    return "Success", "success"


def _dashboard_search_rows(limit: int = 10) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for job in jobs.recent_search_activity(limit):
        label, level = _dashboard_search_status(job)
        rows.append({
            "id": int(job.id),
            "source": "Manual" if str(job.cycle_source or "").lower() == "manual" else "Automatic",
            "targetType": str(job.search_target_type or "search").capitalize(),
            "episodeCode": str(job.episode_code or ""),
            "episodeTitle": str(job.episode_title or ""),
            "query": str(job.search_query or ""),
            "status": label,
            "level": level,
            "message": str(job.message or ""),
            "resultCount": max(0, int(job.search_result_count or 0)),
            "usableCount": max(0, int(job.search_usable_count or 0)),
            "queuedCount": max(0, int(job.search_queued_count or 0)),
            "updatedAt": str(job.updated_at or ""),
        })
    return rows


async def _dashboard_missing_count(settings: Settings, *, force: bool = False) -> tuple[int, str | None]:
    if not media_configured(settings):
        return 0, None

    overrides = jobs.series_download_overrides()
    signature = (
        str(settings.sonarr_url),
        str(settings.sonarr_api_key),
        str(settings.radarr_url),
        str(settings.radarr_api_key),
        bool(settings.new_series_download_enabled),
        tuple(sorted((int(k), bool(v)) for k, v in overrides.items())),
    )
    now = time.monotonic()
    if (
        not force
        and _DASHBOARD_MISSING_CACHE.get("signature") == signature
        and float(_DASHBOARD_MISSING_CACHE.get("expires_at") or 0.0) > now
    ):
        return int(_DASHBOARD_MISSING_CACHE.get("count") or 0), _DASHBOARD_MISSING_CACHE.get("error")

    async with _DASHBOARD_MISSING_CACHE_LOCK:
        now = time.monotonic()
        if (
            not force
            and _DASHBOARD_MISSING_CACHE.get("signature") == signature
            and float(_DASHBOARD_MISSING_CACHE.get("expires_at") or 0.0) > now
        ):
            return int(_DASHBOARD_MISSING_CACHE.get("count") or 0), _DASHBOARD_MISSING_CACHE.get("error")

        count = int(_DASHBOARD_MISSING_CACHE.get("count") or 0)
        error: str | None = None
        try:
            sonarr = media_client(settings, SonarrClient)
            series_rows, wanted_missing = await asyncio.gather(
                sonarr.series(),
                sonarr.missing_episodes(False),
            )
            jobs.sync_series_preferences(series_rows, settings.new_series_download_enabled)
            jobs.save_sonarr_series_snapshot(series_rows)
            all_missing = await _missing_catalog_with_specials(sonarr, series_rows, wanted_missing)
            count = len(_filter_download_enabled_episodes(all_missing))
        except Exception as exc:
            error = _upstream_error_message(exc)

        _DASHBOARD_MISSING_CACHE.update({
            "signature": signature,
            "expires_at": time.monotonic() + _DASHBOARD_MISSING_CACHE_TTL_SECONDS,
            "count": count,
            "error": error,
        })
        return count, error


async def _dashboard_payload(settings: Settings, *, refresh_missing: bool = False) -> dict[str, Any]:
    download_rows, slskd_error = saved_active_download_rows()
    series_rows, series_snapshot_state = _saved_sonarr_series_rows()
    series_options = _series_options(series_rows)
    missing_count, missing_error = await _dashboard_missing_count(settings, force=refresh_missing)

    queued_count = sum(
        1
        for row in download_rows
        if bool(row.get("isQueued"))
        or bool(row.get("waitingForPeerLimit"))
        or str(row.get("status") or "").lower() == "queued"
    )

    return {
        "downloads": download_rows[:10],
        "activeDownloadCount": len(download_rows),
        "searches": _dashboard_search_rows(10),
        "autoSearchSchedule": _auto_search_schedule_status(settings),
        "stats": {
            "missingEpisodes": int(missing_count),
            "seriesEnabled": sum(1 for item in series_options if item["download_enabled"]),
            "seriesTotal": len(series_options),
            "downloading": _actually_downloading_count(),
            "queued": int(queued_count),
        },
        "configured": configured(settings),
        "errors": {
            "missing": missing_error,
            "downloads": slskd_error,
            "series": series_snapshot_state.get("error"),
        },
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard_page(request: Request) -> HTMLResponse:
    settings = settings_store.load()
    payload = await _dashboard_payload(settings)
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "dashboard": payload,
            "configured": payload["configured"],
        },
    )


@app.get("/api/dashboard")
async def dashboard_api(refresh_missing: bool = False) -> JSONResponse:
    settings = settings_store.load()
    return JSONResponse(await _dashboard_payload(settings, refresh_missing=refresh_missing))


@app.get("/missing", response_class=HTMLResponse)
async def missing_page(request: Request, page: int = 1) -> HTMLResponse:
    settings = settings_store.load()
    payload = await _missing_page_payload(settings, page)
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "episodes": payload["episodes"],
            "sonarr_queue_episode_ids": set(payload["sonarrQueueEpisodeIds"]),
            "active_soulseekarr_episode_ids": set(payload["activeSoulseekarrEpisodeIds"]),
            "search_eligible_episode_ids": set(payload["searchEligibleEpisodeIds"]),
            "search_available_at": payload["searchAvailableAt"],
            "missing_episode_delay_hours": payload["missingEpisodeDelayHours"],
            "configured": payload["configured"],
            "error": payload["error"],
            "pagination": payload["pagination"],
            "initial_missing_payload": payload,
        },
    )


@app.get("/api/missing/page/{page}")
async def missing_page_api(page: int) -> JSONResponse:
    settings = settings_store.load()
    return JSONResponse(await _missing_page_payload(settings, page))


@app.get("/api/missing")
async def missing_page_api_legacy(page: int = 1) -> JSONResponse:
    """Backward-compatible endpoint for older open Soulseekarr pages."""
    return await missing_page_api(page)


async def _upgrade_page_payload(settings: Settings, page: int = 1, *, refresh: bool = False) -> dict[str, Any]:
    episodes: list[MissingEpisode] = []
    active_soulseekarr_episode_ids, local_download_ids = _active_soulseekarr_download_markers()
    sonarr_queue_episode_ids: set[int] = set()
    error = None
    if media_configured(settings):
        try:
            sonarr = media_client(settings, SonarrClient)
            series_rows = await sonarr.series()
            jobs.sync_series_preferences(series_rows, settings.new_series_download_enabled)
            episodes, catalog_errors = await _cached_upgrade_episode_catalog(
                sonarr, series_rows, force=refresh
            )
            if catalog_errors:
                error = (
                    f"Could not load {len(catalog_errors)} TV show"
                    f"{'s' if len(catalog_errors) != 1 else ''}: "
                    + "; ".join(catalog_errors[:3])
                )
            try:
                sonarr_queue_episode_ids = await _external_sonarr_queue_episode_ids(
                    sonarr,
                    local_episode_ids=active_soulseekarr_episode_ids,
                    local_download_ids=local_download_ids,
                )
            except Exception:
                sonarr_queue_episode_ids = set()
        except Exception as exc:
            error = str(exc)

    visible_episodes, pagination = _paginate_rows(episodes, page)
    search_eligible_episode_ids = {
        ep.id for ep in visible_episodes if _target_search_eligible(ep, settings)
    }
    search_available_at: dict[int, str | None] = {}
    for ep in visible_episodes:
        if ep.has_file:
            search_available_at[ep.id] = None
            continue
        available_at = episode_search_available_at(
            ep, settings.missing_episode_delay_hours
        )
        search_available_at[ep.id] = (
            available_at.isoformat().replace("+00:00", "Z") if available_at else None
        )

    serialized_episodes = []
    for ep in visible_episodes:
        row = asdict(ep)
        row["episode_code"] = ep.episode_code
        row["search_cooldown_until"] = search_state.search_cooldown_until(
            ep.id, settings.repeat_search_cooldown_hours
        )
        serialized_episodes.append(row)

    return {
        "episodes": serialized_episodes,
        "sonarrQueueEpisodeIds": sorted(sonarr_queue_episode_ids),
        "activeSoulseekarrEpisodeIds": sorted(active_soulseekarr_episode_ids),
        "searchEligibleEpisodeIds": sorted(search_eligible_episode_ids),
        "searchAvailableAt": search_available_at,
        "missingEpisodeDelayHours": settings.missing_episode_delay_hours,
        "configured": configured(settings),
        "error": error,
        "pagination": pagination,
    }


@app.get("/upgrade", response_class=HTMLResponse)
async def upgrade_page(request: Request, page: int = 1) -> HTMLResponse:
    settings = settings_store.load()
    payload = await _upgrade_page_payload(settings, page)
    return templates.TemplateResponse(
        request,
        "upgrade.html",
        {
            "episodes": payload["episodes"],
            "sonarr_queue_episode_ids": set(payload["sonarrQueueEpisodeIds"]),
            "active_soulseekarr_episode_ids": set(payload["activeSoulseekarrEpisodeIds"]),
            "search_eligible_episode_ids": set(payload["searchEligibleEpisodeIds"]),
            "search_available_at": payload["searchAvailableAt"],
            "missing_episode_delay_hours": payload["missingEpisodeDelayHours"],
            "configured": payload["configured"],
            "error": payload["error"],
            "pagination": payload["pagination"],
            "initial_upgrade_payload": payload,
        },
    )


@app.get("/api/upgrade/page/{page}")
async def upgrade_page_api(page: int, refresh: bool = False) -> JSONResponse:
    settings = settings_store.load()
    return JSONResponse(await _upgrade_page_payload(settings, page, refresh=refresh))


@app.get("/api/upgrade")
async def upgrade_page_api_legacy(page: int = 1, refresh: bool = False) -> JSONResponse:
    return await upgrade_page_api(page, refresh)


@app.get("/jobs")
async def jobs_page(page: int = 1) -> RedirectResponse:
    # Backward-compatible redirect for bookmarks from v0.3.65. Recent Jobs now
    # lives inside the History section under the Jobs tab.
    return RedirectResponse(url=f"/history?tab=jobs&page={max(1, int(page or 1))}", status_code=302)


@app.get("/series", response_class=HTMLResponse)
async def series_page(request: Request, page: int = 1, q: str = "") -> HTMLResponse:
    settings = settings_store.load()
    payload = await _series_page_payload(settings, page, q)
    return templates.TemplateResponse(
        request,
        "series.html",
        {
            "series_options": payload["seriesOptions"],
            "series_enabled_count": payload["seriesEnabledCount"],
            "series_total_count": payload["seriesTotalCount"],
            "series_query": payload["seriesQuery"],
            "pagination": payload["pagination"],
            "configured": payload["configured"],
            "error": payload["error"],
            "series_snapshot_refreshed_at": payload["snapshotRefreshedAt"],
            "series_snapshot_error_at": payload["snapshotErrorAt"],
        },
    )


@app.get("/api/series/page/{page}")
async def series_page_api(page: int, q: str = "", refresh: bool = False) -> JSONResponse:
    """Return one TV-show page for background pagination/prefetch."""
    settings = settings_store.load()
    payload = await _series_page_payload(settings, page, q, request_refresh=bool(refresh))
    return JSONResponse(payload)


@app.get("/api/series")
async def series_page_api_legacy(page: int = 1, q: str = "", refresh: bool = False) -> JSONResponse:
    """Backward-compatible endpoint for older open Soulseekarr pages."""
    return await series_page_api(page, q, refresh)


@app.get("/downloads", response_class=HTMLResponse)
async def downloads_page(request: Request, page: int = 1) -> HTMLResponse:
    # Render only from the last SQLite snapshot maintained by the background
    # worker. Opening Downloads never causes an slskd request.
    rows, slskd_error = saved_active_download_rows()
    visible_rows, pagination = _paginate_rows(rows, page)
    return templates.TemplateResponse(
        request,
        "downloads.html",
        {"downloads": visible_rows, "slskd_error": slskd_error, "pagination": pagination},
    )


@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request, tab: str = "jobs", page: int = 1) -> HTMLResponse:
    active_tab = str(tab or "jobs").strip().lower()
    if active_tab not in {"jobs", "downloads"}:
        active_tab = "jobs"

    recent_jobs: list[dict[str, Any]] = []
    history_rows: list[dict[str, Any]] = []
    if active_tab == "downloads":
        history, total, resolved_page = jobs.history_page(PAGE_SIZE, page)
        history_rows = [asdict(x) for x in history]
    else:
        recent_jobs, total, resolved_page = jobs.recent_page(PAGE_SIZE, page)

    pagination = _pagination_meta(total, resolved_page)
    return templates.TemplateResponse(
        request,
        "history.html",
        {
            "active_tab": active_tab,
            "jobs": recent_jobs,
            "history": history_rows,
            "pagination": pagination,
        },
    )


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request) -> HTMLResponse:
    settings = settings_store.load()
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "settings": settings,
            "saved": False,
            "auto_search_schedule": _auto_search_schedule_status(settings),
            "storage_space": _settings_storage_space(settings),
        },
    )


@app.post("/settings", response_class=HTMLResponse)
async def settings_save(
    request: Request,
    sonarr_url: str = Form(""), sonarr_api_key: str = Form(""), slskd_url: str = Form(""), slskd_api_key: str = Form(""),
    sonarr_import_path: str = Form("/downloads"), slskd_downloads_path: str = Form("/slskd-downloads"),
    scan_interval_minutes: int = Form(2), search_timeout_seconds: int = Form(15), auto_search_gap_seconds: int = Form(15),
    repeat_search_cooldown_hours: int = Form(24), missing_episode_delay_hours: int = Form(48),
    history_retention_hours: int = Form(48), max_search_results: int = Form(200),
    min_file_size_mb: int = Form(30), max_file_size_mb: int = Form(25000), download_timeout_minutes: int = Form(360),
    sonarr_import_timeout_minutes: int = Form(120), prefer_hardlinks: bool = Form(False),
    max_auto_per_cycle: int = Form(3), max_active_downloads_for_search: int = Form(100),
    dynamic_active_download_limit_enabled: bool = Form(False), max_actual_downloading_for_search: int = Form(5),
    peer_failure_strike_limit: int = Form(3), peer_failure_cooldown_days: int = Form(7),
    search_order: str = Form("sequential"),
    auto_search_enabled: bool = Form(False), auto_download: bool = Form(False),
    new_series_download_enabled: bool = Form(False),
    mapping_slskd_path: list[str] = Form([]), mapping_soulseekarr_path: list[str] = Form([]),
    mapping_sonarr_path: list[str] = Form([]),
    radarr_url: Annotated[str, Form()] = "",
    radarr_api_key: Annotated[str, Form()] = "",
    radarr_import_path: Annotated[str, Form()] = "/downloads",
    mapping_radarr_path: Annotated[list[str] | None, Form()] = None,
) -> HTMLResponse:
    old = settings_store.load()
    mapping_radarr_path = mapping_radarr_path or []
    raw_mapping_count = max(len(mapping_slskd_path), len(mapping_soulseekarr_path), len(mapping_sonarr_path), len(mapping_radarr_path))
    if raw_mapping_count > 64:
        raise HTTPException(status_code=400, detail="At most 64 folder mappings are allowed.")
    mappings = clean_mappings(
        {"slskd_path": slskd_path, "soulseekarr_path": soulseekarr_path, "sonarr_path": sonarr_path,
         "radarr_path": mapping_radarr_path[index] if index < len(mapping_radarr_path) else ""}
        for index, (slskd_path, soulseekarr_path, sonarr_path) in enumerate(zip(
            mapping_slskd_path, mapping_soulseekarr_path, mapping_sonarr_path
        ))
    )
    try:
        normalized_sonarr_url = normalize_service_url(sonarr_url)
        normalized_radarr_url = normalize_service_url(radarr_url)
        normalized_radarr_import_path = normalize_container_root(radarr_import_path or "/downloads", field_name="Radarr import path")
        normalized_slskd_url = normalize_service_url(slskd_url)
        normalized_sonarr_import_path = normalize_container_root(
            sonarr_import_path or "/downloads", field_name="Sonarr import path"
        )
        normalized_slskd_downloads_path = normalize_container_root(
            slskd_downloads_path or "/slskd-downloads", field_name="slskd downloads path"
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        new_sonarr_key = normalize_api_key(
            old.sonarr_api_key if sonarr_api_key == "********" else sonarr_api_key,
            field_name="Sonarr API key",
        )
        new_radarr_key = normalize_api_key(old.radarr_api_key if radarr_api_key == "********" else radarr_api_key, field_name="Radarr API key")
        new_slskd_key = normalize_api_key(
            old.slskd_api_key if slskd_api_key == "********" else slskd_api_key,
            field_name="slskd API key",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    settings = Settings(
        radarr_url=normalized_radarr_url,
        radarr_api_key=new_radarr_key,
        radarr_import_path=normalized_radarr_import_path,
        sonarr_url=normalized_sonarr_url,
        sonarr_api_key=new_sonarr_key,
        slskd_url=normalized_slskd_url,
        slskd_api_key=new_slskd_key,
        sonarr_import_path=normalized_sonarr_import_path,
        slskd_downloads_path=normalized_slskd_downloads_path,
        folder_mappings=mappings,
        scan_interval_minutes=max(1, min(10080, scan_interval_minutes)),
        search_timeout_seconds=max(5, min(60, search_timeout_seconds)),
        auto_search_gap_seconds=max(1, min(300, auto_search_gap_seconds)),
        repeat_search_cooldown_hours=max(0, min(720, repeat_search_cooldown_hours)),
        missing_episode_delay_hours=max(0, min(720, missing_episode_delay_hours)),
        history_retention_hours=max(1, min(8760, history_retention_hours)),
        max_search_results=max(10, min(1000, max_search_results)),
        min_file_size_mb=max(1, min(1_000_000, min_file_size_mb)), max_file_size_mb=max(max(1, min(1_000_000, min_file_size_mb)), min(1_000_000, max_file_size_mb)),
        download_timeout_minutes=max(10, min(10080, download_timeout_minutes)),
        sonarr_import_timeout_minutes=max(60, min(1440, sonarr_import_timeout_minutes)),
        prefer_hardlinks=prefer_hardlinks,
        max_auto_per_cycle=max(1, min(20, max_auto_per_cycle)),
        max_active_downloads_for_search=max(1, min(10000, max_active_downloads_for_search)),
        dynamic_active_download_limit_enabled=dynamic_active_download_limit_enabled,
        max_actual_downloading_for_search=max(0, min(1000, max_actual_downloading_for_search)),
        peer_failure_strike_limit=max(1, min(100, peer_failure_strike_limit)),
        peer_failure_cooldown_days=max(1, min(365, peer_failure_cooldown_days)),
        search_order="random" if search_order.strip().lower() == "random" else "sequential",
        auto_search_enabled=auto_search_enabled, auto_download=auto_download,
        new_series_download_enabled=new_series_download_enabled, monitored_only=old.monitored_only,
    )
    settings_store.save(settings)
    cleanup_database_history(settings)
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "settings": settings,
            "saved": True,
            "auto_search_schedule": _auto_search_schedule_status(settings),
            "storage_space": _settings_storage_space(settings),
        },
    )


@app.get("/api/notifications")
async def ui_notifications(after: int = 0) -> JSONResponse:
    rows, latest_id = jobs.ui_notifications_after(after)
    return JSONResponse({"notifications": rows, "latestId": latest_id})


@app.get("/api/auto-search-schedule")
async def auto_search_schedule() -> JSONResponse:
    settings = settings_store.load()
    return JSONResponse(_auto_search_schedule_status(settings))


class ConnectionTestRequest(BaseModel):
    url: str = Field(max_length=2048)
    api_key: str = Field(max_length=512)


def _connection_test_credentials(kind: str, connection: ConnectionTestRequest | None) -> tuple[str, str]:
    settings = settings_store.load()
    url = normalize_service_url(connection.url if connection is not None else getattr(settings, f"{kind}_url"))
    key = connection.api_key if connection is not None else getattr(settings, f"{kind}_api_key")
    if key == "********":
        key = getattr(settings, f"{kind}_api_key")
    key = normalize_api_key(key)
    if not url:
        raise ValueError("Enter a service URL starting with http:// or https://.")
    if not key:
        raise ValueError("Enter an API key.")
    return url, key


@app.post("/api/test/sonarr")
async def test_sonarr(connection: ConnectionTestRequest | None = None) -> JSONResponse:
    try:
        url, api_key = _connection_test_credentials("sonarr", connection)
        status = await SonarrClient(url, api_key).test()
        return JSONResponse({"ok": True, "version": status.get("version"), "appName": status.get("appName")})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/api/test/radarr")
async def test_radarr(connection: ConnectionTestRequest | None = None) -> JSONResponse:
    try:
        url, api_key = _connection_test_credentials("radarr", connection)
        status = await RadarrClient(url, api_key).test()
        return JSONResponse({"ok": True, "version": status.get("version")})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": _upstream_error_message(exc)}, status_code=400)


@app.post("/api/test/slskd")
async def test_slskd(connection: ConnectionTestRequest | None = None) -> JSONResponse:
    try:
        url, api_key = _connection_test_credentials("slskd", connection)
        data = await SlskdClient(url, api_key).test()
        summary = {}
        if isinstance(data, dict):
            for key in ("version", "serverVersion", "applicationVersion", "name", "applicationName"):
                value = data.get(key)
                if isinstance(value, (str, int, float, bool)) and value != "":
                    summary[key] = value
        return JSONResponse({"ok": True, **summary})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/api/episodes/{episode_id}/search")
async def search_episode(episode_id: int, variant: int | None = None, allow_existing: bool = False) -> JSONResponse:
    settings = settings_store.load()
    if not configured(settings):
        raise HTTPException(status_code=400, detail="Configure Sonarr or Radarr, and slskd first.")
    sonarr = media_client(settings, SonarrClient)
    series_rows = await _sync_series_preferences_from_sonarr(sonarr, settings)
    wanted_missing = await sonarr.missing_episodes(False)
    ep = await _resolve_search_episode_target(
        sonarr, settings, episode_id, allow_existing=allow_existing,
        series_rows=series_rows, wanted_rows=wanted_missing
    )
    if ep is None:
        detail = (
            "Episode is not in the enabled/monitored Upgrade catalog."
            if allow_existing
            else "Episode is no longer missing or is not eligible."
        )
        raise HTTPException(status_code=404, detail=detail)
    if not _target_search_eligible(ep, settings):
        available_at = episode_search_available_at(ep, settings.missing_episode_delay_hours)
        if available_at is None:
            detail = (
                "Episode has no valid Sonarr airDateUtc; Soulseekarr requires a valid air date "
                "before searching."
            )
        else:
            detail = (
                f"Episode is not eligible for Soulseek search until "
                f"{available_at.isoformat().replace('+00:00', 'Z')} "
                f"({settings.missing_episode_delay_hours} hours after airDateUtc)."
            )
        raise HTTPException(status_code=409, detail=detail)
    queries = search_queries(ep, jobs.series_search_mode(ep.series_id))
    selected_variant_index: int | None = None
    if variant is not None:
        if variant < 1 or variant > len(queries):
            raise HTTPException(
                status_code=400,
                detail=f"Search variant must be between 1 and {len(queries)}.",
            )
        selected_variant_index = variant - 1

    slskd = SlskdClient(settings.slskd_url, settings.slskd_api_key, timeout=settings.search_timeout_seconds + 10)
    all_results, search_errors, query, variant_index, variant_count = await collect_search_results(
        slskd, ep, settings, variant_index=selected_variant_index
    )
    if not all_results and search_errors:
        status = 429 if any("HTTP 429" in error for error in search_errors) else 502
        return JSONResponse(
            {"detail": search_errors[0], "errors": search_errors},
            status_code=status,
        )
    unique, ranked_all = await _usable_search_candidates(sonarr, ep, all_results, settings)
    skipped_downloaded = sum(1 for item in unique.values() if jobs.downloaded_before(item.filename, item.size))
    skipped_active = sum(
        1 for item in unique.values()
        if not jobs.downloaded_before(item.filename, item.size) and jobs.file_active(item.filename, item.size)
    )
    rejection_bans = jobs.active_file_rejection_ban_keys()
    skipped_rejected = sum(
        1 for item in unique.values()
        if peer_file_key(item.username, item.filename, item.size) in rejection_bans
    )
    ranked = ranked_all[:100]
    next_variant_index = _finish_search_variant(
        ep, variant_index, variant_count,
        usable_count=len(ranked_all), search_errors=search_errors,
    )
    next_query = queries[next_variant_index]
    return JSONResponse({
        "episode": {**asdict(ep), "episode_code": ep.episode_code},
        "query": query,
        "variant": variant_index + 1,
        "variantCount": variant_count,
        "currentVariant": next_variant_index + 1,
        "variants": [
            {"variant": index + 1, "query": item}
            for index, item in enumerate(queries)
        ],
        "nextQuery": next_query,
        "variantAdvanced": next_variant_index != variant_index,
        "totalUnique": len(unique),
        "accepted": len(ranked_all),
        "downloadedSkipped": skipped_downloaded,
        "activeSkipped": skipped_active,
        "rejectedFileSkipped": skipped_rejected,
        "warnings": search_errors,
        "slskdDiagnostics": dict(slskd.last_search_diagnostics),
        "results": [{
            "username": r.username, "filename": r.filename, "size": r.size, "uploadSpeed": r.upload_speed,
            "queueLength": r.queue_length, "score": round(r.score, 2),
        } for r in ranked],
    })


@app.post("/api/episodes/{episode_id}/auto-search")
async def auto_search_episode(episode_id: int, allow_existing: bool = False) -> JSONResponse:
    """Search one episode and automatically queue the best eligible candidate.

    This is an explicit user action from the Missing/Upgrade pages, so it does not move
    the background TV-show/season/episode round-robin cursors and it does not
    require Automatic search/downloads to be enabled. It uses the same candidate
    filtering and best-result selection as the automatic cycle, then performs
    the normal same-peer season-folder expansion.
    """
    settings = settings_store.load()
    if not configured(settings):
        raise HTTPException(status_code=400, detail="Configure Sonarr or Radarr, and slskd first.")

    sonarr = media_client(settings, SonarrClient)

    series_rows = await _sync_series_preferences_from_sonarr(sonarr, settings)
    wanted_missing = await sonarr.missing_episodes(False)
    ep = await _resolve_search_episode_target(
        sonarr, settings, episode_id, allow_existing=allow_existing,
        series_rows=series_rows, wanted_rows=wanted_missing
    )
    if ep is None:
        detail = (
            "Episode is not in the enabled/monitored Upgrade catalog."
            if allow_existing
            else "Episode is no longer missing or is not eligible."
        )
        raise HTTPException(status_code=404, detail=detail)
    if not _target_search_eligible(ep, settings):
        available_at = episode_search_available_at(ep, settings.missing_episode_delay_hours)
        if available_at is None:
            detail = (
                "Episode has no valid Sonarr airDateUtc; Soulseekarr requires a valid air date "
                "before searching."
            )
        else:
            detail = (
                f"Episode is not eligible for Soulseek search until "
                f"{available_at.isoformat().replace('+00:00', 'Z')} "
                f"({settings.missing_episode_delay_hours} hours after airDateUtc)."
            )
        raise HTTPException(status_code=409, detail=detail)

    slskd = SlskdClient(
        settings.slskd_url,
        settings.slskd_api_key,
        timeout=settings.search_timeout_seconds + 10,
    )
    attempt = await _automatic_search_candidates(
        slskd, sonarr, ep, settings, pace=True
    )
    search_errors = list(attempt["errors"])
    if not search_errors:
        search_state.record_search_success(ep.id)
    query = str(attempt["query"])
    raw_result_count = int(attempt["raw_result_count"])
    usable_count = int(attempt["usable_count"])
    search_target_type = "upgrade" if ep.has_file else "missing"
    ranked = _queueable_peer_candidates(list(attempt["ranked"]))

    if not ranked:
        if search_errors:
            detail = "Search failed: " + "; ".join(str(item) for item in search_errors)
            status = 429 if any("HTTP 429" in error for error in search_errors) else 502
        elif usable_count > 0:
            detail = (
                f"Found {usable_count} usable candidate(s), but none are queueable right now "
                "(for example peer cooldown/limit)."
            )
            status = 409
        elif raw_result_count > 0:
            detail = (
                f"Found {raw_result_count} result(s), but none passed "
                "quality/score/repeat-download eligibility."
            )
            status = 409
        else:
            detail = "Search returned no results."
            status = 404
        try:
            jobs.record_search_activity(
                cycle_source="manual",
                status="failed" if search_errors else "searched",
                episode_id=ep.id,
                episode_code=ep.episode_code,
                episode_title=f"{ep.series_title} · {ep.title}",
                search_target_type=search_target_type,
                search_query=query,
                result_count=raw_result_count,
                usable_count=usable_count,
                queued_count=0,
                message=detail,
            )
        except Exception:
            pass
        raise HTTPException(status_code=status, detail=detail)

    best = ranked[0]
    created_jobs, folder_warning = await queue_candidate_with_season_folder(
        ep, best, settings, sonarr, slskd
    )
    if not created_jobs:
        detail = folder_warning or "A candidate was selected, but no download was queued."
        try:
            jobs.record_search_activity(
                cycle_source="manual",
                status="skipped",
                episode_id=ep.id,
                episode_code=ep.episode_code,
                episode_title=f"{ep.series_title} · {ep.title}",
                search_target_type=search_target_type,
                search_query=query,
                result_count=raw_result_count,
                usable_count=usable_count,
                queued_count=0,
                message=detail,
            )
        except Exception:
            pass
        raise HTTPException(status_code=409, detail=detail)

    queued_count = len(created_jobs)
    if queued_count > 1:
        message = f"Queued {queued_count} episodes from {best.username}'s season folder."
    else:
        message = f"Queued {ep.series_title} {ep.episode_code} from {best.username}."
    if folder_warning:
        message += f" Folder expansion warning: {folder_warning}"

    try:
        jobs.record_search_activity(
            cycle_source="manual",
            status="searched",
            episode_id=ep.id,
            episode_code=ep.episode_code,
            episode_title=f"{ep.series_title} · {ep.title}",
            search_target_type=search_target_type,
            search_query=query,
            result_count=raw_result_count,
            usable_count=usable_count,
            queued_count=queued_count,
            message=message,
        )
    except Exception:
        pass

    return JSONResponse({
        "ok": True,
        "message": message,
        "query": query,
        "resultCount": raw_result_count,
        "usableCount": usable_count,
        "queuedCount": queued_count,
        "seasonExpanded": queued_count > 1,
        "folderWarning": folder_warning,
        "job": asdict(created_jobs[0]),
        "jobs": [asdict(item) for item in created_jobs],
    })


async def _run_targeted_search_notification(episode_id: int, allow_existing: bool) -> None:
    key = int(episode_id)
    try:
        response = await auto_search_episode(int(episode_id), allow_existing=bool(allow_existing))
        try:
            payload = json.loads(response.body.decode("utf-8"))
        except Exception:
            payload = {}
        queued_count = int(payload.get("queuedCount") or 0) if isinstance(payload, dict) else 0
        message = (
            str(payload.get("message") or "").strip()
            if isinstance(payload, dict)
            else ""
        ) or f"Queued {queued_count} download(s)."
        jobs.add_ui_notification(
            message,
            "success" if queued_count > 0 else "warning",
            topic="targeted-search",
            episode_id=int(episode_id),
        )
    except HTTPException as exc:
        detail = exc.detail
        if isinstance(detail, (dict, list)):
            try:
                message = json.dumps(detail, ensure_ascii=False)
            except Exception:
                message = str(detail)
        else:
            message = str(detail or "Automatic episode search failed.")
        jobs.add_ui_notification(
            message,
            "warning" if int(exc.status_code) in {404, 409} else "danger",
            topic="targeted-search",
            episode_id=int(episode_id),
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        jobs.add_ui_notification(
            f"Automatic episode search failed: {type(exc).__name__}: {exc}",
            "danger",
            topic="targeted-search",
            episode_id=int(episode_id),
        )
    finally:
        current = asyncio.current_task()
        if _TARGETED_SEARCH_TASKS.get(key) is current:
            _TARGETED_SEARCH_TASKS.pop(key, None)


@app.post("/api/episodes/{episode_id}/auto-search/start")
async def start_auto_search_episode(episode_id: int, allow_existing: bool = False) -> JSONResponse:
    key = int(episode_id)
    existing = _TARGETED_SEARCH_TASKS.get(key)
    if existing is not None and not existing.done():
        return JSONResponse({
            "ok": True,
            "started": False,
            "message": "A search is already running for this episode.",
        })

    task = asyncio.create_task(_run_targeted_search_notification(int(episode_id), bool(allow_existing)))
    _TARGETED_SEARCH_TASKS[key] = task
    return JSONResponse({"ok": True, "started": True}, status_code=202)


class SeriesSearchModeRequest(BaseModel):
    mode: str = Field(min_length=1, max_length=16)


@app.put("/api/series/{series_id}/search-mode")
async def set_series_search_mode(series_id: int, req: SeriesSearchModeRequest) -> JSONResponse:
    if series_id <= 0:
        raise HTTPException(status_code=400, detail="Invalid Sonarr series id.")
    mode = str(req.mode or "").strip().lower()
    if mode not in {"current", "legacy", "both"}:
        raise HTTPException(
            status_code=400,
            detail="Search mode must be current (S01E01), legacy (1x01), or both.",
        )
    saved = jobs.set_series_search_mode(series_id, mode)
    return JSONResponse({"ok": True, "seriesId": series_id, "mode": saved})


class SeriesDownloadRequest(BaseModel):
    enabled: bool


@app.put("/api/series/{series_id}/download")
async def set_series_download(series_id: int, req: SeriesDownloadRequest) -> JSONResponse:
    if series_id <= 0:
        raise HTTPException(status_code=400, detail="Invalid Sonarr series id.")
    previous = jobs.series_download_override(series_id)
    enabled = bool(req.enabled)
    jobs.set_series_download_enabled(series_id, enabled)

    priority_pending = False
    if enabled and previous is not True:
        # A deliberate disabled -> enabled toggle means "work on this show next".
        # The automatic cycle resets stale per-episode backoff after it refreshes
        # this show's Sonarr episode catalog, then consumes the flag after one
        # actual Soulseek search.
        search_state.queue_series_enable_priority(series_id)
        priority_pending = True
    elif not enabled:
        # Turning it back off before the cycle runs cancels the pending turn.
        search_state.clear_series_enable_priority(series_id)

    return JSONResponse({
        "ok": True,
        "seriesId": series_id,
        "enabled": enabled,
        "priorityPending": priority_pending,
    })


class DownloadRequest(BaseModel):
    username: str = Field(min_length=1, max_length=255)
    filename: str = Field(min_length=1, max_length=4096)
    size: int = Field(gt=0, le=100 * 1024**4)


@app.post("/api/episodes/{episode_id}/download")
async def download_episode(episode_id: int, req: DownloadRequest, allow_existing: bool = False) -> JSONResponse:
    settings = settings_store.load()
    if not configured(settings):
        raise HTTPException(status_code=400, detail="Configure Sonarr or Radarr, and slskd first.")
    rejection_ban = jobs.file_rejection_ban(req.username, req.filename, req.size)
    if rejection_ban is not None:
        raise HTTPException(
            status_code=409,
            detail=_file_rejection_ban_detail(req.username, rejection_ban),
        )
    cooldown = jobs.peer_cooldown(req.username)
    if cooldown is not None:
        raise HTTPException(
            status_code=429,
            detail=_peer_cooldown_detail(req.username, cooldown),
            headers={"Retry-After": str(max(1, int(cooldown.get("remainingSeconds") or 1)))},
        )
    if jobs.downloaded_before(req.filename, req.size):
        raise HTTPException(status_code=409, detail="Soulseekarr has already downloaded this file before.")
    if jobs.file_active(req.filename, req.size):
        raise HTTPException(status_code=409, detail="Soulseekarr is already downloading this file.")
    sonarr = media_client(settings, SonarrClient)
    series_rows = await _sync_series_preferences_from_sonarr(sonarr, settings)
    wanted_missing = await sonarr.missing_episodes(False)
    ep = await _resolve_search_episode_target(
        sonarr, settings, episode_id, allow_existing=allow_existing,
        series_rows=series_rows, wanted_rows=wanted_missing
    )
    if ep is None:
        detail = (
            "Episode is not in the enabled/monitored Upgrade catalog."
            if allow_existing
            else "Episode is no longer missing or is not eligible."
        )
        raise HTTPException(status_code=404, detail=detail)
    if not _target_search_eligible(ep, settings):
        available_at = episode_search_available_at(ep, settings.missing_episode_delay_hours)
        if available_at is None:
            detail = (
                "Episode has no valid Sonarr airDateUtc; Soulseekarr requires a valid air date "
                "before downloading."
            )
        else:
            detail = (
                f"Episode is not eligible for Soulseek download until "
                f"{available_at.isoformat().replace('+00:00', 'Z')} "
                f"({settings.missing_episode_delay_hours} hours after airDateUtc)."
            )
        raise HTTPException(status_code=409, detail=detail)
    candidate = SearchCandidate(username=req.username, filename=req.filename, size=req.size)
    ranked = rank_candidates(ep, [candidate], settings)
    if not ranked:
        raise HTTPException(status_code=400, detail="Selected file does not pass Soulseekarr's basic episode/size/type checks.")
    if not await _candidate_language_safe(sonarr, candidate, ep):
        raise HTTPException(
            status_code=400,
            detail="Selected file is foreign-language only. Soulseekarr accepts unspecified language or releases that include English.",
        )
    slskd = SlskdClient(settings.slskd_url, settings.slskd_api_key, timeout=20)
    created_jobs, folder_warning = await queue_candidate_with_season_folder(
        ep, candidate, settings, sonarr, slskd
    )
    if not created_jobs:
        cooldown = jobs.peer_cooldown(candidate.username)
        headers = None
        status_code = 409
        if cooldown is not None:
            headers = {"Retry-After": str(max(1, int(cooldown.get("remainingSeconds") or 1)))}
            status_code = 429
        raise HTTPException(
            status_code=status_code,
            detail=folder_warning or f"Soulseek peer {candidate.username} is temporarily unavailable.",
            headers=headers,
        )
    return JSONResponse({
        "ok": True,
        "job": asdict(created_jobs[0]),
        "jobs": [asdict(item) for item in created_jobs],
        "queuedCount": len(created_jobs),
        "seasonExpanded": len(created_jobs) > 1,
        "folderWarning": folder_warning,
    })


@app.get("/api/downloads")
async def list_active_downloads(page: int = 1) -> JSONResponse:
    rows, slskd_error = await active_download_rows()
    visible_rows, pagination = _paginate_rows(rows, page)
    return JSONResponse({
        "downloads": visible_rows,
        "count": pagination["total"],
        "pagination": pagination,
        "slskdError": slskd_error,
    })


@app.delete("/api/downloads/{job_id}")
async def remove_active_download(job_id: int) -> JSONResponse:
    try:
        result = await runner.cancel(int(job_id))
    except KeyError:
        raise HTTPException(status_code=404, detail="Soulseekarr download was not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not remove download from slskd: {exc}")
    return JSONResponse({"ok": True, **result})


@app.get("/api/jobs/{job_id}")
async def job_details(job_id: int) -> JSONResponse:
    try:
        job = jobs.get(int(job_id))
        repeat_protected = (
            jobs.download_repeat_protected(int(job_id))
            if getattr(job, "job_kind", "download") == "download"
            else False
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Soulseekarr job was not found.")
    return JSONResponse({"job": asdict(job), "repeatProtected": repeat_protected})


@app.delete("/api/jobs/{job_id}/download-history")
async def allow_job_download_again(job_id: int) -> JSONResponse:
    try:
        job = jobs.get(int(job_id))
        removed = jobs.forget_download_for_job(int(job_id))
    except KeyError:
        raise HTTPException(status_code=404, detail="Soulseekarr job was not found.")

    # A user explicitly asked to make this episode downloadable again. Preserve
    # the sticky search variant, but do not make them wait out a previous
    # automatic/manual search cooldown after clearing duplicate protection.
    search_state.clear_search_cooldown(job.episode_id)
    if removed:
        message = (
            "Download repeat protection removed. This History entry was kept, "
            "and the episode is immediately eligible to be searched/downloaded again."
        )
    else:
        message = (
            "This file was not currently blocked by downloaded-file history. "
            "The History entry was kept and the episode search cooldown was reset."
        )
    return JSONResponse({
        "ok": True,
        "removed": removed,
        "repeatProtected": False,
        "jobId": job.id,
        "episodeId": job.episode_id,
        "message": message,
    })


@app.get("/api/jobs")
async def list_jobs(page: int | None = None) -> JSONResponse:
    # Keep the legacy unpaginated response for API compatibility. The dedicated
    # Recent Jobs page requests a page explicitly.
    if page is None:
        return JSONResponse({"jobs": [asdict(x) for x in jobs.list(200)]})
    recent_jobs, total, resolved_page = jobs.recent_page(PAGE_SIZE, page)
    pagination = _pagination_meta(total, resolved_page)
    return JSONResponse({
        "jobs": [asdict(x) for x in recent_jobs],
        "pagination": pagination,
    })


@app.get("/api/history")
async def list_history(page: int = 1) -> JSONResponse:
    history, total, resolved_page = jobs.history_page(PAGE_SIZE, page)
    pagination = _pagination_meta(total, resolved_page)
    return JSONResponse({"history": [asdict(x) for x in history], "pagination": pagination})


@app.delete("/api/history/downloaded")
async def clear_downloaded_history() -> JSONResponse:
    cleared = jobs.clear_download_history()
    return JSONResponse({
        "ok": True,
        "cleared": cleared,
        "message": f"Cleared {cleared} downloaded-file history entr{'y' if cleared == 1 else 'ies'}.",
    })


@app.post("/api/run-auto-cycle")
async def run_auto_now() -> JSONResponse:
    settings = settings_store.load()
    if not settings.auto_search_enabled:
        try:
            jobs.record_search_activity(
                cycle_source="manual", status="skipped", search_target_type="cycle",
                message="No Soulseek search was run because Automatic search is disabled in Settings.",
            )
        except Exception:
            pass
        return JSONResponse({
            "ok": True,
            "started": False,
            "disabled": True,
            "message": "Automatic search is disabled in Settings.",
        })
    started = await auto_cycle(cycle_source="manual", max_episode_targets=1)
    if not started:
        try:
            jobs.record_search_activity(
                cycle_source="manual", status="skipped", search_target_type="cycle",
                message="No new cycle was started because another automatic/manual cycle is already running.",
            )
        except Exception:
            pass
    return JSONResponse({
        "ok": True,
        "started": started,
        "disabled": False,
        "message": "Automatic cycle completed." if started else "An automatic cycle is already running.",
    })


async def _run_manual_cycle_notification() -> None:
    global _MANUAL_CYCLE_TASK
    try:
        response = await run_auto_now()
        try:
            payload = json.loads(response.body.decode("utf-8"))
        except Exception:
            payload = {}
        started = bool(payload.get("started")) if isinstance(payload, dict) else False
        message = (
            str(payload.get("message") or "").strip()
            if isinstance(payload, dict)
            else ""
        ) or ("Automatic cycle completed." if started else "Automatic cycle did not start.")
        jobs.add_ui_notification(
            message,
            "success" if started else "warning",
            topic="auto-cycle",
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        jobs.add_ui_notification(
            f"Automatic cycle failed: {type(exc).__name__}: {exc}",
            "danger",
            topic="auto-cycle",
        )
    finally:
        if _MANUAL_CYCLE_TASK is asyncio.current_task():
            _MANUAL_CYCLE_TASK = None


@app.post("/api/run-auto-cycle/start")
async def start_auto_cycle_now() -> JSONResponse:
    global _MANUAL_CYCLE_TASK
    if _MANUAL_CYCLE_TASK is not None and not _MANUAL_CYCLE_TASK.done():
        return JSONResponse({
            "ok": True,
            "started": False,
            "message": "A manual automatic-search cycle is already running.",
        })
    _MANUAL_CYCLE_TASK = asyncio.create_task(_run_manual_cycle_notification())
    return JSONResponse({"ok": True, "started": True}, status_code=202)
