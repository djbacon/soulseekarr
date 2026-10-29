from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Iterator

from .pathmap import remote_transfer_fallback_key, remote_transfer_key


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def peer_file_key(username: str, remote_filename: str, size: int) -> tuple[str, str, int]:
    """Return a stable per-peer remote-file key for temporary rejection bans."""
    username_key = str(username or "").strip().casefold()
    normalized = str(remote_filename or "").replace("\\", "/")
    remote_key = "/".join(part for part in normalized.split("/") if part).strip().casefold()
    return username_key, remote_key, int(size)


def file_history_key(remote_filename: str) -> str:
    """Normalize a Soulseek file name for repeat-download detection.

    Peer folder layouts often differ even when the actual shared file is the
    same. Use the basename (case-insensitive) plus size rather than the entire
    remote path. The size is stored separately in the composite key.
    """
    normalized = str(remote_filename or "").replace("\\", "/")
    return normalized.rsplit("/", 1)[-1].strip().casefold()


@dataclass
class Job:
    id: int
    episode_id: int
    series_title: str
    episode_code: str
    episode_title: str
    username: str
    remote_filename: str
    size: int
    status: str
    message: str
    transfer_id: str | None
    import_file: str | None
    sonarr_file: str | None
    source_file: str | None
    slskd_file: str | None
    created_at: str
    updated_at: str
    downloaded_at: str | None
    prepare_step: str | None = None
    prepare_target_name: str | None = None
    queue_strikes: int = 0
    queue_last_position: int | None = None
    queue_last_checked_at: str | None = None
    candidate_custom_format_score: int | None = None
    candidate_resolution: int | None = None
    job_kind: str = "download"
    cycle_source: str | None = None
    search_target_type: str | None = None
    search_query: str | None = None
    search_result_count: int | None = None
    search_usable_count: int | None = None
    search_queued_count: int | None = None
    retry_parent_job_id: int | None = None
    transfer_retry_count: int = 0


@dataclass
class DownloadHistoryEntry:
    file_name_key: str
    size: int
    remote_filename: str
    username: str
    episode_id: int
    series_title: str
    episode_code: str
    job_id: int
    downloaded_at: str


@dataclass
class JobHistoryEntry:
    id: int
    episode_id: int
    series_title: str
    episode_code: str
    episode_title: str
    username: str
    remote_filename: str
    size: int
    status: str
    message: str
    transfer_id: str | None
    import_file: str | None
    sonarr_file: str | None
    source_file: str | None
    slskd_file: str | None
    created_at: str
    updated_at: str
    downloaded_at: str | None
    prepare_step: str | None = None
    prepare_target_name: str | None = None
    queue_strikes: int = 0
    queue_last_position: int | None = None
    queue_last_checked_at: str | None = None
    candidate_custom_format_score: int | None = None
    candidate_resolution: int | None = None
    job_kind: str = "download"
    cycle_source: str | None = None
    search_target_type: str | None = None
    search_query: str | None = None
    search_result_count: int | None = None
    search_usable_count: int | None = None
    search_queued_count: int | None = None
    retry_parent_job_id: int | None = None
    transfer_retry_count: int = 0


ACTIVE_JOB_STATUSES = ("queued", "downloading", "copying", "ready", "evaluating", "importing")
_ACTIVE_JOB_STATUSES_SQL = ", ".join(f"'{status}'" for status in ACTIVE_JOB_STATUSES)
_HISTORY_WHERE_SQL = f"job_kind = 'download' AND status NOT IN ({_ACTIVE_JOB_STATUSES_SQL})"


class JobStore:
    ACTIVE_STATUSES = ACTIVE_JOB_STATUSES

    def __init__(self) -> None:
        config_dir = Path(os.getenv("SOULSEEKARR_CONFIG_DIR", "/config"))
        if config_dir.is_symlink():
            raise RuntimeError("SOULSEEKARR_CONFIG_DIR may not be a symlink.")
        config_dir.mkdir(parents=True, exist_ok=True)
        if config_dir.is_symlink():
            raise RuntimeError("SOULSEEKARR_CONFIG_DIR may not be a symlink.")
        try:
            config_dir.chmod(0o700)
        except OSError:
            pass
        self.path = config_dir / "soulseekarr.db"
        if self.path.is_symlink():
            raise RuntimeError("Refusing to open soulseekarr.db through a symlink.")
        self._lock = RLock()
        self._init()
        try:
            self.path.chmod(0o600)
        except OSError:
            pass

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        if self.path.is_symlink():
            raise RuntimeError("Refusing to open soulseekarr.db through a symlink.")
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init(self) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    episode_id INTEGER NOT NULL,
                    series_title TEXT NOT NULL,
                    episode_code TEXT NOT NULL,
                    episode_title TEXT NOT NULL,
                    username TEXT NOT NULL,
                    remote_filename TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT NOT NULL DEFAULT '',
                    transfer_id TEXT,
                    import_file TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_episode ON jobs(episode_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
            # v0.3.17: keep the fact that a job reached network-download completion
            # on the job itself. The downloaded_files table can then be cleared to
            # reset duplicate protection without erasing the completed-job history.
            job_columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
            if "downloaded_at" not in job_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN downloaded_at TEXT")
            for column in (
                "sonarr_file",
                "source_file",
                "slskd_file",
                "prepare_step",
                "prepare_target_name",
            ):
                if column not in job_columns:
                    conn.execute(f"ALTER TABLE jobs ADD COLUMN {column} TEXT")
            if "queue_strikes" not in job_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN queue_strikes INTEGER NOT NULL DEFAULT 0")
            if "queue_last_position" not in job_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN queue_last_position INTEGER")
            if "queue_last_checked_at" not in job_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN queue_last_checked_at TEXT")
            if "candidate_custom_format_score" not in job_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN candidate_custom_format_score INTEGER")
            if "candidate_resolution" not in job_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN candidate_resolution INTEGER")
            if "job_kind" not in job_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN job_kind TEXT NOT NULL DEFAULT 'download'")
            for column in ("cycle_source", "search_target_type", "search_query"):
                if column not in job_columns:
                    conn.execute(f"ALTER TABLE jobs ADD COLUMN {column} TEXT")
            for column in ("search_result_count", "search_usable_count", "search_queued_count"):
                if column not in job_columns:
                    conn.execute(f"ALTER TABLE jobs ADD COLUMN {column} INTEGER")
            if "retry_parent_job_id" not in job_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN retry_parent_job_id INTEGER")
            if "transfer_retry_count" not in job_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN transfer_retry_count INTEGER NOT NULL DEFAULT 0")

            # History -> Downloads contains only actual download/import jobs.
            # Search/cycle audit rows belong exclusively to History -> Jobs.
            # Recreate the partial index because older releases used a predicate
            # that did not know about job_kind.
            conn.execute("DROP INDEX IF EXISTS idx_jobs_history_updated")
            conn.execute(
                f"CREATE INDEX idx_jobs_history_updated "
                f"ON jobs(updated_at DESC, id DESC) WHERE {_HISTORY_WHERE_SQL}"
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS downloaded_files (
                    file_name_key TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    remote_filename TEXT NOT NULL,
                    username TEXT NOT NULL,
                    episode_id INTEGER NOT NULL,
                    series_title TEXT NOT NULL,
                    episode_code TEXT NOT NULL,
                    job_id INTEGER NOT NULL,
                    downloaded_at TEXT NOT NULL,
                    PRIMARY KEY (file_name_key, size)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_downloaded_files_time ON downloaded_files(downloaded_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS series_preferences (
                    series_id INTEGER PRIMARY KEY,
                    download_enabled INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS series_search_preferences (
                    series_id INTEGER PRIMARY KEY,
                    search_mode TEXT NOT NULL DEFAULT 'current',
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS peer_cooldowns (
                    username_key TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    cooldown_until TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    kind TEXT NOT NULL DEFAULT 'limit'
                )
                """
            )
            peer_cooldown_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(peer_cooldowns)").fetchall()
            }
            if "kind" not in peer_cooldown_columns:
                conn.execute(
                    "ALTER TABLE peer_cooldowns ADD COLUMN kind TEXT NOT NULL DEFAULT 'limit'"
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS peer_failure_strikes (
                    username_key TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    strikes INTEGER NOT NULL DEFAULT 0,
                    last_reason TEXT NOT NULL,
                    last_failure_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS file_rejection_bans (
                    username_key TEXT NOT NULL,
                    remote_file_key TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    username TEXT NOT NULL,
                    remote_filename TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    cooldown_until TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (username_key, remote_file_key, size)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_file_rejection_bans_until "
                "ON file_rejection_bans(cooldown_until)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS active_transfer_snapshots (
                    job_id INTEGER PRIMARY KEY,
                    transfer_json TEXT,
                    refreshed_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS active_transfer_snapshot_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    refreshed_at TEXT,
                    error TEXT,
                    error_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sonarr_series_snapshot_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    series_json TEXT,
                    refreshed_at TEXT,
                    error TEXT,
                    error_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ui_notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    topic TEXT,
                    episode_id INTEGER,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ui_notifications_created "
                "ON ui_notifications(created_at DESC, id DESC)"
            )

            # Backfill legacy jobs into duplicate-download memory exactly once.
            # This must not run on every startup, otherwise a user-cleared history
            # would silently reappear after the next container restart.
            backfill_key = "download_history_backfill_v0317"
            already_backfilled = conn.execute(
                "SELECT 1 FROM app_meta WHERE key = ? LIMIT 1", (backfill_key,)
            ).fetchone()
            if not already_backfilled:
                old_rows = conn.execute(
                    """
                    SELECT * FROM jobs
                    WHERE import_file IS NOT NULL OR status IN ('imported', 'rejected')
                    ORDER BY id
                    """
                ).fetchall()
                for row in old_rows:
                    self._insert_history_row(conn, dict(row), downloaded_at=row["updated_at"])
                conn.execute(
                    "INSERT OR REPLACE INTO app_meta(key, value) VALUES (?, ?)",
                    (backfill_key, utcnow()),
                )

            # Preserve the download timestamp independently from duplicate-history
            # records so clearing downloaded_files does not rewrite job history.
            conn.execute(
                """
                UPDATE jobs
                SET downloaded_at = (
                    SELECT d.downloaded_at FROM downloaded_files AS d
                    WHERE d.job_id = jobs.id
                    ORDER BY d.downloaded_at DESC
                    LIMIT 1
                )
                WHERE downloaded_at IS NULL
                  AND EXISTS (SELECT 1 FROM downloaded_files AS d WHERE d.job_id = jobs.id)
                """
            )
            conn.execute(
                """
                UPDATE jobs
                SET downloaded_at = updated_at
                WHERE downloaded_at IS NULL
                  AND (import_file IS NOT NULL OR status IN ('imported', 'rejected'))
                """
            )

    @staticmethod
    def _insert_history_row(conn: sqlite3.Connection, job: dict[str, Any], downloaded_at: str | None = None) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO downloaded_files
            (file_name_key, size, remote_filename, username, episode_id, series_title, episode_code, job_id, downloaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                file_history_key(str(job["remote_filename"])),
                int(job["size"]),
                str(job["remote_filename"]),
                str(job["username"]),
                int(job["episode_id"]),
                str(job["series_title"]),
                str(job["episode_code"]),
                int(job["id"]),
                downloaded_at or utcnow(),
            ),
        )

    @staticmethod
    def _peer_key(username: str) -> str:
        return str(username or "").strip().casefold()

    def set_peer_cooldown(
        self, username: str, seconds: float, reason: str, *, kind: str = "limit"
    ) -> dict[str, Any]:
        """Pause new enqueue attempts to one remote peer for a temporary condition."""
        key = self._peer_key(username)
        if not key:
            raise ValueError("username is required")
        normalized_kind = "enqueue_failure" if str(kind) == "enqueue_failure" else "limit"
        now = datetime.now(timezone.utc)
        until = now + timedelta(seconds=max(0.0, float(seconds)))
        until_text = until.isoformat()
        now_text = now.isoformat()
        stored_reason = str(reason)
        stored_kind = normalized_kind
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                "SELECT cooldown_until, reason, kind FROM peer_cooldowns WHERE username_key = ?",
                (key,),
            ).fetchone()
            if existing is not None:
                try:
                    existing_until = datetime.fromisoformat(
                        str(existing["cooldown_until"]).replace("Z", "+00:00")
                    )
                    if existing_until.tzinfo is None:
                        existing_until = existing_until.replace(tzinfo=timezone.utc)
                except (TypeError, ValueError):
                    existing_until = now
                if existing_until > until:
                    until = existing_until
                    until_text = until.isoformat()
                    stored_reason = str(existing["reason"] or reason)
                    stored_kind = (
                        "enqueue_failure"
                        if str(existing["kind"] or "") == "enqueue_failure"
                        else "limit"
                    )
            conn.execute(
                """
                INSERT INTO peer_cooldowns(username_key, username, reason, cooldown_until, updated_at, kind)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(username_key) DO UPDATE SET
                    username = excluded.username,
                    reason = excluded.reason,
                    cooldown_until = excluded.cooldown_until,
                    updated_at = excluded.updated_at,
                    kind = excluded.kind
                """,
                (
                    key,
                    str(username).strip(),
                    stored_reason,
                    until_text,
                    now_text,
                    stored_kind,
                ),
            )
        return {
            "username": str(username).strip(),
            "reason": stored_reason,
            "kind": stored_kind,
            "cooldownUntil": until_text,
            "remainingSeconds": max(0, int((until - now).total_seconds())),
        }

    def peer_cooldown(self, username: str, *, now: datetime | None = None) -> dict[str, Any] | None:
        """Return a live peer cooldown, deleting an expired row on read."""
        key = self._peer_key(username)
        if not key:
            return None
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM peer_cooldowns WHERE username_key = ?", (key,)
            ).fetchone()
            if row is None:
                return None
            try:
                until = datetime.fromisoformat(str(row["cooldown_until"]).replace("Z", "+00:00"))
                if until.tzinfo is None:
                    until = until.replace(tzinfo=timezone.utc)
            except (TypeError, ValueError):
                conn.execute("DELETE FROM peer_cooldowns WHERE username_key = ?", (key,))
                return None
            remaining = (until - current).total_seconds()
            if remaining <= 0:
                conn.execute("DELETE FROM peer_cooldowns WHERE username_key = ?", (key,))
                return None
            return {
                "username": str(row["username"]),
                "reason": str(row["reason"]),
                "kind": (
                    "enqueue_failure"
                    if str(row["kind"] or "") == "enqueue_failure"
                    else "limit"
                ),
                "cooldownUntil": str(row["cooldown_until"]),
                "remainingSeconds": max(1, int(remaining)),
            }

    def peer_cooldown_active(self, username: str) -> bool:
        return self.peer_cooldown(username) is not None

    def set_file_rejection_ban(
        self, username: str, remote_filename: str, size: int, seconds: float, reason: str
    ) -> dict[str, Any]:
        """Temporarily block one exact remote file from one Soulseek peer."""
        username_key, remote_key, wanted_size = peer_file_key(username, remote_filename, size)
        if not username_key or not remote_key or wanted_size <= 0:
            raise ValueError("username, remote_filename and positive size are required")
        now = datetime.now(timezone.utc)
        until = now + timedelta(seconds=max(0.0, float(seconds)))
        now_text = now.isoformat()
        until_text = until.isoformat()
        stored_reason = str(reason or "remote file unavailable")
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                """SELECT cooldown_until, reason FROM file_rejection_bans
                   WHERE username_key = ? AND remote_file_key = ? AND size = ?""",
                (username_key, remote_key, wanted_size),
            ).fetchone()
            if existing is not None:
                try:
                    existing_until = datetime.fromisoformat(
                        str(existing["cooldown_until"]).replace("Z", "+00:00")
                    )
                    if existing_until.tzinfo is None:
                        existing_until = existing_until.replace(tzinfo=timezone.utc)
                except (TypeError, ValueError):
                    existing_until = now
                if existing_until > until:
                    until = existing_until
                    until_text = until.isoformat()
                    stored_reason = str(existing["reason"] or stored_reason)
            conn.execute(
                """
                INSERT INTO file_rejection_bans(
                    username_key, remote_file_key, size, username, remote_filename,
                    reason, cooldown_until, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(username_key, remote_file_key, size) DO UPDATE SET
                    username = excluded.username,
                    remote_filename = excluded.remote_filename,
                    reason = excluded.reason,
                    cooldown_until = excluded.cooldown_until,
                    updated_at = excluded.updated_at
                """,
                (
                    username_key, remote_key, wanted_size, str(username).strip(),
                    str(remote_filename), stored_reason, until_text, now_text, now_text,
                ),
            )
        return {
            "username": str(username).strip(),
            "remoteFilename": str(remote_filename),
            "size": wanted_size,
            "reason": stored_reason,
            "cooldownUntil": until_text,
            "remainingSeconds": max(0, int((until - now).total_seconds())),
        }

    def file_rejection_ban(
        self, username: str, remote_filename: str, size: int, *, now: datetime | None = None
    ) -> dict[str, Any] | None:
        """Return a live per-file rejection ban, deleting it after expiry."""
        username_key, remote_key, wanted_size = peer_file_key(username, remote_filename, size)
        if not username_key or not remote_key or wanted_size <= 0:
            return None
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """SELECT * FROM file_rejection_bans
                   WHERE username_key = ? AND remote_file_key = ? AND size = ?""",
                (username_key, remote_key, wanted_size),
            ).fetchone()
            if row is None:
                return None
            try:
                until = datetime.fromisoformat(str(row["cooldown_until"]).replace("Z", "+00:00"))
                if until.tzinfo is None:
                    until = until.replace(tzinfo=timezone.utc)
            except (TypeError, ValueError):
                conn.execute(
                    "DELETE FROM file_rejection_bans WHERE username_key = ? AND remote_file_key = ? AND size = ?",
                    (username_key, remote_key, wanted_size),
                )
                return None
            remaining = (until - current).total_seconds()
            if remaining <= 0:
                conn.execute(
                    "DELETE FROM file_rejection_bans WHERE username_key = ? AND remote_file_key = ? AND size = ?",
                    (username_key, remote_key, wanted_size),
                )
                return None
            return {
                "username": str(row["username"]),
                "remoteFilename": str(row["remote_filename"]),
                "size": int(row["size"]),
                "reason": str(row["reason"]),
                "cooldownUntil": str(row["cooldown_until"]),
                "remainingSeconds": max(1, int(remaining)),
            }

    def file_rejection_ban_active(self, username: str, remote_filename: str, size: int) -> bool:
        return self.file_rejection_ban(username, remote_filename, size) is not None

    def active_file_rejection_ban_keys(
        self, *, now: datetime | None = None
    ) -> set[tuple[str, str, int]]:
        """Return all live per-peer file bans as normalized lookup keys."""
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        live: set[tuple[str, str, int]] = set()
        expired: list[tuple[str, str, int]] = []
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT * FROM file_rejection_bans").fetchall()
            for row in rows:
                key = (str(row["username_key"]), str(row["remote_file_key"]), int(row["size"]))
                try:
                    until = datetime.fromisoformat(str(row["cooldown_until"]).replace("Z", "+00:00"))
                    if until.tzinfo is None:
                        until = until.replace(tzinfo=timezone.utc)
                except (TypeError, ValueError):
                    expired.append(key)
                    continue
                if until <= current:
                    expired.append(key)
                else:
                    live.add(key)
            if expired:
                conn.executemany(
                    "DELETE FROM file_rejection_bans WHERE username_key = ? AND remote_file_key = ? AND size = ?",
                    expired,
                )
        return live

    def record_peer_failure_strike(self, username: str, reason: str) -> dict[str, Any]:
        """Persist one user-level enqueue failure strike, case-insensitively."""
        key = self._peer_key(username)
        if not key:
            raise ValueError("username is required")
        now_text = utcnow()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO peer_failure_strikes(
                    username_key, username, strikes, last_reason, last_failure_at, updated_at
                )
                VALUES (?, ?, 1, ?, ?, ?)
                ON CONFLICT(username_key) DO UPDATE SET
                    username = excluded.username,
                    strikes = peer_failure_strikes.strikes + 1,
                    last_reason = excluded.last_reason,
                    last_failure_at = excluded.last_failure_at,
                    updated_at = excluded.updated_at
                """,
                (key, str(username).strip(), str(reason), now_text, now_text),
            )
            row = conn.execute(
                "SELECT * FROM peer_failure_strikes WHERE username_key = ?", (key,)
            ).fetchone()
        return {
            "username": str(row["username"]),
            "strikes": max(0, int(row["strikes"] or 0)),
            "lastReason": str(row["last_reason"]),
            "lastFailureAt": str(row["last_failure_at"]),
        }

    def peer_failure_strikes(self, username: str) -> dict[str, Any] | None:
        key = self._peer_key(username)
        if not key:
            return None
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM peer_failure_strikes WHERE username_key = ?", (key,)
            ).fetchone()
        if row is None:
            return None
        return {
            "username": str(row["username"]),
            "strikes": max(0, int(row["strikes"] or 0)),
            "lastReason": str(row["last_reason"]),
            "lastFailureAt": str(row["last_failure_at"]),
        }

    def clear_peer_failure_strikes(self, username: str) -> int:
        key = self._peer_key(username)
        if not key:
            return 0
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM peer_failure_strikes WHERE username_key = ?", (key,)
            )
        return int(cur.rowcount or 0)

    def retryable_peer_limit_failures(self, limit: int = 500) -> list[Job]:
        """Return historical slskd peer-limit failures for runner policy classification.

        The runner owns the exact retry policy because it understands Soulseek
        rejection semantics. Keep this database query intentionally broad enough
        to include queued, daily, and weekly ``Too many ...`` variants from older
        Soulseekarr versions.
        """
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM jobs
                WHERE job_kind = 'download'
                  AND status = 'failed'
                  AND LOWER(message) LIKE '%slskd%'
                  AND LOWER(message) LIKE '%transfer rejected:%'
                  AND LOWER(message) LIKE '%too many%'
                ORDER BY id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [Job(**dict(row)) for row in rows]

    def create(self, *, episode_id: int, series_title: str, episode_code: str, episode_title: str,
               username: str, remote_filename: str, size: int,
               prepare_target_name: str | None = None,
               candidate_custom_format_score: int | None = None,
               candidate_resolution: int | None = None,
               retry_parent_job_id: int | None = None,
               transfer_retry_count: int = 0) -> Job:
        """Create a queued job, reusing an equivalent active transfer atomically.

        Multiple search/recovery paths can converge on the same slskd transfer.
        The old check-then-insert flow allowed a race where several active jobs
        could be created for one remote file, causing the Downloads page (and
        runners) to mirror one real slskd transfer several times.  Serialize the
        normalized transfer-identity check and insert under the JobStore lock.
        """
        now = utcnow()
        wanted = remote_transfer_key(username, remote_filename, int(size))
        placeholders = ",".join("?" for _ in self.ACTIVE_STATUSES)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                f"""SELECT * FROM jobs
                    WHERE username = ? COLLATE NOCASE AND size = ?
                      AND status IN ({placeholders})
                    ORDER BY id""",
                (str(username), int(size), *self.ACTIVE_STATUSES),
            ).fetchall()
            for row in rows:
                existing_data = dict(row)
                existing = Job(**existing_data)
                if remote_transfer_key(existing.username, existing.remote_filename, existing.size) == wanted:
                    updates: dict[str, Any] = {}
                    if prepare_target_name and not existing.prepare_target_name:
                        updates["prepare_target_name"] = str(prepare_target_name)
                    if candidate_custom_format_score is not None and existing.candidate_custom_format_score is None:
                        updates["candidate_custom_format_score"] = int(candidate_custom_format_score)
                    if candidate_resolution is not None and existing.candidate_resolution is None:
                        updates["candidate_resolution"] = int(candidate_resolution)
                    if updates:
                        assignments = ", ".join(f"{key} = ?" for key in updates)
                        conn.execute(
                            f"UPDATE jobs SET {assignments}, updated_at = ? WHERE id = ?",
                            (*updates.values(), now, int(existing.id)),
                        )
                        existing_data.update(updates)
                        existing_data["updated_at"] = now
                        return Job(**existing_data)
                    return existing

            cur = conn.execute(
                """INSERT INTO jobs
                (episode_id, series_title, episode_code, episode_title, username, remote_filename, size,
                 status, message, prepare_target_name, candidate_custom_format_score, candidate_resolution,
                 retry_parent_job_id, transfer_retry_count, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', '', ?, ?, ?, ?, ?, ?, ?)""",
                (episode_id, series_title, episode_code, episode_title, username, remote_filename, size,
                 prepare_target_name, candidate_custom_format_score, candidate_resolution,
                 retry_parent_job_id, max(0, int(transfer_retry_count)), now, now),
            )
            job_id = int(cur.lastrowid)
        return self.get(job_id)

    def record_search_activity(
        self,
        *,
        cycle_source: str,
        message: str,
        status: str = "searched",
        episode_id: int = 0,
        episode_code: str = "",
        episode_title: str = "",
        search_target_type: str | None = None,
        search_query: str | None = None,
        result_count: int | None = None,
        usable_count: int | None = None,
        queued_count: int | None = None,
    ) -> Job:
        """Persist an automatic/manual search-cycle audit row.

        These rows intentionally live in the jobs table so History -> Jobs has
        one chronological activity stream. ``job_kind='search'`` keeps them out
        of History -> Downloads and out of every active-download query.
        """
        now = utcnow()
        source = str(cycle_source or "automatic").strip().lower()
        source_label = "Manual Cycle" if source == "manual" else "Automatic Cycle"
        query = str(search_query or "")
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO jobs
                (episode_id, series_title, episode_code, episode_title, username, remote_filename, size,
                 status, message, created_at, updated_at, job_kind, cycle_source, search_target_type,
                 search_query, search_result_count, search_usable_count, search_queued_count)
                VALUES (?, ?, ?, ?, '', ?, 0, ?, ?, ?, ?, 'search', ?, ?, ?, ?, ?, ?)""",
                (
                    int(episode_id or 0), source_label, str(episode_code or ""),
                    str(episode_title or ""), query, str(status or "searched"), str(message or ""),
                    now, now, source, search_target_type, search_query,
                    result_count, usable_count, queued_count,
                ),
            )
            job_id = int(cur.lastrowid)
        return self.get(job_id)

    def get(self, job_id: int) -> Job:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            raise KeyError(job_id)
        return Job(**dict(row))

    def update(self, job_id: int, **changes: Any) -> Job:
        allowed = {
            "status",
            "message",
            "transfer_id",
            "import_file",
            "sonarr_file",
            "source_file",
            "slskd_file",
            "prepare_step",
            "prepare_target_name",
            "queue_strikes",
            "queue_last_position",
            "queue_last_checked_at",
            "candidate_custom_format_score",
            "candidate_resolution",
        }
        values = {k: v for k, v in changes.items() if k in allowed}
        values["updated_at"] = utcnow()
        assignments = ", ".join(f"{key} = ?" for key in values)
        params = list(values.values()) + [job_id]
        with self._lock, self._connect() as conn:
            conn.execute(f"UPDATE jobs SET {assignments} WHERE id = ?", params)
        return self.get(job_id)

    def record_queue_check(self, job_id: int, position: int | None, *, checked_at: str | None = None) -> Job:
        """Persist one hourly queue-health observation.

        A lower positive queue number means the remote queue moved toward the
        front and resets strikes. Same/worse positions, and missing positions,
        add one strike. Missing positions do not erase the last known baseline.
        """
        timestamp = checked_at or utcnow()
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT queue_strikes, queue_last_position FROM jobs WHERE id = ?",
                (int(job_id),),
            ).fetchone()
            if row is None:
                raise KeyError(job_id)

            old_strikes = max(0, int(row["queue_strikes"] or 0))
            old_position = row["queue_last_position"]
            try:
                old_position = int(old_position) if old_position is not None else None
            except (TypeError, ValueError):
                old_position = None
            try:
                new_position = int(position) if position is not None else None
            except (TypeError, ValueError):
                new_position = None
            if new_position is not None and new_position <= 0:
                new_position = None

            improved = (
                old_position is not None
                and new_position is not None
                and new_position < old_position
            )
            strikes = 0 if improved else old_strikes + 1
            baseline = new_position if new_position is not None else old_position
            conn.execute(
                """
                UPDATE jobs
                SET queue_strikes = ?, queue_last_position = ?, queue_last_checked_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (strikes, baseline, timestamp, utcnow(), int(job_id)),
            )
        return self.get(job_id)

    def reset_queue_checks(self, job_id: int) -> Job:
        """Reset queued-transfer health state after the transfer actually starts."""
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET queue_strikes = 0, queue_last_position = NULL,
                    queue_last_checked_at = NULL, updated_at = ?
                WHERE id = ?
                """,
                (utcnow(), int(job_id)),
            )
        return self.get(job_id)

    def list(self, limit: int = 200) -> list[Job]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [Job(**dict(row)) for row in rows]

    def prune_history(self, retention_hours: float, *, now: datetime | None = None) -> dict[str, int]:
        """Delete expired finished/search history while preserving every active job.

        The retention window applies to History -> Jobs, History -> Downloads,
        and duplicate-download memory owned by those expired jobs.  Active jobs
        are never pruned, even if they have been queued/downloading/importing for
        longer than the configured retention period.
        """
        hours = max(1.0, float(retention_hours))
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        cutoff = (current.astimezone(timezone.utc) - timedelta(hours=hours)).isoformat()
        placeholders = ",".join("?" for _ in self.ACTIVE_STATUSES)

        with self._lock, self._connect() as conn:
            expired_rows = conn.execute(
                f"""SELECT id FROM jobs
                    WHERE status NOT IN ({placeholders}) AND updated_at < ?""",
                (*self.ACTIVE_STATUSES, cutoff),
            ).fetchall()
            expired_ids = [int(row["id"]) for row in expired_rows]

            repeat_rows = 0
            if expired_ids:
                id_placeholders = ",".join("?" for _ in expired_ids)
                cur = conn.execute(
                    f"DELETE FROM downloaded_files WHERE job_id IN ({id_placeholders})",
                    expired_ids,
                )
                repeat_rows += int(cur.rowcount or 0)
                cur = conn.execute(
                    f"DELETE FROM jobs WHERE id IN ({id_placeholders})",
                    expired_ids,
                )
                job_rows = int(cur.rowcount or 0)
            else:
                job_rows = 0

            # Clean legacy/orphan repeat-protection rows too, but never remove a
            # row belonging to a still-present active or recent job.
            cur = conn.execute(
                """DELETE FROM downloaded_files
                   WHERE downloaded_at < ?
                     AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.id = downloaded_files.job_id)""",
                (cutoff,),
            )
            repeat_rows += int(cur.rowcount or 0)

        return {"jobs": job_rows, "downloadedFiles": repeat_rows}

    def recent_search_activity(self, limit: int = 10) -> list[Job]:
        """Return the newest real episode-search audit rows for Dashboard.

        Generic cycle notices are intentionally excluded: the Dashboard search
        card is about which episode/query was actually searched and what happened.
        """
        limit = max(1, min(100, int(limit or 10)))
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM jobs
                WHERE job_kind = 'search'
                  AND episode_id > 0
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [Job(**dict(row)) for row in rows]

    def recent_page(self, limit: int = 25, page: int = 1) -> tuple[list[Job], int, int]:
        """Return one clamped Recent Jobs page and its total."""
        limit = max(1, int(limit))
        requested_page = max(1, int(page))
        with self._lock, self._connect() as conn:
            count_row = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
            total = int(count_row[0]) if count_row else 0
            total_pages = max(1, (total + limit - 1) // limit)
            resolved_page = min(requested_page, total_pages)
            offset = (resolved_page - 1) * limit
            rows = conn.execute(
                """
                SELECT *
                FROM jobs
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
        return [Job(**dict(row)) for row in rows], total, resolved_page

    def save_active_transfer_snapshots(
        self, snapshots: dict[int, dict[str, Any] | None], *, refreshed_at: str | None = None
    ) -> None:
        """Persist the latest successful background slskd snapshot.

        Downloads UI/API readers consume only this table; they never contact
        slskd directly. A successful refresh replaces the active-job snapshot
        set atomically while preserving an explicit ``NULL`` transfer for active
        jobs that are currently absent from slskd (for example post-download
        preparation/import states).
        """
        timestamp = refreshed_at or utcnow()
        ids = sorted({int(job_id) for job_id in snapshots})
        with self._lock, self._connect() as conn:
            if ids:
                placeholders = ",".join("?" for _ in ids)
                conn.execute(
                    f"DELETE FROM active_transfer_snapshots WHERE job_id NOT IN ({placeholders})",
                    ids,
                )
            else:
                conn.execute("DELETE FROM active_transfer_snapshots")

            for job_id, transfer in snapshots.items():
                payload = None if transfer is None else json.dumps(transfer, separators=(",", ":"), default=str)
                conn.execute(
                    """
                    INSERT INTO active_transfer_snapshots(job_id, transfer_json, refreshed_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(job_id) DO UPDATE SET
                        transfer_json = excluded.transfer_json,
                        refreshed_at = excluded.refreshed_at
                    """,
                    (int(job_id), payload, timestamp),
                )

            conn.execute(
                """
                INSERT INTO active_transfer_snapshot_state(singleton, refreshed_at, error, error_at)
                VALUES (1, ?, NULL, NULL)
                ON CONFLICT(singleton) DO UPDATE SET
                    refreshed_at = excluded.refreshed_at,
                    error = NULL,
                    error_at = NULL
                """,
                (timestamp,),
            )

    def record_active_transfer_snapshot_error(self, error: str, *, error_at: str | None = None) -> None:
        """Record a background refresh error without discarding the last good snapshot."""
        timestamp = error_at or utcnow()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO active_transfer_snapshot_state(singleton, refreshed_at, error, error_at)
                VALUES (1, NULL, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    error = excluded.error,
                    error_at = excluded.error_at
                """,
                (str(error or ""), timestamp),
            )

    def active_transfer_snapshots(self) -> tuple[dict[int, dict[str, Any] | None], dict[str, str | None]]:
        """Load the last saved live-transfer snapshot and refresh metadata."""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT job_id, transfer_json, refreshed_at FROM active_transfer_snapshots"
            ).fetchall()
            state_row = conn.execute(
                "SELECT refreshed_at, error, error_at FROM active_transfer_snapshot_state WHERE singleton = 1"
            ).fetchone()

        snapshots: dict[int, dict[str, Any] | None] = {}
        for row in rows:
            payload = row["transfer_json"]
            if payload is None:
                snapshots[int(row["job_id"])] = None
                continue
            try:
                decoded = json.loads(str(payload))
            except (TypeError, ValueError):
                decoded = None
            snapshots[int(row["job_id"])] = decoded if isinstance(decoded, dict) else None

        state = {
            "refreshedAt": str(state_row["refreshed_at"]) if state_row and state_row["refreshed_at"] else None,
            "error": str(state_row["error"]) if state_row and state_row["error"] else None,
            "errorAt": str(state_row["error_at"]) if state_row and state_row["error_at"] else None,
        }
        return snapshots, state

    def save_sonarr_series_snapshot(
        self, series_rows: list[dict[str, Any]], *, refreshed_at: str | None = None
    ) -> None:
        """Persist the latest successful Sonarr series list for UI/background readers."""
        timestamp = refreshed_at or utcnow()
        payload = json.dumps(list(series_rows or []), separators=(",", ":"), default=str)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO sonarr_series_snapshot_state(singleton, series_json, refreshed_at, error, error_at)
                VALUES (1, ?, ?, NULL, NULL)
                ON CONFLICT(singleton) DO UPDATE SET
                    series_json = excluded.series_json,
                    refreshed_at = excluded.refreshed_at,
                    error = NULL,
                    error_at = NULL
                """,
                (payload, timestamp),
            )

    def record_sonarr_series_snapshot_error(self, error: str, *, error_at: str | None = None) -> None:
        """Record a background Sonarr series refresh error without dropping the last good list."""
        timestamp = error_at or utcnow()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO sonarr_series_snapshot_state(singleton, series_json, refreshed_at, error, error_at)
                VALUES (1, NULL, NULL, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    error = excluded.error,
                    error_at = excluded.error_at
                """,
                (str(error or ""), timestamp),
            )

    def sonarr_series_snapshot(self) -> tuple[list[dict[str, Any]], dict[str, str | None]]:
        """Load the last saved Sonarr series list and refresh metadata from SQLite only."""
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT series_json, refreshed_at, error, error_at FROM sonarr_series_snapshot_state WHERE singleton = 1"
            ).fetchone()

        rows: list[dict[str, Any]] = []
        if row is not None and row["series_json"]:
            try:
                decoded = json.loads(str(row["series_json"]))
            except (TypeError, ValueError):
                decoded = []
            if isinstance(decoded, list):
                rows = [item for item in decoded if isinstance(item, dict)]

        state = {
            "refreshedAt": str(row["refreshed_at"]) if row and row["refreshed_at"] else None,
            "error": str(row["error"]) if row and row["error"] else None,
            "errorAt": str(row["error_at"]) if row and row["error_at"] else None,
        }
        return rows, state

    def active_for_episode(self, episode_id: int) -> bool:
        placeholders = ",".join("?" for _ in self.ACTIVE_STATUSES)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                f"SELECT 1 FROM jobs WHERE episode_id = ? AND status IN ({placeholders}) LIMIT 1",
                (episode_id, *self.ACTIVE_STATUSES),
            ).fetchone()
        return bool(row)

    def active(self) -> list[Job]:
        placeholders = ",".join("?" for _ in self.ACTIVE_STATUSES)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM jobs WHERE status IN ({placeholders}) ORDER BY id",
                self.ACTIVE_STATUSES,
            ).fetchall()
        return [Job(**dict(row)) for row in rows]

    def collapse_duplicate_active_transfers(self) -> list[tuple[int, int]]:
        """Terminalize duplicate active rows that represent one slskd transfer.

        Returns ``(duplicate_job_id, keeper_job_id)`` pairs.  This intentionally
        changes only Soulseekarr database rows; the shared slskd transfer is left
        untouched.  It repairs databases produced by older race-prone versions.
        """
        priority = {
            "queued": 0,
            "downloading": 1,
            "copying": 2,
            "ready": 3,
            "evaluating": 4,
            "importing": 5,
        }
        placeholders = ",".join("?" for _ in self.ACTIVE_STATUSES)
        now = utcnow()
        collapsed: list[tuple[int, int]] = []
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM jobs WHERE status IN ({placeholders}) ORDER BY id",
                self.ACTIVE_STATUSES,
            ).fetchall()
            groups: dict[tuple[str, str, int], list[Job]] = {}
            for row in rows:
                job = Job(**dict(row))
                key = remote_transfer_key(job.username, job.remote_filename, job.size)
                groups.setdefault(key, []).append(job)

            for group in groups.values():
                if len(group) <= 1:
                    continue
                keeper = max(
                    group,
                    key=lambda job: (
                        priority.get(job.status, -1),
                        bool(job.sonarr_file or job.import_file or job.source_file or job.slskd_file),
                        bool(job.transfer_id),
                        -int(job.id),
                    ),
                )
                for duplicate in group:
                    if duplicate.id == keeper.id:
                        continue
                    conn.execute(
                        """UPDATE jobs
                           SET status = 'cancelled', message = ?, updated_at = ?
                           WHERE id = ?""",
                        (
                            f"Duplicate active Soulseekarr job collapsed into job #{keeper.id}; "
                            "the shared slskd transfer was left running.",
                            now,
                            int(duplicate.id),
                        ),
                    )
                    collapsed.append((int(duplicate.id), int(keeper.id)))
        return collapsed

    def latest_for_transfer(self, username: str, remote_filename: str, size: int) -> Job | None:
        """Return the newest Soulseekarr job that owns an equivalent slskd transfer.

        Exact identity remains authoritative.  If slskd restored the transfer
        with a stripped/normalized remote path root, a rootless fallback is used
        only when it resolves to a single historical Soulseekarr transfer
        identity for that peer and size.
        """
        wanted = remote_transfer_key(username, remote_filename, size)
        fallback = remote_transfer_fallback_key(username, remote_filename, size)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM jobs
                WHERE username = ? AND remote_filename = ? AND size = ?
                ORDER BY id DESC LIMIT 1
                """,
                (str(username), str(remote_filename), int(size)),
            ).fetchone()
            if row is not None:
                return Job(**dict(row))

            candidates = conn.execute(
                """
                SELECT * FROM jobs
                WHERE size = ? AND username = ? COLLATE NOCASE
                ORDER BY id DESC
                """,
                (int(size), str(username)),
            ).fetchall()

        exact_matches: list[Job] = []
        fallback_matches: dict[tuple[str, str, int], Job] = {}
        for candidate in candidates:
            job = Job(**dict(candidate))
            key = remote_transfer_key(job.username, job.remote_filename, job.size)
            if key == wanted:
                exact_matches.append(job)
                continue
            if remote_transfer_fallback_key(job.username, job.remote_filename, job.size) == fallback:
                # Several attempts of the same exact remote transfer are one
                # identity; keep only the newest job for that exact identity.
                fallback_matches.setdefault(key, job)

        if exact_matches:
            return exact_matches[0]
        if len(fallback_matches) == 1:
            return next(iter(fallback_matches.values()))
        return None

    def recoverable_missing_episode_ids(self, limit: int = 128) -> list[int]:
        """Return recent terminal download episodes with no active Soulseekarr job.

        Sonarr's Wanted/Missing endpoint can temporarily omit an episode after a
        download-client item disappears.  These episode ids are cheap recovery
        hints: callers can ask Sonarr's direct episode endpoint whether the file
        is truly still absent, without scanning every episode of every series.
        """
        limit = max(1, min(1000, int(limit)))
        active_placeholders = ",".join("?" for _ in self.ACTIVE_STATUSES)
        terminal = ("failed", "cancelled", "rejected")
        terminal_placeholders = ",".join("?" for _ in terminal)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT j.episode_id, MAX(j.updated_at) AS last_updated
                FROM jobs j
                WHERE j.job_kind = 'download'
                  AND j.status IN ({terminal_placeholders})
                  AND NOT EXISTS (
                      SELECT 1 FROM jobs a
                      WHERE a.episode_id = j.episode_id
                        AND a.job_kind = 'download'
                        AND a.status IN ({active_placeholders})
                  )
                GROUP BY j.episode_id
                ORDER BY last_updated DESC
                LIMIT ?
                """,
                (*terminal, *self.ACTIVE_STATUSES, limit),
            ).fetchall()
        return [int(row["episode_id"]) for row in rows]

    def active_count(self) -> int:
        """Return the number of jobs currently occupying the Downloads queue."""
        placeholders = ",".join("?" for _ in self.ACTIVE_STATUSES)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS count FROM jobs WHERE status IN ({placeholders})",
                self.ACTIVE_STATUSES,
            ).fetchone()
        return int(row["count"] if row else 0)


    @staticmethod
    def _normalize_series_search_mode(mode: str | None) -> str:
        value = str(mode or "current").strip().lower()
        return value if value in {"current", "legacy", "both"} else "current"

    def series_search_modes(self) -> dict[int, str]:
        """Return explicit per-series episode query-format choices.

        Absence from this table intentionally means ``current`` so upgrades from
        releases before v0.3.101 give every existing show the new default without
        needing a bulk migration row.
        """
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT series_id, search_mode FROM series_search_preferences"
            ).fetchall()
        return {
            int(row["series_id"]): self._normalize_series_search_mode(row["search_mode"])
            for row in rows
        }

    def series_search_mode(self, series_id: int) -> str:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT search_mode FROM series_search_preferences WHERE series_id = ?",
                (int(series_id),),
            ).fetchone()
        if row is None:
            return "current"
        return self._normalize_series_search_mode(row["search_mode"])

    def set_series_search_mode(self, series_id: int, mode: str) -> str:
        normalized = self._normalize_series_search_mode(mode)
        if str(mode or "").strip().lower() not in {"current", "legacy", "both"}:
            raise ValueError("search mode must be current, legacy, or both")
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO series_search_preferences(series_id, search_mode, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(series_id) DO UPDATE SET
                    search_mode = excluded.search_mode,
                    updated_at = excluded.updated_at
                """,
                (int(series_id), normalized, utcnow()),
            )
        return normalized

    def series_download_overrides(self) -> dict[int, bool]:
        """Return explicit per-series Soulseekarr download choices.

        Series without an override follow Sonarr's current monitored state. This
        keeps newly added monitored series enabled by default while unmonitored
        series remain disabled until the user explicitly opts them in.
        """
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT series_id, download_enabled FROM series_preferences"
            ).fetchall()
        return {int(row["series_id"]): bool(row["download_enabled"]) for row in rows}

    def series_download_override(self, series_id: int) -> bool | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT download_enabled FROM series_preferences WHERE series_id = ?",
                (int(series_id),),
            ).fetchone()
        if row is None:
            return None
        return bool(row["download_enabled"])

    def set_series_download_enabled(self, series_id: int, enabled: bool) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO series_preferences(series_id, download_enabled, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(series_id) DO UPDATE SET
                    download_enabled = excluded.download_enabled,
                    updated_at = excluded.updated_at
                """,
                (int(series_id), 1 if enabled else 0, utcnow()),
            )

    def sync_series_preferences(self, series_rows: list[dict[str, Any]], default_enabled: bool) -> dict[int, bool]:
        """Persist a stable download choice for every Sonarr series we have seen.

        On the first v0.3.43 sync, series that predate this feature keep the
        effective behavior from older Soulseekarr releases: monitored Sonarr
        series start enabled and unmonitored series start disabled. After that
        bootstrap, a series seen for the first time uses the user's configured
        new-series default. Once inserted, changing the default never rewrites
        an existing series choice.
        """
        bootstrap_key = "series_preferences_bootstrap_v0343"
        with self._lock, self._connect() as conn:
            seeded = conn.execute(
                "SELECT 1 FROM app_meta WHERE key = ? LIMIT 1", (bootstrap_key,)
            ).fetchone() is not None
            existing = {
                int(row["series_id"]): bool(row["download_enabled"])
                for row in conn.execute(
                    "SELECT series_id, download_enabled FROM series_preferences"
                ).fetchall()
            }

            for item in series_rows:
                if not isinstance(item, dict):
                    continue
                try:
                    series_id = int(item.get("id"))
                except (TypeError, ValueError):
                    continue
                if series_id <= 0 or series_id in existing:
                    continue

                enabled = bool(default_enabled) if seeded else bool(item.get("monitored", False))
                conn.execute(
                    "INSERT INTO series_preferences(series_id, download_enabled, updated_at) VALUES (?, ?, ?)",
                    (series_id, 1 if enabled else 0, utcnow()),
                )
                existing[series_id] = enabled

            if not seeded:
                conn.execute(
                    "INSERT OR REPLACE INTO app_meta(key, value) VALUES (?, ?)",
                    (bootstrap_key, utcnow()),
                )

        return existing

    def episode_jobs(self, episode_id: int, statuses: tuple[str, ...] | None = None) -> list[Job]:
        with self._lock, self._connect() as conn:
            if statuses:
                placeholders = ",".join("?" for _ in statuses)
                rows = conn.execute(
                    f"SELECT * FROM jobs WHERE episode_id = ? AND status IN ({placeholders}) ORDER BY id",
                    (int(episode_id), *statuses),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE episode_id = ? ORDER BY id",
                    (int(episode_id),),
                ).fetchall()
        return [Job(**dict(row)) for row in rows]

    def has_pending_downloads_for_episode(self, episode_id: int) -> bool:
        pending = ("queued", "downloading", "copying")
        placeholders = ",".join("?" for _ in pending)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                f"SELECT 1 FROM jobs WHERE episode_id = ? AND status IN ({placeholders}) LIMIT 1",
                (int(episode_id), *pending),
            ).fetchone()
        return bool(row)

    def mark_downloaded(self, job_id: int) -> DownloadHistoryEntry:
        job = self.get(job_id)
        downloaded_at = job.downloaded_at or utcnow()
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE jobs SET downloaded_at = ?, updated_at = ? WHERE id = ?",
                (downloaded_at, downloaded_at, job_id),
            )
            job_data = dict(job.__dict__)
            job_data["downloaded_at"] = downloaded_at
            self._insert_history_row(conn, job_data, downloaded_at=downloaded_at)
            row = conn.execute(
                "SELECT * FROM downloaded_files WHERE file_name_key = ? AND size = ?",
                (file_history_key(job.remote_filename), int(job.size)),
            ).fetchone()
        if not row:
            raise RuntimeError("Failed to persist downloaded-file history")
        return DownloadHistoryEntry(**dict(row))

    def downloaded_before(self, remote_filename: str, size: int) -> bool:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM downloaded_files WHERE file_name_key = ? AND size = ? LIMIT 1",
                (file_history_key(remote_filename), int(size)),
            ).fetchone()
        return bool(row)

    def download_repeat_protected(self, job_id: int) -> bool:
        """Return whether this job's file is currently blocked by download history."""
        job = self.get(int(job_id))
        return self.downloaded_before(job.remote_filename, job.size)

    def forget_download_for_job(self, job_id: int) -> bool:
        """Remove duplicate-download protection for one job without deleting the job.

        Download completion is intentionally kept on ``jobs.downloaded_at`` so the
        History row remains a faithful audit record. Only the filename+size entry
        used by candidate repeat protection is removed.
        """
        job = self.get(int(job_id))
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM downloaded_files WHERE file_name_key = ? AND size = ?",
                (file_history_key(job.remote_filename), int(job.size)),
            )
        return int(cur.rowcount or 0) > 0

    def clear_owned_repeat_protection(self, job_id: int) -> bool:
        """Remove repeat protection only when this exact job created the block row.

        Retryable peer-limit rejections are not completed downloads. This narrow
        cleanup repairs stale rows from older versions without deleting a valid
        repeat-protection record created by some other successful job that happens
        to have the same basename and size.
        """
        job = self.get(int(job_id))
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """DELETE FROM downloaded_files
                   WHERE file_name_key = ? AND size = ? AND job_id = ?""",
                (file_history_key(job.remote_filename), int(job.size), int(job.id)),
            )
        return int(cur.rowcount or 0) > 0

    def file_active(self, remote_filename: str, size: int) -> bool:
        wanted_key = file_history_key(remote_filename)
        wanted_size = int(size)
        for job in self.active():
            if int(job.size) == wanted_size and file_history_key(job.remote_filename) == wanted_key:
                return True
        return False

    def unavailable_file(self, remote_filename: str, size: int) -> bool:
        return self.downloaded_before(remote_filename, size) or self.file_active(remote_filename, size)

    def download_history(self, limit: int = 200) -> list[DownloadHistoryEntry]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM downloaded_files ORDER BY downloaded_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [DownloadHistoryEntry(**dict(row)) for row in rows]

    def clear_download_history(self) -> int:
        """Clear persistent duplicate-download memory without deleting job history."""
        with self._lock, self._connect() as conn:
            count = int(conn.execute("SELECT COUNT(*) FROM downloaded_files").fetchone()[0])
            conn.execute("DELETE FROM downloaded_files")
        return count

    def add_ui_notification(
        self,
        message: str,
        level: str = "info",
        *,
        topic: str | None = None,
        episode_id: int | None = None,
    ) -> int:
        """Persist a transient UI notification so it survives page navigation."""
        clean_message = str(message or "").strip()
        if not clean_message:
            return 0
        clean_level = str(level or "info").strip().lower()
        if clean_level not in {"info", "success", "warning", "danger"}:
            clean_level = "info"
        clean_topic = str(topic or "").strip() or None
        created_at = utcnow()
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO ui_notifications(level, message, topic, episode_id, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    clean_level,
                    clean_message[:4000],
                    clean_topic[:128] if clean_topic else None,
                    int(episode_id) if episode_id is not None else None,
                    created_at,
                ),
            )
            notification_id = int(cur.lastrowid or 0)
            # Notifications are only a short-lived browser delivery queue. Keep
            # enough history to bridge navigation/restarts without growing the DB.
            cutoff = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
            conn.execute("DELETE FROM ui_notifications WHERE created_at < ?", (cutoff,))
        return notification_id

    def ui_notifications_after(self, after_id: int = 0, limit: int = 100) -> tuple[list[dict[str, Any]], int]:
        """Return UI notifications after a cursor plus the current latest id."""
        after_id = max(0, int(after_id or 0))
        limit = max(1, min(500, int(limit or 100)))
        with self._lock, self._connect() as conn:
            latest_row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM ui_notifications").fetchone()
            latest_id = int(latest_row[0] or 0) if latest_row else 0
            rows = conn.execute(
                """
                SELECT id, level, message, topic, episode_id, created_at
                FROM ui_notifications
                WHERE id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (after_id, limit),
            ).fetchall()
        return [dict(row) for row in rows], latest_id

    def history(self, limit: int = 500, offset: int = 0) -> list[JobHistoryEntry]:
        """Return finished jobs with download/import outcome for the History page."""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM jobs
                WHERE {_HISTORY_WHERE_SQL}
                ORDER BY updated_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (int(limit), max(0, int(offset))),
            ).fetchall()
        return [JobHistoryEntry(**dict(row)) for row in rows]

    def history_count(self) -> int:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) FROM jobs WHERE {_HISTORY_WHERE_SQL}"
            ).fetchone()
        return int(row[0]) if row else 0

    def history_page(self, limit: int = 25, page: int = 1) -> tuple[list[JobHistoryEntry], int, int]:
        """Return one clamped History page and its total using one DB connection."""
        limit = max(1, int(limit))
        requested_page = max(1, int(page))
        with self._lock, self._connect() as conn:
            count_row = conn.execute(
                f"SELECT COUNT(*) FROM jobs WHERE {_HISTORY_WHERE_SQL}"
            ).fetchone()
            total = int(count_row[0]) if count_row else 0
            total_pages = max(1, (total + limit - 1) // limit)
            resolved_page = min(requested_page, total_pages)
            offset = (resolved_page - 1) * limit
            rows = conn.execute(
                f"""
                SELECT *
                FROM jobs
                WHERE {_HISTORY_WHERE_SQL}
                ORDER BY updated_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
        return [JobHistoryEntry(**dict(row)) for row in rows], total, resolved_page
