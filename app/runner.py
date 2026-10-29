from __future__ import annotations

import asyncio
import os
import shutil
import stat

import httpx
from contextlib import suppress
from pathlib import Path, PurePosixPath

from .config import SettingsStore
from .jobs import JobStore
from .models import MissingEpisode, SearchCandidate
from .pathmap import resolve_mapping, strip_remote_path_root
from .queue_policy import (
    transfer_has_started,
    transfer_is_queued,
    transfer_queue_age_seconds,
    transfer_started_age_seconds,
    transfer_state,
)
from .release_name import build_import_filename, repair_planned_filename_quality
from .slskd import SlskdClient
from .models import is_movie_id
from .media import MediaClient, media_client, media_configured, settings_for_target
from .sonarr import SonarrClient, imported_file_replaced, language_names, quality_name, quality_resolution, quality_source_rank, release_languages_allowed


TERMINAL_FAILURE_WORDS = ("cancelled", "timedout", "errored", "rejected", "aborted", "failed")
TRANSIENT_TERMINAL_FAILURE_WORDS = ("timedout", "errored", "aborted")
TRANSFER_POLL_SECONDS = 5
TRANSFER_MISSING_AFTER_SEEN_SECONDS = 5 * 60
TRANSFER_MISSING_BEFORE_SEEN_SECONDS = 30
SLSKD_RESTART_RECOVERY_GRACE_SECONDS = 5 * 60
PEER_QUEUE_RETRY_SECONDS = 12 * 60
PEER_DAILY_LIMIT_RETRY_SECONDS = 24 * 60 * 60
PEER_WEEKLY_LIMIT_RETRY_SECONDS = 7 * 24 * 60 * 60
FILE_NOT_SHARED_BAN_SECONDS = 30 * 24 * 60 * 60
SONARR_IMPORT_ACTIVITY_POLL_SECONDS = 5
SONARR_IMPORT_ACTIVITY_CACHE_SECONDS = 2


class JobRunner:
    def __init__(self, settings: SettingsStore, jobs: JobStore, import_root: Path) -> None:
        self.settings_store = settings
        self.jobs = jobs
        self.import_root = import_root
        self.tasks: dict[int, asyncio.Task] = {}
        self.episode_locks: dict[int, asyncio.Lock] = {}
        # Staging copies are intentionally serialized. On HDD-backed storage,
        # multiple simultaneous full-file copies cause heavy seek contention and
        # make every preparation slower. Jobs that need a staging copy wait on
        # this single lane while locate/rename work can still proceed in parallel.
        self.copy_lock = asyncio.Lock()
        # Multiple completed downloads may be waiting for the single staging
        # lane at once. Cache Sonarr import activity briefly so all of those
        # waiters do not hammer /api/v3/command independently every few seconds.
        self.sonarr_activity_lock = asyncio.Lock()
        self._sonarr_activity_checked_at = 0.0
        self._sonarr_active_import: dict | None = None
        # Enqueue/cooldown changes for one peer share a lock. Once a remote
        # queue limit or repeated enqueue-failure cooldown is recorded, no later
        # runner can slip another enqueue through for that peer.
        self.peer_enqueue_locks: dict[str, asyncio.Lock] = {}
        # slskd can accept HTTP requests before its persisted transfer set has
        # fully reappeared after a container/service restart.  Any observed
        # control-plane outage opens a shared grace window so per-job pollers do
        # not mistake that restore interval for user removal.
        self.slskd_recovery_until = 0.0
        # Recovery windows are scoped to jobs that could already have existed in
        # slskd when the restart/outage happened. New jobs created while a
        # recovery window is open must not be delayed by restoration of older
        # persisted transfers.
        self.slskd_recovery_job_ids: set[int] = set()
        self.slskd_reconnect_pending = False
        self.slskd_reconnect_job_ids: set[int] = set()
        self.last_slskd_transfer_count: int | None = None

    @staticmethod
    def _path_within_root(path: Path, root: Path) -> bool:
        try:
            resolved_path = path.resolve(strict=False)
            resolved_root = root.resolve(strict=False)
        except OSError:
            return False
        return resolved_path == resolved_root or resolved_root in resolved_path.parents

    @classmethod
    def _path_within_any_root(cls, path: Path, roots: list[Path]) -> bool:
        return any(cls._path_within_root(path, root) for root in roots)

    def _local_data_roots(self, settings) -> list[Path]:
        roots = [self.import_root, Path(settings.slskd_downloads_path)]
        for mapping in getattr(settings, "folder_mappings", []) or []:
            value = str(mapping.get("soulseekarr_path") or "").strip()
            if value:
                roots.append(Path(value))
        return roots

    @classmethod
    def _is_safe_regular_file(cls, path: Path | None, expected_size: int, roots: list[Path]) -> bool:
        if path is None or not cls._path_within_any_root(path, roots):
            return False
        try:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode):
                return False
            return info.st_size == int(expected_size)
        except OSError:
            return False

    @staticmethod
    def _sonarr_import_label(command: dict | None) -> str:
        if not command:
            return ""
        body = command.get("body") if isinstance(command.get("body"), dict) else {}
        return str(command.get("name") or body.get("name") or "Sonarr import").strip()

    async def _active_sonarr_import(self, sonarr: SonarrClient) -> dict | None:
        """Return one active Sonarr import command, using a short shared cache.

        Failing to query Sonarr must not deadlock preparation. A later Sonarr
        validation/import call will still fail normally if the service is truly
        unavailable, but a transient command-list failure is not a reason to keep
        a completed media file stuck forever.
        """
        loop = asyncio.get_running_loop()
        now = loop.time()
        if now - self._sonarr_activity_checked_at < SONARR_IMPORT_ACTIVITY_CACHE_SECONDS:
            return self._sonarr_active_import

        async with self.sonarr_activity_lock:
            now = loop.time()
            if now - self._sonarr_activity_checked_at < SONARR_IMPORT_ACTIVITY_CACHE_SECONDS:
                return self._sonarr_active_import
            try:
                active = await sonarr.active_import_commands()
                current = active[0] if active else None
            except Exception:
                current = None
            self._sonarr_active_import = current
            self._sonarr_activity_checked_at = loop.time()
            return current

    async def _wait_for_sonarr_import_idle(
        self,
        job_id: int,
        sonarr: SonarrClient,
        *,
        prepare_step: str,
        resume_message: str,
    ) -> bool:
        """Pause a staging/copy job while Sonarr is already importing media."""
        waited = False
        while True:
            command = await self._active_sonarr_import(sonarr)
            if command is None:
                if waited:
                    self.jobs.update(
                        job_id,
                        status="copying",
                        prepare_step=prepare_step,
                        message=resume_message,
                    )
                return waited

            waited = True
            label = self._sonarr_import_label(command)
            detail = f" ({label})" if label else ""
            self.jobs.update(
                job_id,
                status="copying",
                prepare_step=prepare_step,
                message=f"Preparing: waiting — Sonarr is importing another file{detail}.",
            )
            await asyncio.sleep(SONARR_IMPORT_ACTIVITY_POLL_SECONDS)

    def start(self, job_id: int) -> None:
        task = self.tasks.get(job_id)
        if task and not task.done():
            return
        self.tasks[job_id] = asyncio.create_task(self.run(job_id))

    def repair_active_planned_filenames(self) -> int:
        """Repair persisted planned names from older releases before preparation.

        Only queued/downloading jobs are safe to rename here. Once preparation
        has started, changing the target could orphan an in-progress staging
        file. If the planned name changes, clear the cached Sonarr profile so
        the normal reconciliation path can score the corrected filename again.
        """
        repaired = 0
        for job in self.jobs.active():
            if job.status not in {"queued", "downloading"} or not job.prepare_target_name:
                continue
            corrected = repair_planned_filename_quality(
                job.remote_filename, job.prepare_target_name
            )
            if corrected == job.prepare_target_name:
                continue
            self.jobs.update(
                job.id,
                prepare_target_name=corrected,
                candidate_custom_format_score=None,
                candidate_resolution=None,
            )
            repaired += 1
        return repaired

    async def recover(self) -> None:
        # On Soulseekarr startup slskd may already answer HTTP while still
        # rebuilding its persisted transfer tree. Protect existing queued/live
        # jobs from both false cancellation and duplicate enqueue during that
        # settling period. The window only delays action when a persisted
        # transfer is temporarily absent from slskd's API.
        startup_jobs = self.jobs.active()
        if startup_jobs:
            self._note_slskd_recovery_window(
                job_ids={int(job.id) for job in startup_jobs}
            )

        # Planned filenames are persisted at enqueue time. Repair active jobs
        # created by older builds before any runner can prepare/import them.
        self.repair_active_planned_filenames()

        # Repair databases produced by older versions that could create several
        # active Soulseekarr rows for one real slskd transfer.  Do this before
        # starting runner tasks so only the keeper can attach to/process it.
        self.jobs.collapse_duplicate_active_transfers()

        # Older versions treated some remote-peer upload limits as terminal
        # failures. Revive those historical jobs on startup and put the peer on
        # the same persisted retry schedule used for newly observed limits.
        for job in self.jobs.retryable_peer_limit_failures():
            policy = self._peer_limit_retry_policy_from_text(job.message)
            if not policy:
                continue
            reason, retry_seconds = policy
            # Peer-limit rejections are not completed downloads. Older versions or
            # recovery edge cases may have left duplicate-download protection behind;
            # clear it so this exact file remains eligible for the scheduled retry.
            self.jobs.clear_owned_repeat_protection(job.id)
            cooldown = self.jobs.set_peer_cooldown(job.username, retry_seconds, reason)
            self.jobs.update(
                job.id,
                status="queued",
                transfer_id=None,
                message=self._peer_queue_wait_message(job.username, reason, cooldown),
            )

        for job in self.jobs.active():
            self.start(job.id)

    @staticmethod
    def _slskd_exception_text(transfer: dict) -> str:
        status_obj = transfer.get("status") if isinstance(transfer.get("status"), dict) else {}
        return str(
            transfer.get("exception")
            or transfer.get("Exception")
            or status_obj.get("exception")
            or status_obj.get("Exception")
            or ""
        ).strip()

    @classmethod
    def _file_not_shared_rejection(cls, transfer: dict, state: str) -> bool:
        """Return True only for a peer rejecting this exact file as no longer shared."""
        if "rejected" not in str(state or "").casefold():
            return False
        reason = cls._slskd_exception_text(transfer).strip().casefold().rstrip(". ")
        return reason == "file not shared" or reason.endswith(": file not shared")

    @staticmethod
    def _peer_limit_retry_policy_from_text(text: str) -> tuple[str, int] | None:
        """Return (reason, cooldown seconds) for retryable remote-peer limits.

        slskd uses rolling 24-hour and 7-day statistics for its daily/weekly
        upload limits. The remote peer does not expose the oldest contributing
        transfer timestamp, so Soulseekarr cannot know the exact instant the
        limit will clear. Waiting a full window from the observed rejection is a
        conservative upper-bound retry time and gives the UI a concrete
        ``retry after`` timestamp without hammering the peer.
        """
        normalized = str(text or "").strip().casefold().rstrip(". ")
        marker = "transfer rejected: "
        if marker in normalized:
            normalized = normalized.rsplit(marker, 1)[-1].strip()

        exact = {
            "too many megabytes": ("Too many megabytes", PEER_QUEUE_RETRY_SECONDS),
            "too many files": ("Too many files", PEER_QUEUE_RETRY_SECONDS),
            "too many files today": ("Too many files today", PEER_DAILY_LIMIT_RETRY_SECONDS),
            "too many megabytes today": ("Too many megabytes today", PEER_DAILY_LIMIT_RETRY_SECONDS),
            "too many failures today": ("Too many failures today", PEER_DAILY_LIMIT_RETRY_SECONDS),
            "too many failed transfers today": ("Too many failed transfers today", PEER_DAILY_LIMIT_RETRY_SECONDS),
            "too many files this week": ("Too many files this week", PEER_WEEKLY_LIMIT_RETRY_SECONDS),
            "too many megabytes this week": ("Too many megabytes this week", PEER_WEEKLY_LIMIT_RETRY_SECONDS),
            "too many failures this week": ("Too many failures this week", PEER_WEEKLY_LIMIT_RETRY_SECONDS),
            "too many failed transfers this week": ("Too many failed transfers this week", PEER_WEEKLY_LIMIT_RETRY_SECONDS),
        }
        return exact.get(normalized)

    @classmethod
    def _temporary_peer_queue_limit_reason_from_text(cls, text: str) -> str | None:
        policy = cls._peer_limit_retry_policy_from_text(text)
        return policy[0] if policy else None

    @classmethod
    def _temporary_peer_queue_limit_reason(cls, transfer: dict) -> str | None:
        return cls._temporary_peer_queue_limit_reason_from_text(cls._slskd_exception_text(transfer))

    @classmethod
    def _peer_limit_retry_policy(cls, transfer: dict) -> tuple[str, int] | None:
        return cls._peer_limit_retry_policy_from_text(cls._slskd_exception_text(transfer))

    @staticmethod
    def _transient_transfer_failure(state: str) -> bool:
        """Return True for terminal failures worth trying on another peer/file.

        User cancellations are intentionally excluded, and rejected transfers
        remain under the separate peer-limit policy because many rejection
        reasons are durable rather than transient.
        """
        lowered = str(state or "").casefold().replace(" ", "")
        return "completed" in lowered and any(
            marker in lowered for marker in TRANSIENT_TERMINAL_FAILURE_WORDS
        )

    @staticmethod
    def _peer_queue_wait_message(username: str, reason: str, cooldown: dict) -> str:
        until = str(cooldown.get("cooldownUntil") or "").replace("+00:00", "Z")
        return (
            f"Waiting to retry: remote peer limit ({reason}). "
            f"Next retry for Soulseek peer {username}: {until}. "
            "Soulseekarr will not enqueue new files from this peer before that timestamp. "
            "This is a remote Soulseek uploader limit, not a Sonarr rejection."
        )

    @staticmethod
    def _enqueue_http_500(exc: Exception) -> bool:
        return (
            isinstance(exc, httpx.HTTPStatusError)
            and exc.response is not None
            and int(exc.response.status_code) == 500
        )

    @staticmethod
    def _peer_failure_cooldown_message(username: str, cooldown: dict) -> str:
        until = str(cooldown.get("cooldownUntil") or "").replace("+00:00", "Z")
        reason = str(cooldown.get("reason") or "repeated enqueue failures")
        return (
            f"Soulseek peer {username} is on enqueue-failure cooldown until {until} "
            f"after {reason}. This file will not be retried from this peer; new searches "
            "will skip the peer until the cooldown expires."
        )

    def _record_enqueue_http_500(
        self, username: str
    ) -> tuple[dict, int, int, dict | None]:
        """Record one slskd HTTP 500 for a peer and start cooldown at the threshold."""
        settings = self.settings_store.load()
        limit = max(1, int(getattr(settings, "peer_failure_strike_limit", 3) or 3))
        cooldown_days = max(1, int(getattr(settings, "peer_failure_cooldown_days", 7) or 7))
        strike = self.jobs.record_peer_failure_strike(
            username, "slskd HTTP 500 while enqueueing a download"
        )
        count = max(1, int(strike.get("strikes") or 1))
        if count < limit:
            return strike, limit, cooldown_days, None

        reason = f"{count} slskd HTTP 500 enqueue failures"
        cooldown = self.jobs.set_peer_cooldown(
            username,
            cooldown_days * 24 * 60 * 60,
            reason,
            kind="enqueue_failure",
        )
        # The cooldown is a completed strike cycle. After it expires the peer
        # gets a fresh threshold instead of being re-cooled on its first error.
        self.jobs.clear_peer_failure_strikes(username)
        return strike, limit, cooldown_days, cooldown

    def _restorable_active_job_ids(self) -> set[int]:
        """Return active jobs that could already own a transfer in slskd.

        A newly-created queued job has not been accepted by slskd yet and must
        therefore never be held behind a restore grace window. Once enqueue has
        succeeded Soulseekarr moves the job to ``downloading``; a persisted
        transfer id is also direct evidence that the job may need reattachment.
        """
        return {
            int(job.id)
            for job in self.jobs.active()
            if job.status == "downloading" or bool(job.transfer_id)
        }

    def _note_slskd_recovery_window(
        self, *, reset: bool = False, job_ids: set[int] | None = None
    ) -> None:
        """Open one bounded, job-scoped restore window.

        slskd can briefly expose an incomplete transfer tree after restart. The
        deadline must remain bounded, but the protection must also apply only to
        transfers that predate the restart/outage. Otherwise a brand-new job
        created after a Soulseekarr update waits several minutes for persisted
        transfers that it could never have belonged to.
        """
        try:
            loop = asyncio.get_running_loop()
            now = loop.time()
            if not reset and now < float(self.slskd_recovery_until or 0.0):
                return

            protected = (
                {int(job_id) for job_id in job_ids}
                if job_ids is not None
                else self._restorable_active_job_ids()
            )
            if not protected:
                if reset:
                    self.slskd_recovery_until = 0.0
                    self.slskd_recovery_job_ids.clear()
                return

            self.slskd_recovery_job_ids = protected
            self.slskd_recovery_until = now + SLSKD_RESTART_RECOVERY_GRACE_SECONDS
        except RuntimeError:
            pass

    def _note_slskd_unavailable(self) -> None:
        # Snapshot the jobs that could already exist in slskd when the outage is
        # first observed. New queued jobs created during the outage are excluded.
        # If this happens during startup recovery, keep those pre-startup ids too.
        if not self.slskd_reconnect_pending:
            protected = self._restorable_active_job_ids()
            try:
                if asyncio.get_running_loop().time() < float(self.slskd_recovery_until or 0.0):
                    protected.update(self.slskd_recovery_job_ids)
            except RuntimeError:
                pass
            self.slskd_reconnect_job_ids = protected
        self.slskd_reconnect_pending = True

    def _note_slskd_available(self) -> None:
        if self.slskd_reconnect_pending:
            self.slskd_reconnect_pending = False
            protected = set(self.slskd_reconnect_job_ids)
            self.slskd_reconnect_job_ids.clear()
            self._note_slskd_recovery_window(reset=True, job_ids=protected)

    def _slskd_recovery_window_active(self, job_id: int | None = None) -> bool:
        try:
            active = asyncio.get_running_loop().time() < float(self.slskd_recovery_until or 0.0)
        except RuntimeError:
            return False
        if not active:
            self.slskd_recovery_job_ids.clear()
            return False
        if job_id is None:
            return bool(self.slskd_recovery_job_ids)
        return int(job_id) in self.slskd_recovery_job_ids

    def _slskd_recovery_wait_message(self) -> str:
        try:
            remaining = max(0, int(float(self.slskd_recovery_until or 0.0) - asyncio.get_running_loop().time() + 0.999))
        except RuntimeError:
            remaining = 0
        minutes, seconds = divmod(remaining, 60)
        if minutes:
            countdown = f"{minutes}m {seconds:02d}s"
        else:
            countdown = f"{seconds}s"
        return (
            "Waiting for slskd to finish restoring persisted transfers after restart. "
            f"Restore timeout: {countdown}."
        )

    async def _find_slskd_transfer(self, slskd: SlskdClient, job) -> dict | None:
        """Find a live transfer using persisted id first, then tolerant path identity."""
        transfer_id = str(getattr(job, "transfer_id", None) or "").strip()
        by_id = getattr(slskd, "download_by_id", None)
        if transfer_id and callable(by_id):
            transfer = await by_id(job.username, transfer_id)
            if isinstance(transfer, dict):
                return transfer
        return await slskd.find_download(job.username, job.remote_filename, job.size)

    def _peer_enqueue_lock(self, username: str) -> asyncio.Lock:
        key = str(username or "").strip().casefold()
        lock = self.peer_enqueue_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self.peer_enqueue_locks[key] = lock
        return lock

    async def _wait_for_peer_cooldown(self, job_id: int, username: str) -> bool:
        while True:
            try:
                current = self.jobs.get(job_id)
            except KeyError:
                return False
            if current.status not in {"queued", "downloading"}:
                return False
            cooldown = self.jobs.peer_cooldown(username)
            if cooldown is None:
                return True
            if str(cooldown.get("kind") or "") == "enqueue_failure":
                self.jobs.update(
                    job_id,
                    status="failed",
                    transfer_id=None,
                    message=self._peer_failure_cooldown_message(username, cooldown),
                )
                return False
            self.jobs.update(
                job_id,
                status="queued",
                message=self._peer_queue_wait_message(
                    username, str(cooldown.get("reason") or "remote queue limit"), cooldown
                ),
            )
            await asyncio.sleep(min(60, max(1, int(cooldown.get("remainingSeconds") or 1))))

    async def _enqueue_when_peer_available(
        self, job_id: int, candidate: SearchCandidate, slskd: SlskdClient
    ) -> bool:
        """Wait out a per-peer queue cooldown, then enqueue exactly once safely."""
        peer_lock = self._peer_enqueue_lock(candidate.username)
        while True:
            if not await self._wait_for_peer_cooldown(job_id, candidate.username):
                return False
            async with peer_lock:
                # Recheck after taking the lock: another runner may have observed
                # a queue-limit rejection while this task was waiting.
                if self.jobs.peer_cooldown(candidate.username) is not None:
                    continue
                try:
                    existing = await slskd.find_download(
                        candidate.username, candidate.filename, candidate.size
                    )
                except Exception as exc:
                    self.jobs.update(
                        job_id,
                        status="queued",
                        message=f"Waiting for slskd to reconnect before retry: {type(exc).__name__}: {exc}",
                    )
                    existing = ...

                if existing is ...:
                    pass
                elif existing is not None:
                    existing_state = transfer_state(existing)
                    retry_policy = self._peer_limit_retry_policy(existing)
                    if "completed" in existing_state.casefold() and retry_policy:
                        reason, retry_seconds = retry_policy
                        self.jobs.clear_owned_repeat_protection(job_id)
                        cooldown = self.jobs.set_peer_cooldown(
                            candidate.username, retry_seconds, reason
                        )
                        self.jobs.update(
                            job_id,
                            status="queued",
                            transfer_id=None,
                            message=self._peer_queue_wait_message(
                                candidate.username, reason, cooldown
                            ),
                        )
                        transfer_id = str(existing.get("id") or existing.get("Id") or "").strip()
                        if transfer_id:
                            try:
                                await slskd.cancel_download(
                                    candidate.username, transfer_id, remove=True
                                )
                                existing = None
                            except Exception:
                                # Keep the rejected record; the next pass after
                                # cooldown will attempt removal again.
                                pass
                        # Re-enter the outer loop so the newly-created cooldown is
                        # honored before any enqueue attempt.
                        continue
                    else:
                        self.jobs.clear_peer_failure_strikes(candidate.username)
                        return True

                if existing is None:
                    try:
                        await slskd.enqueue(candidate)
                    except Exception as exc:
                        policy = self._peer_limit_retry_policy_from_text(str(exc))
                        if policy:
                            reason, retry_seconds = policy
                            self.jobs.clear_owned_repeat_protection(job_id)
                            cooldown = self.jobs.set_peer_cooldown(
                                candidate.username, retry_seconds, reason
                            )
                            self.jobs.update(
                                job_id,
                                status="queued",
                                transfer_id=None,
                                message=self._peer_queue_wait_message(
                                    candidate.username, reason, cooldown
                                ),
                            )
                        elif self._enqueue_http_500(exc):
                            strike, limit, cooldown_days, cooldown = self._record_enqueue_http_500(
                                candidate.username
                            )
                            strike_count = max(1, int(strike.get("strikes") or 1))
                            if cooldown is not None:
                                self.jobs.update(
                                    job_id,
                                    status="failed",
                                    transfer_id=None,
                                    message=(
                                        f"Soulseek peer {candidate.username} reached "
                                        f"{strike_count}/{limit} enqueue-failure strikes after repeated "
                                        f"slskd HTTP 500 responses. "
                                        + self._peer_failure_cooldown_message(
                                            candidate.username, cooldown
                                        )
                                    ),
                                )
                                return False
                            self.jobs.update(
                                job_id,
                                status="queued",
                                message=(
                                    f"Could not enqueue in slskd: HTTP 500. Soulseek peer "
                                    f"{candidate.username} enqueue-failure strike "
                                    f"{strike_count}/{limit}; will retry. If the peer reaches "
                                    f"{limit} strikes it will be skipped for {cooldown_days} day(s)."
                                ),
                            )
                        else:
                            self.jobs.update(
                                job_id,
                                status="queued",
                                message=f"Could not enqueue in slskd yet; will retry: {type(exc).__name__}: {exc}",
                            )
                    else:
                        self.jobs.clear_peer_failure_strikes(candidate.username)
                        self.jobs.update(job_id, status="downloading", message="Queued in slskd.")
                        return True
            await asyncio.sleep(TRANSFER_POLL_SECONDS)

    async def reconcile_slskd_downloads(self) -> int:
        """Reattach persisted jobs to transfers that survived an slskd restart.

        slskd persists/resumes its own transfers independently from Soulseekarr.
        A short slskd outage must therefore never be allowed to permanently
        orphan a Soulseekarr job.  This reconciliation pass scans slskd once,
        matches transfers against Soulseekarr's persistent SQLite jobs using the
        same normalized Windows/Unix transfer identity as normal polling, and
        revives only jobs that were previously marked failed/cancelled while the
        matching transfer is demonstrably still alive (or completed successfully).

        Completed failures are deliberately ignored.  Explicit Soulseekarr
        cancellation removes the slskd transfer before marking the job cancelled,
        so a cancelled job can only be revived here when slskd still proves that
        the transfer exists.
        """
        settings = self.settings_store.load()
        if not settings.slskd_url or not settings.slskd_api_key:
            return 0

        slskd = SlskdClient(settings.slskd_url, settings.slskd_api_key, timeout=15)
        try:
            groups = await slskd.all_downloads()
        except Exception:
            self._note_slskd_unavailable()
            raise
        self._note_slskd_available()
        flattened = slskd.flatten_downloads(groups)
        current_count = len(flattened)
        previous_count = self.last_slskd_transfer_count
        if (
            previous_count is not None
            and previous_count >= 4
            and current_count <= max(1, previous_count // 4)
            and self.jobs.active()
        ):
            # A sudden successful 200 response containing only a tiny fraction
            # of the previous transfer set is another common restart signature.
            # Protect jobs even when the short HTTP outage happened between polls.
            self._note_slskd_recovery_window(
                job_ids=self._restorable_active_job_ids()
            )
        self.last_slskd_transfer_count = current_count
        revived = 0

        for transfer in flattened:
            username = str(transfer.get("username") or transfer.get("Username") or "").strip()
            filename = str(transfer.get("filename") or transfer.get("Filename") or "").strip()
            try:
                size = int(transfer.get("size", transfer.get("Size")) or 0)
            except (TypeError, ValueError):
                size = 0
            if not username or not filename or size <= 0:
                continue

            state = transfer_state(transfer)
            lowered = state.casefold()
            ended_at = transfer.get("endedAt", transfer.get("EndedAt"))
            completed_successfully = "completed" in lowered and "succeeded" in lowered
            terminal_failure = (
                any(word in lowered for word in TERMINAL_FAILURE_WORDS)
                and ("completed" in lowered or bool(ended_at))
            )
            if terminal_failure:
                continue

            job = self.jobs.latest_for_transfer(username, filename, size)
            if job is None:
                continue

            transfer_id = str(transfer.get("id") or transfer.get("Id") or "").strip() or None

            if job.status in {"failed", "cancelled"}:
                # A live or successfully-completed matching transfer is stronger
                # evidence than the old terminal status, which may have been
                # caused by an slskd restart/control-plane outage.
                if not (completed_successfully or not ended_at):
                    continue
                self.jobs.update(
                    job.id,
                    status="downloading",
                    transfer_id=transfer_id,
                    message=(
                        f"Reattached to slskd after restart: {state or 'transfer found'}."
                    ),
                )
                revived += 1
                self.start(job.id)
                continue

            if job.status in {"queued", "downloading"}:
                # Keep the persistent transfer id fresh because slskd may assign a
                # new id while restoring transfers after its own restart.
                if transfer_id and transfer_id != job.transfer_id:
                    self.jobs.update(job.id, transfer_id=transfer_id, message=state or job.message)
                self.start(job.id)

        return revived

    async def reconcile_sonarr_superseded_downloads(self) -> int:
        """Remove queued/downloading releases that Sonarr has already surpassed.

        Sonarr can acquire a better file independently while Soulseekarr still has
        slower alternatives queued or transferring.  Compare Sonarr's installed
        EpisodeFile against each active Soulseek release using the same three
        dimensions used for upgrade decisions. The installed file must be no
        worse in Custom Format score, resolution, and source quality. Equality on
        all axes is enough to remove the redundant Soulseek transfer because
        Sonarr already has an equivalent file. Unknown profiles are left untouched.
        """
        settings = self.settings_store.load()
        if not (media_configured(settings) and settings.slskd_url and settings.slskd_api_key):
            return 0

        sonarr = media_client(settings, SonarrClient)
        active = [
            job for job in self.jobs.active()
            if job.status in {"queued", "downloading"}
        ]
        by_episode: dict[int, list] = {}
        for job in active:
            by_episode.setdefault(int(job.episode_id), []).append(job)

        removed = 0
        for episode_id, episode_jobs in by_episode.items():
            try:
                installed = None
                detailed_installed = getattr(sonarr, "installed_episode_quality_profile", None)
                if callable(detailed_installed):
                    detailed_value = await detailed_installed(episode_id)
                    if isinstance(detailed_value, tuple) and len(detailed_value) == 3:
                        installed = (int(detailed_value[0]), int(detailed_value[1]), int(detailed_value[2]))
                if installed is None:
                    legacy_installed = await sonarr.installed_episode_profile(episode_id)
                    if legacy_installed is not None:
                        installed = (int(legacy_installed[0]), int(legacy_installed[1]), 0)
            except Exception:
                continue
            if installed is None:
                continue
            installed_score, installed_resolution, installed_source_rank = installed

            for job in episode_jobs:
                candidate: tuple[int, int, int] | None = None
                try:
                    detailed_candidate = getattr(sonarr, "candidate_release_quality_profile_for_episode_id", None)
                    if callable(detailed_candidate):
                        detailed_value = await detailed_candidate(
                            episode_id, job.prepare_target_name or job.remote_filename
                        )
                        if isinstance(detailed_value, tuple) and len(detailed_value) == 3:
                            candidate = (
                                int(detailed_value[0]), int(detailed_value[1]), int(detailed_value[2])
                            )
                    if candidate is None:
                        legacy_candidate = await sonarr.candidate_release_profile_for_episode_id(
                            episode_id, job.prepare_target_name or job.remote_filename
                        )
                        if legacy_candidate is not None:
                            candidate = (int(legacy_candidate[0]), int(legacy_candidate[1]), 0)
                except Exception:
                    candidate = None

                if candidate is None and (
                    job.candidate_custom_format_score is not None
                    and job.candidate_resolution is not None
                    and int(job.candidate_resolution) > 0
                ):
                    candidate = (
                        int(job.candidate_custom_format_score),
                        int(job.candidate_resolution),
                        0,
                    )
                elif candidate is not None:
                    try:
                        self.jobs.update(
                            job.id,
                            candidate_custom_format_score=int(candidate[0]),
                            candidate_resolution=int(candidate[1]),
                        )
                    except Exception:
                        pass
                if candidate is None:
                    continue
                candidate_score, candidate_resolution, candidate_source_rank = candidate

                dominates_or_equals = (
                    installed_score >= candidate_score
                    and installed_resolution >= candidate_resolution
                    and installed_source_rank >= candidate_source_rank
                )
                if not dominates_or_equals:
                    continue

                reason = (
                    "Removed automatically because Sonarr already has an equal or better episode file "
                    f"(CF score {installed_score}, {installed_resolution}p) than this "
                    f"Soulseek candidate (CF score {candidate_score}, {candidate_resolution}p)."
                )

                # Once Sonarr proves this release is no longer an upgrade, never
                # allow the local watcher to carry it on into preparation/import.
                # slskd cancellation can still fail transiently (for example while
                # its API is restarting), so stop the runner first and leave the
                # job active with a visible pending-removal message. The periodic
                # reconciliation pass will retry cancellation until it succeeds.
                task = self.tasks.get(job.id)
                if task and not task.done() and task is not asyncio.current_task():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task

                self.jobs.update(
                    job.id,
                    message=(
                        "No longer an upgrade; cancelling Soulseek transfer. "
                        f"Sonarr has CF score {installed_score}, {installed_resolution}p; "
                        f"candidate is CF score {candidate_score}, {candidate_resolution}p."
                    ),
                )
                try:
                    await self.cancel(job.id, reason=reason)
                except Exception as exc:
                    self.jobs.update(
                        job.id,
                        message=(
                            "No longer an upgrade; automatic removal pending because "
                            f"slskd cancellation failed: {type(exc).__name__}: {exc}"
                        ),
                    )
                    continue
                removed += 1

        return removed

    async def cancel(self, job_id: int, *, reason: str | None = None) -> dict[str, object]:
        """Remove one active Soulseekarr download and its disposable files.

        The slskd transfer is cancelled/removed first so hiding the Soulseekarr
        row can never leave an untracked transfer running in the background.
        Afterwards the local runner task is stopped, any completed/staged files
        are removed, slskd's partial file is deleted when remote file management
        permits it, and empty temporary directories are pruned.
        """
        job = self.jobs.get(job_id)
        if job.status not in self.jobs.ACTIVE_STATUSES:
            raise ValueError("job is no longer active")
        if job.status == "importing":
            raise ValueError("job is already being imported by Sonarr")

        settings = self.settings_store.load()
        slskd = SlskdClient(settings.slskd_url, settings.slskd_api_key, timeout=20)

        # Prefer the persisted transfer id. Active jobs normally already know it,
        # and cancelling directly avoids an unnecessary full slskd transfer-list
        # request exactly when we most want a redundant download stopped quickly.
        # If the id is stale (for example after an slskd restart), fall back to
        # matching the live transfer by peer/path/size.
        transfer = None
        transfer_was_present = False
        persisted_transfer_id = str(job.transfer_id or "").strip()
        persisted_cancel_error: Exception | None = None
        if persisted_transfer_id:
            try:
                await slskd.cancel_download(job.username, persisted_transfer_id, remove=True)
                transfer_was_present = True
            except Exception as exc:
                persisted_cancel_error = exc

        if not transfer_was_present:
            try:
                transfer = await slskd.find_download(job.username, job.remote_filename, job.size)
            except Exception:
                if persisted_cancel_error is not None:
                    raise persisted_cancel_error
                raise
            transfer_was_present = transfer is not None
            if transfer is not None:
                transfer_id = str(transfer.get("id") or transfer.get("Id") or "").strip()
                if not transfer_id:
                    raise RuntimeError("slskd transfer has no id and could not be removed")
                await slskd.cancel_download(job.username, transfer_id, remove=True)
            elif persisted_cancel_error is not None:
                # The persisted id failed and no replacement transfer could be
                # found. Treat the transfer as already gone and continue local
                # cleanup rather than leaving a redundant job active forever.
                transfer_was_present = False

        task = self.tasks.get(job_id)
        if task and not task.done() and task is not asyncio.current_task():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        # A click can race the initial enqueue POST: the first lookup may happen
        # just before slskd creates its transfer record. After stopping the local
        # enqueue task, briefly recheck for a late record so it cannot become an
        # orphaned slskd download.
        if not transfer_was_present and job.status == "queued":
            for attempt in range(4):
                late_transfer = await slskd.find_download(job.username, job.remote_filename, job.size)
                if late_transfer is not None:
                    transfer_id = str(late_transfer.get("id") or late_transfer.get("Id") or "").strip()
                    if not transfer_id:
                        raise RuntimeError("late slskd transfer has no id and could not be removed")
                    await slskd.cancel_download(job.username, transfer_id, remove=True)
                    break
                if attempt < 3:
                    await asyncio.sleep(0.5)

        # Refresh after stopping the runner; it may have recorded source/staging
        # paths just before the cancellation was processed.
        job = self.jobs.get(job_id)
        cleanup_paths: set[Path] = set()
        for value in (job.source_file, job.import_file):
            if value:
                cleanup_paths.add(Path(value))

        protected_roots = {self.import_root, Path(settings.slskd_downloads_path)}
        for mapping in settings.folder_mappings:
            local_root = str(mapping.get("soulseekarr_path") or "").strip()
            if local_root:
                protected_roots.add(Path(local_root))

        # A completed file can exist before _prepare_completed_download() has had
        # a chance to persist source_file. Find it immediately without waiting.
        local_completed = self._find_local_file_now(
            Path(settings.slskd_downloads_path),
            job.remote_filename,
            job.size,
        )
        if local_completed is not None:
            cleanup_paths.add(local_completed)

        removed, errors = await self._cleanup_download_files(
            cleanup_paths, protected_roots=protected_roots
        )

        # The staging directory is owned exclusively by this job. This also
        # catches a partially-created staging file if cancellation landed while
        # a copy was in progress.
        staging_dir = self.import_root / f"soulseekarr-{job_id}"
        if staging_dir.exists():
            try:
                await asyncio.to_thread(shutil.rmtree, staging_dir)
                removed += 1
            except FileNotFoundError:
                pass
            except OSError as exc:
                errors.append(f"{staging_dir}: {exc}")

        # slskd keeps resumable partial files below its separate incomplete
        # directory. The Files API is the only reliable way to reach that path
        # when Soulseekarr mounts only the completed-download directory.
        try:
            incomplete = await slskd.find_incomplete_file(job.username, job.remote_filename)
            if incomplete:
                await slskd.delete_incomplete_file_and_empty_parents(incomplete)
                removed += 1
        except Exception as exc:
            errors.append(f"slskd incomplete-file cleanup: {exc}")

        # If slskd had already moved the file to its completed directory but the
        # Soulseekarr mount could not see/unlink it, use the remote file API as a
        # fallback. Existing runner cleanup uses the same fallback.
        try:
            completed = await slskd.find_downloaded_file(job.remote_filename, job.size)
            if completed:
                local_still_exists = False
                if local_completed is not None:
                    try:
                        local_still_exists = local_completed.exists()
                    except OSError:
                        local_still_exists = True
                if local_completed is None or local_still_exists:
                    removed += 1 + await slskd.delete_downloaded_file_and_empty_parents(completed)
        except Exception as exc:
            errors.append(f"slskd completed-file cleanup: {exc}")

        message = reason or "Removed by user from Soulseekarr and slskd."
        if reason and not message.endswith("."):
            message += "."
        if errors:
            message += f" Cleanup removed {removed} item(s), but failed for: {'; '.join(errors)}"
        elif removed:
            message += f" Cleanup removed {removed} temporary item(s) and empty folders."
        else:
            message += " No temporary file remained to clean."
        self.jobs.update(job_id, status="cancelled", message=message)

        # Removing one candidate may unblock another completed candidate for the
        # same episode. Restart ready evaluators asynchronously rather than making
        # the DELETE request wait for a potentially long Sonarr import.
        for item in self.jobs.episode_jobs(job.episode_id, ("ready", "evaluating")):
            if item.id != job_id:
                self.start(item.id)

        return {"removed": removed, "errors": errors, "message": message}

    async def run(self, job_id: int) -> None:
        try:
            await self._run(job_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.jobs.update(job_id, status="failed", message=f"{type(exc).__name__}: {exc}")

    async def _run(self, job_id: int) -> None:
        job = self.jobs.get(job_id)
        settings = self.settings_store.load()
        sonarr = (
            media_client(settings, SonarrClient)
            if media_configured(settings)
            else None
        )
        slskd = SlskdClient(settings.slskd_url, settings.slskd_api_key)

        # Preparation is checkpointed in SQLite. A container restart while a
        # completed file is being located/renamed/staged must resume preparation
        # directly; going back through slskd transfer discovery is unsafe because
        # completed transfer records may already have been cleaned from slskd and
        # the source file may already have been renamed.
        if job.status == "copying":
            episode: MissingEpisode | None = None
            try:
                missing = await sonarr.missing_episodes(False)
                episode = next((ep for ep in missing if ep.id == job.episode_id), None)
            except Exception:
                pass
            prepared = await self._prepare_completed_download(
                job_id=job_id, episode=episode, slskd=slskd, settings=settings
            )
            if prepared:
                await self._process_episode_batch(job.episode_id, sonarr, slskd, settings)
            return

        # Fully prepared jobs survive restarts too. Re-enter the episode batch
        # evaluator instead of redownloading or rebuilding the staging copy.
        if job.status in {"ready", "evaluating", "importing"} and job.sonarr_file and job.import_file:
            if job.status != "ready":
                self.jobs.update(
                    job_id,
                    status="ready",
                    prepare_step="prepared",
                    message="Recovered prepared download; ready for immediate Sonarr evaluation.",
                )
            await self._process_episode_batch(job.episode_id, sonarr, slskd, settings)
            return

        episode: MissingEpisode | None = None
        try:
            missing = await sonarr.missing_episodes(False)
            episode = next((ep for ep in missing if ep.id == job.episode_id), None)
        except Exception:
            # A running download should not be abandoned because a preliminary
            # Sonarr lookup failed. The direct episode endpoint is queried again
            # during batch evaluation.
            pass

        candidate = SearchCandidate(username=job.username, filename=job.remote_filename, size=job.size)
        initial_lookup_ok = True
        try:
            transfer = await self._find_slskd_transfer(slskd, job)
        except Exception as exc:
            self._note_slskd_unavailable()
            # slskd can be briefly unavailable while its container/service is
            # restarting. Do not fail the persistent Soulseekarr job, and more
            # importantly do not enqueue a duplicate when we could not verify
            # whether the original transfer already exists.
            transfer = None
            initial_lookup_ok = False
            self.jobs.update(
                job_id,
                message=f"Waiting for slskd to reconnect: {type(exc).__name__}: {exc}",
            )
        else:
            self._note_slskd_available()
        waited_for_restore = False
        if transfer is None and job.status == "queued" and initial_lookup_ok:
            if self._slskd_recovery_window_active(job_id):
                # slskd can accept requests before its persisted queue has fully
                # restored. Do not create a duplicate while that old transfer may
                # still be about to reappear. The poll loop below will reattach
                # it. If it never returns, the bounded restore timeout marks the old job removed.
                waited_for_restore = True
                self.jobs.update(
                    job_id,
                    status="queued",
                    message=self._slskd_recovery_wait_message(),
                )
            else:
                if not await self._enqueue_when_peer_available(job_id, candidate, slskd):
                    return
                transfer = await self._find_slskd_transfer(slskd, self.jobs.get(job_id))

        loop = asyncio.get_running_loop()
        # Queue waiting has its own policy: queued transfers are not subject to
        # the normal download timeout. The hourly queue-position health check can
        # eventually remove a persistently non-improving queue entry, but it does
        # not launch a competing search; only the main automatic round-robin does.
        # The normal download timeout therefore begins only once slskd actually
        # starts/initializes the transfer instead of expiring a healthy remote queue.
        download_deadline: float | None = None
        transfer_seen = transfer is not None or bool(job.transfer_id)
        missing_since: float | None = None

        while True:
            try:
                current_job = self.jobs.get(job_id)
                transfer = await self._find_slskd_transfer(slskd, current_job)
            except Exception as exc:
                self._note_slskd_unavailable()
                # A control-plane outage is not evidence that the Soulseek
                # transfer failed. Keep the job active, reset the missing timer,
                # and reattach automatically when slskd comes back.
                missing_since = None
                self.jobs.update(
                    job_id,
                    message=f"Waiting for slskd to reconnect: {type(exc).__name__}: {exc}",
                )
                await asyncio.sleep(TRANSFER_POLL_SECONDS)
                continue
            self._note_slskd_available()
            if transfer:
                transfer_seen = True
                waited_for_restore = False
                missing_since = None
                status_obj = transfer.get("status") if isinstance(transfer.get("status"), dict) else {}
                state = str(transfer.get("state") or status_obj.get("state") or "")
                transfer_id = str(transfer.get("id") or "") or None
                self.jobs.update(job_id, status="downloading", transfer_id=transfer_id, message=state or "Downloading")
                lowered = state.lower()
                if "completed" in lowered and "succeeded" in lowered:
                    self.jobs.mark_downloaded(job_id)
                    break
                if "completed" in lowered and any(word in lowered for word in TERMINAL_FAILURE_WORDS):
                    retry_policy = self._peer_limit_retry_policy(transfer)
                    if retry_policy:
                        temporary_reason, retry_seconds = retry_policy
                        # A remote uploader limit is a retryable rejection, not a
                        # successful download. Ensure it can never remain in the
                        # downloaded-file repeat-protection block list.
                        self.jobs.clear_owned_repeat_protection(job_id)
                        async with self._peer_enqueue_lock(job.username):
                            cooldown = self.jobs.set_peer_cooldown(
                                job.username, retry_seconds, temporary_reason
                            )
                            rejected_id = str(transfer.get("id") or transfer.get("Id") or "").strip()
                            if rejected_id:
                                try:
                                    await slskd.cancel_download(job.username, rejected_id, remove=True)
                                except Exception:
                                    # A stale rejected record is harmless; after
                                    # the cooldown the enqueue helper rechecks it.
                                    pass
                            self.jobs.update(
                                job_id,
                                status="queued",
                                transfer_id=None,
                                message=self._peer_queue_wait_message(
                                    job.username, temporary_reason, cooldown
                                ),
                            )
                        if not await self._enqueue_when_peer_available(job_id, candidate, slskd):
                            return
                        transfer_seen = False
                        missing_since = None
                        download_deadline = None
                        continue

                    failure_message = self._slskd_transfer_failure_message(transfer, state)
                    if self._file_not_shared_rejection(transfer, state):
                        # The peer explicitly says this exact shared path is no longer
                        # available. Remember the user+path+size pair so automatic and
                        # manual searches do not select the same dead result on every
                        # pass. Other files from the peer remain fully eligible.
                        self.jobs.clear_owned_repeat_protection(job_id)
                        ban = self.jobs.set_file_rejection_ban(
                            job.username,
                            job.remote_filename,
                            job.size,
                            FILE_NOT_SHARED_BAN_SECONDS,
                            "Transfer rejected: File not shared",
                        )
                        until = str(ban.get("cooldownUntil") or "").replace("+00:00", "Z")
                        failure_message += (
                            f" This exact file from {job.username} is blocked until {until} "
                            "so future searches can choose a different result."
                        )

                    if self._transient_transfer_failure(state):
                        # Timed-out/errored/aborted transfers are terminal for
                        # this transfer attempt, but they must not launch an
                        # out-of-band Soulseek search. Remove the dead slskd row
                        # and let the normal automatic TV-show -> season ->
                        # episode round-robin decide when this episode is due
                        # again. This keeps *all* background searches on one
                        # persisted rotation.
                        self.jobs.clear_owned_repeat_protection(job_id)
                        failed_id = str(transfer.get("id") or transfer.get("Id") or "").strip()
                        if failed_id:
                            try:
                                await slskd.cancel_download(job.username, failed_id, remove=True)
                            except Exception:
                                pass

                        self.jobs.update(
                            job_id,
                            status="failed",
                            message=(
                                f"{failure_message} No immediate alternate search was started; "
                                "the episode will be reconsidered by the normal automatic "
                                "round-robin when its repeat-search cooldown is due."
                            ),
                        )
                        await self._process_episode_batch(job.episode_id, sonarr, slskd, settings)
                        return

                    self.jobs.update(
                        job_id,
                        status="failed",
                        message=failure_message,
                    )
                    await self._process_episode_batch(job.episode_id, sonarr, slskd, settings)
                    return

                if transfer_is_queued(transfer):
                    # Queue state is authoritative even for a resumed partial
                    # transfer that still has historical startedAt/byte progress.
                    # Do not run the active-download timeout while waiting for the
                    # peer to resume us. Scheduler maintenance tracks queue health.
                    download_deadline = None
                elif transfer_has_started(transfer):
                    if download_deadline is None:
                        timeout_seconds = max(60, int(settings.download_timeout_minutes) * 60)
                        already_started = transfer_started_age_seconds(transfer) or 0.0
                        download_deadline = loop.time() + max(0.0, timeout_seconds - already_started)
                    if loop.time() >= download_deadline:
                        self.jobs.update(job_id, status="failed", message="Download timed out after transfer started.")
                        await self._process_episode_batch(job.episode_id, sonarr, slskd, settings)
                        return
            else:
                now = loop.time()
                if self._slskd_recovery_window_active(job_id):
                    # slskd may temporarily return a valid but incomplete
                    # transfer list while restoring persisted state. Do not let
                    # that transient absence advance the removal timer or enqueue
                    # a second copy of a queued transfer.
                    waited_for_restore = True
                    missing_since = None
                    self.jobs.update(
                        job_id,
                        message=self._slskd_recovery_wait_message(),
                    )
                    await asyncio.sleep(TRANSFER_POLL_SECONDS)
                    continue

                if waited_for_restore:
                    # The bounded restore timeout expired and this persisted
                    # transfer still did not reappear. Treat that as a removed
                    # transfer rather than silently resurrecting the old candidate.
                    # The episode can immediately return to Missing Episodes and
                    # the normal automatic round-robin can decide when to search it.
                    completed_path = None
                    try:
                        completed_path = await slskd.find_downloaded_file(job.remote_filename, job.size)
                    except Exception:
                        completed_path = None
                    if completed_path:
                        self.jobs.mark_downloaded(job_id)
                        break
                    self.jobs.update(
                        job_id,
                        status="cancelled",
                        transfer_id=None,
                        message=(
                            "slskd transfer did not reappear before the 5-minute restart restore "
                            "timeout expired; treating it as removed. The episode may be searched "
                            "again by the normal automatic round-robin."
                        ),
                    )
                    await self._process_episode_batch(job.episode_id, sonarr, slskd, settings)
                    return

                # A queued job that was never part of restart recovery may have
                # been created before slskd accepted the enqueue. Only those jobs
                # are allowed to retry the same candidate here. A job that waited
                # for restart restoration is handled above and is never silently
                # re-enqueued after its restore timeout.
                current_job = self.jobs.get(job_id)
                if current_job.status == "queued" and not transfer_seen:
                    if not await self._enqueue_when_peer_available(job_id, candidate, slskd):
                        return
                    transfer = await self._find_slskd_transfer(slskd, self.jobs.get(job_id))
                    if transfer is not None:
                        transfer_seen = True
                        missing_since = None
                        continue

                if missing_since is None:
                    missing_since = now
                grace = (
                    TRANSFER_MISSING_AFTER_SEEN_SECONDS
                    if transfer_seen or bool(self.jobs.get(job_id).transfer_id)
                    else TRANSFER_MISSING_BEFORE_SEEN_SECONDS
                )
                if now - missing_since >= grace:
                    completed_path = None
                    try:
                        completed_path = await slskd.find_downloaded_file(job.remote_filename, job.size)
                    except Exception:
                        completed_path = None
                    if completed_path:
                        self.jobs.mark_downloaded(job_id)
                        break
                    self.jobs.update(
                        job_id,
                        status="cancelled",
                        message="slskd transfer was removed or no longer exists.",
                    )
                    await self._process_episode_batch(job.episode_id, sonarr, slskd, settings)
                    return
            await asyncio.sleep(TRANSFER_POLL_SECONDS)

        prepared = await self._prepare_completed_download(
            job_id=job_id,
            episode=episode,
            slskd=slskd,
            settings=settings,
        )
        if not prepared:
            await self._process_episode_batch(job.episode_id, sonarr, slskd, settings)
            return

        await self._process_episode_batch(job.episode_id, sonarr, slskd, settings)

    async def _prepare_completed_download(
        self,
        job_id: int,
        episode: MissingEpisode | None,
        slskd: SlskdClient,
        settings,
    ) -> bool:
        """Prepare a completed download using restart-safe persisted checkpoints.

        Filesystem operations and SQLite updates cannot be one atomic transaction.
        Each destructive/expensive operation therefore persists its intent before
        it starts and its successful result immediately afterwards. On restart we
        inspect both sides of that checkpoint and continue without depending on a
        completed slskd transfer record still existing.
        """
        job = self.jobs.get(job_id)
        settings = settings_for_target(settings, job.episode_id)
        step = str(job.prepare_step or "").strip().lower()

        # Resume any checkpoint created after the file was successfully located.
        # This is the critical restart path: do not ask slskd to rediscover a file
        # that Soulseekarr may already have renamed or partially copied.
        if step in {
            "located",
            "rename_pending",
            "renamed",
            "copy_queued",
            "copying",
            "copied",
        }:
            return await self._resume_preparation(job_id, settings)

        self.jobs.update(
            job_id,
            status="copying",
            prepare_step="locating",
            message="Preparing: locating completed file.",
        )

        source: Path | None = None
        sonarr_file: str | None = None
        mapped_local_path: str | None = None
        mapped_sonarr_path: str | None = None
        slskd_lookup_error: str | None = None
        direct_mapped = False

        try:
            slskd_file = await slskd.find_downloaded_file(job.remote_filename, job.size)
        except Exception as exc:
            slskd_file = None
            slskd_lookup_error = f"{type(exc).__name__}: {exc}"

        if settings.folder_mappings and slskd_file:
            mapped = resolve_mapping(slskd_file, settings.folder_mappings)
            if mapped:
                mapped_local_path = mapped.local_file
                mapped_sonarr_path = mapped.sonarr_file
                source = await self._wait_for_exact_file(Path(mapped.local_file), job.size, root=Path(mapped.soulseekarr_path))
                if source is not None:
                    sonarr_file = mapped.sonarr_file
                    direct_mapped = True

        if source is None:
            source = await self._wait_for_local_file(
                Path(settings.slskd_downloads_path), job.remote_filename, job.size
            )
            direct_mapped = False
            sonarr_file = None
            if source is None:
                desired_name = str(job.prepare_target_name or "").strip() or build_import_filename(
                    series_title=job.series_title,
                    series_year=episode.series_year if episode else None,
                    episode_code=job.episode_code,
                    episode_title=job.episode_title,
                    remote_filename=job.remote_filename,
                )
                would_stage_to = str(
                    PurePosixPath(settings.sonarr_import_path)
                    / f"soulseekarr-{job_id}"
                    / desired_name
                )
                mapping_summary = "; ".join(
                    f"{m.get('slskd_path')} -> {m.get('soulseekarr_path')} -> {m.get('sonarr_path')}"
                    for m in settings.folder_mappings
                ) or "none"
                details = [
                    "Sonarr request sent: no (Soulseekarr never located the completed file)",
                    f"Soulseek remote path: {job.remote_filename}",
                    f"Expected size: {job.size} bytes",
                    f"slskd completed-file path: {slskd_file or 'not returned by slskd'}",
                    f"Mapped Soulseekarr path: {mapped_local_path or 'not resolved'}",
                    f"Mapped Sonarr path: {mapped_sonarr_path or 'not resolved'}",
                    f"Fallback Soulseekarr search root: {settings.slskd_downloads_path}",
                    f"Would-be staged Sonarr path: {would_stage_to}",
                    f"Configured mappings: {mapping_summary}",
                ]
                if slskd_lookup_error:
                    details.append(f"slskd completed-file lookup error: {slskd_lookup_error}")
                self.jobs.update(
                    job_id,
                    status="failed",
                    message=(
                        "slskd reported success but the completed file could not be located. "
                        + " | ".join(details)
                    ),
                )
                return False

        desired_name = str(job.prepare_target_name or "").strip() or build_import_filename(
            series_title=job.series_title,
            series_year=episode.series_year if episode else None,
            episode_code=job.episode_code,
            episode_title=job.episode_title,
            remote_filename=job.remote_filename,
        )

        # The successful locate is the first durable preparation checkpoint.
        # source_file is intentionally written now, not only when preparation is
        # finished, so recovery never has to find the original basename again.
        self.jobs.update(
            job_id,
            status="copying",
            prepare_step="located",
            prepare_target_name=desired_name,
            source_file=str(source),
            sonarr_file=str(sonarr_file) if direct_mapped and sonarr_file else None,
            slskd_file=str(slskd_file) if slskd_file else None,
            message="Preparing: normalizing filename.",
        )
        return await self._resume_preparation(job_id, settings)

    @staticmethod
    def _safe_target_filename(value: str | None) -> str | None:
        """Return a single safe filename component or ``None``.

        The planned filename is persisted in SQLite and can also originate from
        remote release metadata. Treat it as data on resume: it must never become
        a path, an alternate-directory selector, or a control-character carrier.
        """
        text = str(value or "").strip()
        if not text or text in {".", ".."}:
            return None
        if "/" in text or "\\" in text:
            return None
        if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in text):
            return None
        try:
            if len(text.encode("utf-8")) > 255:
                return None
        except UnicodeError:
            return None
        return text

    @staticmethod
    def _is_exact_file(path: Path | None, expected_size: int) -> bool:
        if path is None:
            return False
        try:
            info = path.lstat()
            return stat.S_ISREG(info.st_mode) and info.st_size == int(expected_size)
        except OSError:
            return False

    async def _resume_preparation(self, job_id: int, settings) -> bool:
        """Continue from the newest durable preparation checkpoint."""
        job = self.jobs.get(job_id)
        settings = settings_for_target(settings, job.episode_id)
        step = str(job.prepare_step or "").strip().lower()
        desired_name = self._safe_target_filename(job.prepare_target_name)

        if not desired_name:
            self.jobs.update(
                job_id,
                status="failed",
                message="Preparation checkpoint has an invalid normalized target filename.",
            )
            return False

        source = Path(job.source_file) if job.source_file else None
        destination = Path(job.import_file) if job.import_file else None
        allowed_roots = self._local_data_roots(settings)

        # Persisted paths and mapped slskd paths are data, not authority. Refuse
        # to resume a checkpoint that points outside configured data roots or
        # through a symlink. This prevents path-mapping traversal from becoming
        # arbitrary host-file read/rename/delete access.
        if source is not None and not self._path_within_any_root(source, allowed_roots):
            self.jobs.update(job_id, status="failed", message="Preparation source path escaped configured data roots.")
            return False
        if destination is not None and not self._path_within_any_root(destination, allowed_roots):
            self.jobs.update(job_id, status="failed", message="Preparation destination path escaped configured data roots.")
            return False
        if source is not None and source.exists() and source.is_symlink():
            self.jobs.update(job_id, status="failed", message="Preparation source is a symlink; refusing unsafe file access.")
            return False
        if destination is not None and destination.exists() and destination.is_symlink():
            self.jobs.update(job_id, status="failed", message="Preparation destination is a symlink; refusing unsafe file access.")
            return False

        if step == "located":
            if not self._is_exact_file(source, job.size):
                self.jobs.update(
                    job_id,
                    status="failed",
                    message=(
                        "Preparation recovery could not find the previously located source file: "
                        f"{source or 'not recorded'}."
                    ),
                )
                return False

            self.jobs.update(job_id, message="Preparing: normalizing filename.")

            # A Sonarr path at the located checkpoint means folder mapping made
            # this a direct-mapped file. Try the cheap in-place rename first.
            if job.sonarr_file:
                normalized_source = source.with_name(desired_name)
                normalized_sonarr = str(PurePosixPath(job.sonarr_file).with_name(desired_name))

                if normalized_source == source:
                    self.jobs.update(
                        job_id,
                        prepare_step="renamed",
                        import_file=str(source),
                        sonarr_file=normalized_sonarr,
                        message="Preparing: filename already normalized.",
                    )
                    return await self._finish_prepared(job_id)

                # Persist the rename destination before touching the filesystem.
                # If the process dies after rename() but before the success update,
                # recovery can prove success by finding this exact target file.
                self.jobs.update(
                    job_id,
                    prepare_step="rename_pending",
                    import_file=str(normalized_source),
                    sonarr_file=normalized_sonarr,
                    message="Preparing: renaming file in place.",
                )
                return await self._resume_preparation(job_id, settings)

            # No direct Sonarr mapping: create a durable copy intent before the
            # copy lane is entered. _stage_copy persists both local and Sonarr
            # target paths and resumes an existing partial file when necessary.
            await self._stage_copy(
                job_id, source, desired_name, settings.sonarr_import_path,
                prefer_hardlinks=settings.prefer_hardlinks,
            )
            return await self._finish_prepared(job_id)

        if step == "rename_pending":
            if destination is None or not job.sonarr_file:
                self.jobs.update(
                    job_id, status="failed", message="Rename checkpoint is missing its target path."
                )
                return False

            self.jobs.update(job_id, message="Preparing: renaming file in place.")

            # The rename may already have succeeded immediately before a crash.
            if not self._is_exact_file(source, job.size) and self._is_exact_file(destination, job.size):
                renamed = True
            elif self._is_exact_file(source, job.size):
                renamed = await self._rename_in_place(source, destination)
                if not renamed and not self._is_exact_file(source, job.size) and self._is_exact_file(destination, job.size):
                    renamed = True
            else:
                renamed = False

            if renamed:
                slskd_file = job.slskd_file
                if slskd_file:
                    normalized = str(slskd_file).replace("\\", "/")
                    normalized = str(PurePosixPath(normalized).with_name(desired_name))
                    if "\\" in str(slskd_file):
                        normalized = normalized.replace("/", "\\")
                    slskd_file = normalized
                self.jobs.update(
                    job_id,
                    prepare_step="renamed",
                    source_file=str(destination),
                    import_file=str(destination),
                    slskd_file=slskd_file,
                    message="Preparing: rename complete.",
                )
                return await self._finish_prepared(job_id)

            # Rename can legitimately fail across mounts/read-only mappings. Keep
            # the original source checkpoint and fall back to the serialized copy
            # lane rather than losing the already-located file.
            if not self._is_exact_file(source, job.size):
                self.jobs.update(
                    job_id,
                    status="failed",
                    message=(
                        "Preparation recovery found neither the pre-rename source nor the "
                        f"renamed target ({source} -> {destination})."
                    ),
                )
                return False
            self.jobs.update(job_id, import_file=None, sonarr_file=None, prepare_step="located")
            await self._stage_copy(
                job_id, source, desired_name, settings.sonarr_import_path,
                prefer_hardlinks=settings.prefer_hardlinks,
            )
            return await self._finish_prepared(job_id)

        if step == "renamed":
            if destination is None:
                destination = source
            if not self._is_exact_file(destination, job.size):
                self.jobs.update(
                    job_id,
                    status="failed",
                    message=f"Preparation recovery could not find renamed file: {destination}.",
                )
                return False
            if job.source_file != str(destination):
                self.jobs.update(job_id, source_file=str(destination))
            return await self._finish_prepared(job_id)

        if step in {"copy_queued", "copying"}:
            # A completed target wins even if the source disappeared after the
            # last byte was written but before the database checkpoint commit.
            if destination is not None and self._is_exact_file(destination, job.size):
                self.jobs.update(
                    job_id,
                    prepare_step="copied",
                    message="Preparing: finalizing copied file.",
                )
                return await self._finish_prepared(job_id)

            if not self._is_exact_file(source, job.size):
                self.jobs.update(
                    job_id,
                    status="failed",
                    message=(
                        "Preparation recovery cannot resume the staging copy because the "
                        f"persisted source file is missing: {source or 'not recorded'}."
                    ),
                )
                return False

            await self._stage_copy(
                job_id,
                source,
                desired_name,
                settings.sonarr_import_path,
                destination=destination,
                sonarr_file=job.sonarr_file,
                prefer_hardlinks=settings.prefer_hardlinks,
            )
            return await self._finish_prepared(job_id)

        if step == "copied":
            if destination is None or not self._is_exact_file(destination, job.size):
                self.jobs.update(
                    job_id,
                    status="failed",
                    message=f"Preparation recovery could not find completed staging file: {destination}.",
                )
                return False
            return await self._finish_prepared(job_id)

        self.jobs.update(
            job_id, status="failed", message=f"Unknown preparation checkpoint: {step or 'none'}."
        )
        return False

    async def _finish_prepared(self, job_id: int) -> bool:
        job = self.jobs.get(job_id)
        if not job.import_file or not job.sonarr_file:
            self.jobs.update(
                job_id, status="failed", message="Prepared job is missing its persisted import path."
            )
            return False
        import_path = Path(job.import_file)
        if not self._is_exact_file(import_path, job.size):
            self.jobs.update(
                job_id,
                status="failed",
                message=f"Prepared file is missing or has the wrong size: {import_path}.",
            )
            return False
        self.jobs.update(
            job_id,
            status="ready",
            prepare_step="prepared",
            message="Download prepared; ready for immediate Sonarr evaluation.",
        )
        return True

    @staticmethod
    async def _wait_for_import_command(
        sonarr, command_id: int, episode_id: int, previous_file_id: int,
        timeout_seconds: int,
        previous_file: dict | None = None, expected_size: int = 0,
    ) -> dict:
        """Also watch the library: command bookkeeping can lag a finished import."""
        command_task = asyncio.create_task(
            sonarr.wait_for_command(command_id, timeout_seconds=timeout_seconds)
        )
        try:
            while True:
                done, _pending = await asyncio.wait({command_task}, timeout=2)
                if done:
                    return command_task.result()
                try:
                    episode = await sonarr.episode(episode_id)
                    file_id = int(
                        episode.get("episodeFileId") or episode.get("EpisodeFileId") or 0
                    )
                except Exception:
                    # A temporary library lookup failure must not interrupt the
                    # existing command waiter or trigger premature file cleanup.
                    continue
                has_file = bool(episode.get("hasFile") or episode.get("HasFile"))
                if has_file and file_id > 0 and file_id != previous_file_id:
                    return {"status": "completed"}
                if has_file and file_id > 0 and previous_file:
                    try:
                        current_file = await sonarr.episode_file(file_id)
                    except Exception:
                        continue
                    if imported_file_replaced(previous_file, current_file, expected_size):
                        return {"status": "completed"}
        finally:
            command_task.cancel()
            await asyncio.gather(command_task, return_exceptions=True)

    async def _process_episode_batch(self, episode_id: int, sonarr, slskd, settings) -> None:
        if isinstance(sonarr, MediaClient):
            sonarr = sonarr.for_target(episode_id)
        service = "Radarr" if is_movie_id(episode_id) else "Sonarr"
        media_label = "movie" if is_movie_id(episode_id) else "episode"
        lock = self.episode_locks.setdefault(int(episode_id), asyncio.Lock())
        async with lock:
            # A completed, prepared candidate is evaluated immediately. Other
            # queued/downloading candidates for the same episode remain alive and
            # can be evaluated later as upgrades after they finish. Holding a
            # ready file hostage to an active peer transfer can otherwise leave
            # imports stuck indefinitely.
            batch = self.jobs.episode_jobs(episode_id, ("ready", "evaluating", "importing"))
            if not batch:
                return
            batch_ids = {item.id for item in batch}

            for item in batch:
                self.jobs.update(
                    item.id,
                    status="evaluating",
                    message=f"Asking {service} to score this completed candidate batch.",
                )

            try:
                episode_data = await sonarr.episode(episode_id)
                expected_series_id = int(
                    episode_data.get("seriesId") or episode_data.get("SeriesId") or 0
                )
                if expected_series_id <= 0:
                    raise RuntimeError(f"{service} did not return the episode's series id")

                has_existing = bool(episode_data.get("hasFile") or episode_data.get("HasFile"))
                try:
                    existing_file_id = int(
                        episode_data.get("episodeFileId")
                        or episode_data.get("EpisodeFileId")
                        or 0
                    )
                except (TypeError, ValueError):
                    existing_file_id = 0

                existing_score: int | None = None
                existing_resolution = 0
                existing_source_rank = 0
                existing_quality_name: str | None = None
                existing_file: dict = {}
                if has_existing and existing_file_id > 0:
                    existing_file = await sonarr.episode_file(existing_file_id)
                    try:
                        existing_score = int(
                            existing_file.get("customFormatScore")
                            if existing_file.get("customFormatScore") is not None
                            else existing_file.get("CustomFormatScore")
                        )
                    except (TypeError, ValueError):
                        raise RuntimeError(
                            f"{service} did not return a Custom Format score for existing {media_label} file #{existing_file_id}"
                        )
                    existing_quality = existing_file.get("quality") or existing_file.get("Quality")
                    existing_resolution = quality_resolution(existing_quality)
                    existing_source_rank = quality_source_rank(existing_quality)
                    existing_quality_name = quality_name(existing_quality)

                eligible: list[tuple[int, int, int, int, object, dict]] = []
                evaluated_scores: dict[int, int] = {}
                for item in batch:
                    current = self.jobs.get(item.id)
                    if not current.sonarr_file:
                        self.jobs.update(
                            item.id,
                            status="failed",
                            message=f"Prepared job has no persisted {service} file path.",
                        )
                        continue
                    try:
                        result = await sonarr.evaluate_import_candidate(
                            current.sonarr_file,
                            current.episode_id,
                            expected_series_id,
                        )
                    except Exception as exc:
                        self.jobs.update(
                            item.id,
                            status="failed",
                            message=(
                                f"{service} candidate evaluation failed: {type(exc).__name__}: {exc}."
                                f"{self._job_path_note(current)}"
                            ),
                        )
                        continue

                    recognized = result.get("recognized") or {}
                    try:
                        score = int(
                            recognized.get("customFormatScore")
                            if recognized.get("customFormatScore") is not None
                            else recognized.get("CustomFormatScore") or 0
                        )
                    except (TypeError, ValueError):
                        score = 0
                    evaluated_scores[item.id] = score

                    if not result.get("ok"):
                        reason = self._rejection_message(result)
                        self.jobs.update(
                            item.id,
                            status="rejected",
                            message=(
                                f"{reason} Candidate Custom Format score: {score}."
                                f"{self._job_path_note(current)}"
                            ),
                        )
                        continue

                    recognized_languages = (
                        recognized.get("languages")
                        if recognized.get("languages") is not None
                        else recognized.get("Languages")
                    )
                    if not release_languages_allowed(recognized_languages):
                        detected = ", ".join(language_names(recognized_languages)) or "foreign language"
                        self.jobs.update(
                            item.id,
                            status="skipped",
                            message=(
                                f"Not imported: candidate is foreign-language only ({detected}). "
                                "Soulseekarr accepts unspecified language or releases that include English."
                                f"{self._job_path_note(current)}"
                            ),
                        )
                        continue

                    if existing_score is not None and score < existing_score:
                        self.jobs.update(
                            item.id,
                            status="skipped",
                            message=(
                                f"No upgrade: candidate Custom Format score {score} does not beat "
                                f"existing {service} score {existing_score}."
                                f"{self._job_path_note(current)}"
                            ),
                        )
                        continue

                    candidate_quality = recognized.get("quality") or recognized.get("Quality")
                    candidate_resolution = quality_resolution(candidate_quality)
                    candidate_source_rank = quality_source_rank(candidate_quality)
                    candidate_quality_name = quality_name(candidate_quality)
                    if existing_resolution > 0 and (
                        candidate_resolution <= 0 or candidate_resolution < existing_resolution
                    ):
                        shown_candidate = (
                            f"{candidate_resolution}p" if candidate_resolution > 0 else "unknown quality"
                        )
                        self.jobs.update(
                            item.id,
                            status="skipped",
                            message=(
                                f"No upgrade: candidate quality {shown_candidate} would downgrade "
                                f"the existing {existing_resolution}p episode file, even though its "
                                f"Custom Format score is {score}."
                                f"{self._job_path_note(current)}"
                            ),
                        )
                        continue

                    if existing_score is not None and candidate_source_rank < existing_source_rank:
                        self.jobs.update(
                            item.id,
                            status="skipped",
                            message=(
                                "No upgrade: candidate source "
                                f"{candidate_quality_name or 'unknown'} would downgrade existing source "
                                f"{existing_quality_name or 'unknown'}, even though its Custom Format "
                                f"score is {score}."
                                f"{self._job_path_note(current)}"
                            ),
                        )
                        continue

                    if (
                        existing_score is not None
                        and score == existing_score
                        and candidate_resolution == existing_resolution
                        and candidate_source_rank == existing_source_rank
                    ):
                        self.jobs.update(
                            item.id,
                            status="skipped",
                            message=(
                                f"No upgrade: candidate matches the existing {service} file in Custom Format "
                                "score, resolution, and source quality."
                                f"{self._job_path_note(current)}"
                            ),
                        )
                        continue

                    # Prefer Sonarr Custom Format score, then resolution and source
                    # quality. Earlier job id is a deterministic final tie breaker.
                    eligible.append((
                        score, candidate_resolution, candidate_source_rank, -item.id, item, recognized
                    ))

                eligible.sort(reverse=True, key=lambda row: (row[0], row[1], row[2], row[3]))
                imported_job_id: int | None = None
                imported_score: int | None = None

                for score, _resolution, _source_rank, _neg_id, item, recognized in eligible:
                    current = self.jobs.get(item.id)
                    self.jobs.update(
                        item.id,
                        status="importing",
                        message=(
                            f"Selected by {service} Custom Format score {score}"
                            + (f" over existing score {existing_score}" if existing_score is not None else "")
                            + "; submitting ManualImport."
                            + self._job_path_note(current)
                        ),
                    )
                    try:
                        command = await sonarr.submit_manual_import(recognized)
                    except Exception as exc:
                        self.jobs.update(
                            item.id,
                            status="failed",
                            message=(
                                f"{service} manual-import request failed: {type(exc).__name__}: {exc}."
                                f"{self._job_path_note(current)}"
                            ),
                        )
                        continue

                    command_id = command.get("id")
                    if not command_id:
                        self.jobs.update(
                            item.id,
                            status="failed",
                            message=(
                                f"{service} accepted the manual-import request but did not return a command id."
                                f"{self._job_path_note(current)}"
                            ),
                        )
                        continue

                    import_timeout_seconds = max(3600, min(86400, int(settings.sonarr_import_timeout_minutes) * 60))
                    self.jobs.update(
                        item.id,
                        message=f"Waiting for {service} ManualImport command {command_id} and library confirmation."
                        + self._job_path_note(current),
                    )
                    final = await self._wait_for_import_command(
                        sonarr, int(command_id), episode_id, existing_file_id,
                        timeout_seconds=import_timeout_seconds,
                        previous_file=existing_file, expected_size=current.size,
                    )
                    status = str(final.get("status") or "").lower()
                    if status == "failed":
                        self.jobs.update(
                            item.id,
                            status="failed",
                            message=(
                                f"{service} import command failed: {final.get('message') or final}."
                                f"{self._job_path_note(current)}"
                            ),
                        )
                        continue
                    if status != "completed":
                        self.jobs.update(
                            item.id,
                            status="failed",
                            message=(
                                f"Timed out waiting for {service} import command {command_id} to finish "
                                f"(last status: {status or 'unknown'})."
                                f"{self._job_path_note(current)}"
                            ),
                        )
                        continue

                    if existing_file_id > 0:
                        verified_episode = await sonarr.wait_for_episode_file_change(
                            episode_id,
                            existing_file_id,
                            timeout_seconds=import_timeout_seconds,
                            previous_file=existing_file, expected_size=current.size,
                        )
                    else:
                        verified_episode = await sonarr.wait_for_episode_file(
                            episode_id,
                            timeout_seconds=import_timeout_seconds,
                        )

                    has_file = bool(
                        verified_episode.get("hasFile") or verified_episode.get("HasFile")
                    )
                    try:
                        new_file_id = int(
                            verified_episode.get("episodeFileId")
                            or verified_episode.get("EpisodeFileId")
                            or 0
                        )
                    except (TypeError, ValueError):
                        new_file_id = 0

                    changed = new_file_id > 0 and (
                        existing_file_id <= 0 or new_file_id != existing_file_id
                    )
                    if has_file and new_file_id > 0 and not changed:
                        new_file = await sonarr.episode_file(new_file_id)
                        changed = imported_file_replaced(existing_file, new_file, current.size)
                    if not has_file or not changed:
                        if existing_file_id > 0:
                            failure_text = (
                                f"{service} ManualImport command {command_id} completed, but {service} still reports "
                                f"the previous {media_label} file #{existing_file_id}; the upgrade did not complete successfully."
                            )
                        else:
                            failure_text = (
                                f"{service} ManualImport command {command_id} completed, but {service} still reports "
                                "no episode file for this episode. The import did not complete successfully."
                            )
                        self.jobs.update(
                            item.id,
                            status="failed",
                            message=f"{failure_text}{self._job_path_note(current)}",
                        )
                        continue

                    new_score = score
                    try:
                        new_file = await sonarr.episode_file(new_file_id)
                        raw_score = (
                            new_file.get("customFormatScore")
                            if new_file.get("customFormatScore") is not None
                            else new_file.get("CustomFormatScore")
                        )
                        if raw_score is not None:
                            new_score = int(raw_score)
                    except Exception:
                        pass

                    imported_job_id = item.id
                    imported_score = new_score
                    if existing_score is None:
                        score_note = f"Custom Format score {new_score}."
                    else:
                        score_note = f"Custom Format score upgraded {existing_score} -> {new_score}."
                    self.jobs.update(
                        item.id,
                        status="imported",
                        message=(
                            f"{service} imported the selected candidate as {media_label} file #{new_file_id}. "
                            f"{score_note}"
                        ),
                    )
                    break

                if imported_job_id is not None:
                    for _score, _resolution, _source_rank, _neg_id, item, _recognized in eligible:
                        if item.id == imported_job_id:
                            continue
                        current = self.jobs.get(item.id)
                        if current.status in {"evaluating", "ready", "importing"}:
                            candidate_score = evaluated_scores.get(item.id, 0)
                            self.jobs.update(
                                item.id,
                                status="skipped",
                                message=(
                                    f"Not imported: candidate Custom Format score {candidate_score}; "
                                    f"selected candidate score {imported_score}."
                                    f"{self._job_path_note(current)}"
                                ),
                            )
                elif eligible:
                    # Every eligible import attempt failed. Jobs already carry
                    # their individual failure messages.
                    pass

            except Exception as exc:
                for item in self.jobs.episode_jobs(
                    episode_id, ("ready", "evaluating", "importing")
                ):
                    self.jobs.update(
                        item.id,
                        status="failed",
                        message=f"Episode upgrade evaluation failed: {type(exc).__name__}: {exc}",
                    )
            finally:
                # Every prepared candidate is disposable after the batch has
                # reached a terminal decision. Sonarr's final library file is
                # never one of these cleanup paths.
                for item_id in sorted(batch_ids):
                    item = self.jobs.get(item_id)
                    if item.status in {"imported", "rejected", "failed", "skipped"} and item.downloaded_at:
                        await self._cleanup_job(item.id, slskd, settings)

    @staticmethod
    def _slskd_transfer_failure_message(transfer: dict, state: str) -> str:
        """Describe terminal slskd failures without confusing them with Sonarr rejection."""
        exception = JobRunner._slskd_exception_text(transfer)
        lowered = str(state or "").casefold()
        if "rejected" in lowered:
            summary = "slskd/Soulseek download rejected the transfer before Sonarr import"
        elif "timedout" in lowered or "timed out" in lowered:
            summary = "slskd/Soulseek download timed out before Sonarr import"
        elif "aborted" in lowered:
            summary = "slskd/Soulseek download was aborted before Sonarr import"
        elif "cancelled" in lowered or "canceled" in lowered:
            summary = "slskd/Soulseek download was cancelled before Sonarr import"
        else:
            summary = "slskd/Soulseek download failed before Sonarr import"

        parts = [f"{summary}."]
        if state:
            parts.append(f"slskd state: {state}.")
        if exception:
            parts.append(f"slskd reason: {exception}")
        else:
            parts.append("slskd did not report a more specific exception reason.")
        return " ".join(parts)

    @staticmethod
    def _rejection_message(result: dict) -> str:
        reason = result.get("reason") or "Sonarr rejected the file."
        rejections = result.get("rejections") or []
        if rejections:
            details = "; ".join(
                str(x.get("reason") or x) if isinstance(x, dict) else str(x)
                for x in rejections
            )
            reason = f"{reason} {details}"
        if not str(reason).casefold().startswith("sonarr"):
            reason = f"Sonarr import rejection: {reason}"
        return reason

    @staticmethod
    def _job_path_note(job) -> str:
        sonarr_file = job.sonarr_file or "not recorded"
        sonarr_folder = str(PurePosixPath(sonarr_file).parent) if job.sonarr_file else "not recorded"
        return (
            f" Sonarr manual-import folder: {sonarr_folder};"
            f" Sonarr file path: {sonarr_file}."
        )

    async def _cleanup_job(self, job_id: int, slskd, settings) -> None:
        job = self.jobs.get(job_id)
        paths: set[Path] = set()
        if job.source_file:
            paths.add(Path(job.source_file))
        if job.import_file:
            paths.add(Path(job.import_file))

        protected_roots = {self.import_root, Path(settings.slskd_downloads_path)}
        for mapping in settings.folder_mappings:
            local_root = str(mapping.get("soulseekarr_path") or "").strip()
            if local_root:
                protected_roots.add(Path(local_root))

        removed, errors = await self._cleanup_download_files(
            paths, protected_roots=protected_roots
        )

        original = Path(job.source_file) if job.source_file else None
        original_still_exists = False
        if original is not None:
            try:
                original_still_exists = original.exists()
            except OSError:
                original_still_exists = True

        if original_still_exists and job.slskd_file:
            try:
                await slskd.delete_downloaded_file(job.slskd_file)
                removed += 1
                errors = [
                    error for error in errors
                    if not error.startswith(f"{original}:")
                ]
            except Exception as exc:
                errors.append(f"slskd API cleanup for {job.slskd_file}: {exc}")

        current = self.jobs.get(job_id)
        if current.status not in {"imported", "rejected", "failed", "skipped"}:
            return
        if errors:
            cleanup_note = f" Cleanup removed {removed} file(s), but failed for: {'; '.join(errors)}"
        elif removed:
            cleanup_note = f" Cleanup removed {removed} leftover download file(s)."
        else:
            cleanup_note = " No leftover download file remained after import processing."
        if cleanup_note not in current.message:
            self.jobs.update(job_id, message=f"{current.message}{cleanup_note}")

    async def _episode_for_id(self, sonarr: SonarrClient, episode_id: int) -> MissingEpisode | None:
        for ep in await sonarr.missing_episodes(False):
            if ep.id == episode_id:
                return ep
        return None

    async def _series_id_for_episode(self, sonarr: SonarrClient, episode_id: int) -> int | None:
        ep = await self._episode_for_id(sonarr, episode_id)
        return ep.series_id if ep else None

    async def _rename_in_place(self, source: Path, destination: Path) -> bool:
        if destination == source:
            return True
        try:
            if destination.exists():
                return False
            await asyncio.to_thread(source.rename, destination)
            return True
        except OSError:
            return False

    async def _stage_copy(
        self,
        job_id: int,
        source: Path,
        desired_name: str,
        sonarr_import_path: str,
        *,
        destination: Path | None = None,
        sonarr_file: str | None = None,
        prefer_hardlinks: bool = False,
    ) -> tuple[Path, str]:
        safe_name = self._safe_target_filename(desired_name)
        if not safe_name:
            raise RuntimeError("Refusing invalid staging filename.")
        desired_name = safe_name
        job_dir_name = f"soulseekarr-{job_id}"
        destination_dir = self.import_root / job_dir_name
        expected_destination = destination_dir / desired_name
        destination = destination or expected_destination
        if destination != expected_destination:
            raise RuntimeError("Refusing unexpected staging destination.")
        sonarr_file = sonarr_file or str(
            PurePosixPath(sonarr_import_path) / job_dir_name / desired_name
        )
        if not self._path_within_root(destination, self.import_root):
            raise RuntimeError("Refusing staging path outside Soulseekarr import root.")
        if destination.exists() and destination.is_symlink():
            raise RuntimeError("Refusing symlink staging destination.")

        # Persist the copy intent before waiting for the lane. This makes both the
        # original source and exact destination durable across container restarts.
        # The destination may already be a partial file from an interrupted copy.
        self.jobs.update(
            job_id,
            status="copying",
            prepare_step="copy_queued",
            prepare_target_name=desired_name,
            source_file=str(source),
            import_file=str(destination),
            sonarr_file=str(sonarr_file),
            message="Preparing: waiting for copy slot.",
        )

        settings = self.settings_store.load()
        sonarr = media_client(settings, SonarrClient)
        # Do not start another staging job while Sonarr is already moving/importing
        # media. This keeps large imports and HDD-backed staging from competing for
        # disk I/O. All waiting jobs remain visible as preparation work.
        if sonarr is not None:
            await self._wait_for_sonarr_import_idle(
                job_id,
                sonarr,
                prepare_step="copy_queued",
                resume_message="Preparing: waiting for copy slot.",
            )

        async with self.copy_lock:
            # Sonarr may have started an import while this job waited behind a
            # different staging copy, so check again after obtaining the lane.
            if sonarr is not None:
                await self._wait_for_sonarr_import_idle(
                    job_id,
                    sonarr,
                    prepare_step="copy_queued",
                    resume_message="Preparing: copy slot available; starting staging.",
                )
            destination.parent.mkdir(parents=True, exist_ok=True)
            linked = False
            if prefer_hardlinks:
                self.jobs.update(
                    job_id,
                    prepare_step="copying",
                    message="Preparing: trying hardlink staging.",
                )
                linked = await self._try_stage_hardlink(source, destination)

            if linked:
                self.jobs.update(
                    job_id,
                    prepare_step="copied",
                    message="Preparing: hardlink created; finalizing file.",
                )
            else:
                self.jobs.update(
                    job_id,
                    prepare_step="copying",
                    message=(
                        "Preparing: hardlink unavailable; copying file."
                        if prefer_hardlinks
                        else "Preparing: copying file."
                    ),
                )
                try:
                    await self._copy_staged_file(job_id, source, destination, sonarr=sonarr)
                except BaseException:
                    # Keep a partial file on ordinary process cancellation/restart so
                    # it can be resumed. Explicit user removal owns cleanup and will
                    # delete the persisted import_file/staging directory itself. Only
                    # remove a zero-byte artifact created before any useful progress.
                    try:
                        if destination.exists() and destination.stat().st_size == 0:
                            destination.unlink()
                            destination.parent.rmdir()
                    except OSError:
                        pass
                    raise

                self.jobs.update(
                    job_id,
                    prepare_step="copied",
                    message="Preparing: finalizing copied file.",
                )

        return destination, str(sonarr_file)

    async def _try_stage_hardlink(self, source: Path, destination: Path) -> bool:
        """Atomically replace the staging target with a hardlink when supported.

        Link to a temporary name first so an interrupted/resumable copy at the
        final destination is preserved unless hardlink creation actually succeeds.
        Cross-device links, filesystems without hardlink support, permissions, and
        other ordinary OS failures all fall back to the resumable copy path.
        """
        temp_link = destination.with_name(f".{destination.name}.soulseekarr-link")

        def create_link() -> bool:
            try:
                if source.is_symlink() or destination.is_symlink():
                    return False
                try:
                    temp_link.unlink()
                except FileNotFoundError:
                    pass
                os.link(source, temp_link, follow_symlinks=False)
                os.replace(temp_link, destination)
                return True
            except OSError:
                try:
                    temp_link.unlink()
                except OSError:
                    pass
                return False

        return await asyncio.to_thread(create_link)

    async def _copy_staged_file(
        self,
        job_id: int,
        source: Path,
        destination: Path,
        *,
        sonarr: SonarrClient | None = None,
    ) -> None:
        """Copy one staging file with restart-resume and UI progress updates."""
        try:
            total = max(0, int(source.stat().st_size))
        except OSError:
            total = 0

        existing = 0
        try:
            if destination.is_file():
                existing = max(0, int(destination.stat().st_size))
        except OSError:
            existing = 0

        # A corrupt/oversized partial cannot be resumed safely; restart this one
        # file from zero. Normal interrupted copies append from their exact size.
        if total > 0 and existing > total:
            await asyncio.to_thread(destination.unlink)
            existing = 0

        copied = existing
        percent = 100 if total <= 0 and existing else (0 if total <= 0 else min(100, int(copied * 100 / total)))
        last_reported_percent = percent - 2
        self.jobs.update(job_id, message=f"Preparing: copying file ({percent}%).")

        loop = asyncio.get_running_loop()
        next_sonarr_check = 0.0

        if total <= 0 or copied < total:
            mode = "ab" if copied > 0 else "wb"
            with source.open("rb") as src, destination.open(mode) as dst:
                if copied > 0:
                    src.seek(copied)
                while True:
                    # A Sonarr import can begin after a copy has already started.
                    # Pause at a chunk boundary so the two heavy disk operations do
                    # not compete. Checks are throttled; the copy does not call the
                    # Sonarr API for every 8 MiB block.
                    if sonarr is not None and loop.time() >= next_sonarr_check:
                        await self._wait_for_sonarr_import_idle(
                            job_id,
                            sonarr,
                            prepare_step="copying",
                            resume_message=f"Preparing: copying file ({percent}%).",
                        )
                        next_sonarr_check = loop.time() + SONARR_IMPORT_ACTIVITY_POLL_SECONDS

                    chunk = await asyncio.to_thread(src.read, 8 * 1024 * 1024)
                    if not chunk:
                        break
                    await asyncio.to_thread(dst.write, chunk)
                    copied += len(chunk)
                    percent = 100 if total <= 0 else min(100, int(copied * 100 / total))
                    if percent >= 100 or percent >= last_reported_percent + 2:
                        self.jobs.update(job_id, message=f"Preparing: copying file ({percent}%).")
                        last_reported_percent = percent

        # Refuse to advance a short/truncated copy into Sonarr evaluation.
        if total > 0:
            try:
                final_size = int(destination.stat().st_size)
            except OSError as exc:
                raise RuntimeError(f"staging copy disappeared before finalization: {exc}") from exc
            if final_size != total:
                raise RuntimeError(
                    f"staging copy size mismatch: expected {total} bytes, got {final_size}"
                )

        self.jobs.update(job_id, message="Preparing: finalizing copied file.")
        await asyncio.to_thread(shutil.copystat, source, destination)

    async def _cleanup_download_files(
        self, paths: set[Path], protected_roots: set[Path] | None = None
    ) -> tuple[int, list[str]]:
        """Remove completed/staging files and empty temporary parent folders.

        After each disposable file is removed (or Sonarr has already moved it),
        walk upward from that file's parent and remove every empty directory until
        a configured mount/mapping root is reached. Each removal uses ``rmdir`` so
        a directory containing any unrelated file or subdirectory is never touched.
        Protected roots themselves are never removed.
        """
        removed = 0
        errors: list[str] = []
        unique = sorted({Path(path) for path in paths}, key=lambda p: len(str(p)), reverse=True)
        parents = {path.parent for path in unique}
        protected = {self.import_root.resolve(strict=False)}
        for root in protected_roots or set():
            protected.add(Path(root).resolve(strict=False))

        for path in unique:
            try:
                resolved = path.resolve(strict=False)
                if protected and not any(
                    resolved == root or root in resolved.parents for root in protected
                ):
                    errors.append(f"{path}: refused cleanup outside configured data roots")
                    continue
                if path.is_file() or path.is_symlink():
                    await asyncio.to_thread(path.unlink)
                    removed += 1
            except FileNotFoundError:
                pass
            except OSError as exc:
                errors.append(f"{path}: {exc}")

        # Remove empty temporary directory chains. This deliberately stops on
        # the first non-empty/unremovable directory in each chain because any
        # ancestor of that directory cannot be empty either.
        for parent in sorted(parents, key=lambda p: len(str(p)), reverse=True):
            current = parent
            while True:
                current_resolved = current.resolve(strict=False)
                if current_resolved in protected:
                    break

                # Do not walk outside every configured protected root. All paths
                # passed here should live under one of these roots; this guard
                # prevents a malformed path from walking toward filesystem root.
                if protected and not any(
                    current_resolved == root or root in current_resolved.parents
                    for root in protected
                ):
                    break

                try:
                    await asyncio.to_thread(current.rmdir)
                except FileNotFoundError:
                    # The folder may already have been removed through another
                    # alias/path. Continue upward, but keep root protection.
                    pass
                except OSError:
                    # Non-empty, read-only, mount point, or otherwise not removable.
                    break

                next_parent = current.parent
                if next_parent == current:
                    break
                current = next_parent

        return removed, errors

    @staticmethod
    def _local_path_candidates(root: Path, remote_filename: str) -> tuple[list[Path], str]:
        """Return likely slskd completed-file paths plus the literal basename.

        Current slskd defaults completed downloads to ``${SOURCE_DIRECTORY}``,
        i.e. only the remote file's immediate parent folder. Older/custom
        configurations may preserve ``${SOURCE_PATH}`` or put files directly in
        the downloads root. Check all of those deterministic layouts before the
        recursive fallback.
        """
        remote_parts = [
            part for part in strip_remote_path_root(remote_filename).split("/")
            if part not in ("", ".", "..")
        ]
        basename = remote_parts[-1] if remote_parts else ""
        if not basename:
            return [], ""

        candidates: list[Path] = []
        if len(remote_parts) >= 2:
            # slskd 0.26+ default: transfers.download.destination.subdirectory
            # is ${SOURCE_DIRECTORY}, the immediate remote parent directory.
            candidates.append(root / remote_parts[-2] / basename)
        # Custom/legacy configuration preserving the full remote directory path.
        candidates.append(root.joinpath(*remote_parts))
        # Explicit slskd destination.subdirectory: {} puts files at root.
        candidates.append(root / basename)

        unique: list[Path] = []
        seen: set[str] = set()
        for candidate in candidates:
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            unique.append(candidate)
        return unique, basename

    def _find_local_file_now(self, root: Path, remote_filename: str, expected_size: int) -> Path | None:
        """Find a completed local file immediately, with literal filename matching."""
        candidates, basename = self._local_path_candidates(root, remote_filename)
        if not basename or not root.exists():
            return None

        for direct in candidates:
            try:
                if self._is_safe_regular_file(direct, expected_size, [root]):
                    return direct
            except OSError:
                pass

        matches: list[Path] = []
        try:
            for path in root.rglob("*"):
                if path.name != basename:
                    continue
                if self._is_safe_regular_file(path, expected_size, [root]):
                    matches.append(path)
        except OSError:
            return None
        if not matches:
            return None
        matches.sort(key=lambda path: path.stat().st_mtime, reverse=True)
        return matches[0]

    async def _wait_for_exact_file(
        self, path: Path, expected_size: int, *, root: Path | None = None
    ) -> Path | None:
        roots = [root] if root is not None else [path.parent]
        for _ in range(24):
            if self._is_safe_regular_file(path, expected_size, roots):
                return path
            await asyncio.sleep(5)
        return None

    async def _wait_for_local_file(self, root: Path, remote_filename: str, expected_size: int) -> Path | None:
        # Never pass the remote basename itself to rglob(). Soulseek release
        # names commonly contain glob metacharacters such as ``[TAoE]``. If
        # used as a glob pattern, a perfectly valid literal filename can be
        # invisible to the fallback locator even though it is on disk.
        candidates, basename = self._local_path_candidates(root, remote_filename)

        for _ in range(24):  # up to two minutes for filesystem visibility/flush
            if root.exists() and basename:
                # First try deterministic layouts used by current/default and
                # legacy/custom slskd destination settings. This avoids walking
                # a potentially large downloads tree for common Windows paths.
                for direct in candidates:
                    try:
                        if self._is_safe_regular_file(direct, expected_size, [root]):
                            return direct
                    except OSError:
                        pass

                matches = []
                try:
                    # Use a fixed wildcard only for traversal and compare the
                    # filename literally. This keeps [], ?, * and similar
                    # characters in Soulseek filenames from becoming glob
                    # syntax.
                    for path in root.rglob("*"):
                        if path.name != basename:
                            continue
                        if self._is_safe_regular_file(path, expected_size, [root]):
                            matches.append(path)
                except OSError:
                    pass
                if matches:
                    matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                    return matches[0]
            await asyncio.sleep(5)
        return None
