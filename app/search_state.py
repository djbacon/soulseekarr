from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class SearchStateStore:
    """Persistent state for query rotation and delayed slskd search cleanup."""

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

    def _connect(self) -> sqlite3.Connection:
        if self.path.is_symlink():
            raise RuntimeError("Refusing to open soulseekarr.db through a symlink.")
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS search_cycles (
                    episode_id INTEGER PRIMARY KEY,
                    next_variant INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS slskd_search_history (
                    search_id TEXT PRIMARY KEY,
                    slskd_url TEXT NOT NULL,
                    completed_at REAL NOT NULL,
                    delete_after REAL NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_slskd_search_delete_after "
                "ON slskd_search_history(slskd_url, delete_after)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auto_search_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    last_series_id INTEGER,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auto_series_season_state (
                    series_id INTEGER PRIMARY KEY,
                    last_season_number INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auto_series_season_episode_state (
                    series_id INTEGER NOT NULL,
                    season_number INTEGER NOT NULL,
                    last_episode_id INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (series_id, season_number)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auto_series_enable_priority (
                    series_id INTEGER PRIMARY KEY,
                    queued_at REAL NOT NULL,
                    cooldowns_reset INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_auto_series_enable_priority_queue "
                "ON auto_series_enable_priority(queued_at, series_id)"
            )
            columns = {row[1] for row in conn.execute("PRAGMA table_info(search_cycles)").fetchall()}
            if "last_search_at" not in columns:
                conn.execute("ALTER TABLE search_cycles ADD COLUMN last_search_at REAL")
            if "next_search_at" not in columns:
                conn.execute("ALTER TABLE search_cycles ADD COLUMN next_search_at REAL")
            if "last_outcome" not in columns:
                conn.execute("ALTER TABLE search_cycles ADD COLUMN last_outcome TEXT")
            if "last_target_type" not in columns:
                conn.execute("ALTER TABLE search_cycles ADD COLUMN last_target_type TEXT")
            if "outcome_streak" not in columns:
                conn.execute("ALTER TABLE search_cycles ADD COLUMN outcome_streak INTEGER NOT NULL DEFAULT 0")

    def current_variant(self, episode_id: int, variant_count: int) -> int:
        """Return the query variant currently assigned to an episode.

        The variant is intentionally sticky. Call ``advance_variant()`` only after
        a successful search has produced no usable candidates. This prevents a
        good search spelling from being rotated away merely because time passed.
        """
        if variant_count <= 0:
            raise ValueError("variant_count must be positive")
        now = utcnow()
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT next_variant FROM search_cycles WHERE episode_id = ?",
                (episode_id,),
            ).fetchone()
            index = int(row["next_variant"]) % variant_count if row else 0
            conn.execute(
                """
                INSERT INTO search_cycles (episode_id, next_variant, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(episode_id) DO UPDATE SET
                    next_variant = excluded.next_variant,
                    updated_at = excluded.updated_at
                """,
                (episode_id, index, now),
            )
            conn.commit()
        return index

    def set_variant(self, episode_id: int, variant_count: int, index: int) -> int:
        """Explicitly select the current query variant for an episode.

        Manual searches use this to make the user's chosen spelling the same
        persisted spelling automatic searches will use. The selection is
        normalized to the available variant count and returned.
        """
        if variant_count <= 0:
            raise ValueError("variant_count must be positive")
        selected = int(index) % variant_count
        now = utcnow()
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO search_cycles (episode_id, next_variant, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(episode_id) DO UPDATE SET
                    next_variant = excluded.next_variant,
                    updated_at = excluded.updated_at
                """,
                (episode_id, selected, now),
            )
            conn.commit()
        return selected

    def advance_variant(
        self,
        episode_id: int,
        variant_count: int,
        *,
        expected_index: int | None = None,
    ) -> int:
        """Advance to the next query variant and return the newly selected index.

        ``expected_index`` makes completion idempotent when overlapping callers
        finish the same search: only the caller still matching the stored variant
        advances it.
        """
        if variant_count <= 0:
            raise ValueError("variant_count must be positive")
        now = utcnow()
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT next_variant FROM search_cycles WHERE episode_id = ?",
                (episode_id,),
            ).fetchone()
            current = int(row["next_variant"]) % variant_count if row else 0
            if expected_index is not None and current != int(expected_index) % variant_count:
                conn.commit()
                return current
            next_index = (current + 1) % variant_count
            conn.execute(
                """
                INSERT INTO search_cycles (episode_id, next_variant, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(episode_id) DO UPDATE SET
                    next_variant = excluded.next_variant,
                    updated_at = excluded.updated_at
                """,
                (episode_id, next_index, now),
            )
            conn.commit()
        return next_index

    def record_search_success(self, episode_id: int, *, now_ts: float | None = None) -> None:
        """Record a completed search using the legacy fixed-cooldown behavior.

        Manual result browsing still uses this path. Automatic discovery records a
        richer outcome with :meth:`record_search_outcome`, which supplies an
        adaptive ``next_search_at`` instead of a single fixed delay.
        """
        now = time.time() if now_ts is None else float(now_ts)
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT next_variant FROM search_cycles WHERE episode_id = ?",
                (int(episode_id),),
            ).fetchone()
            variant = int(row["next_variant"]) if row else 0
            conn.execute(
                """
                INSERT INTO search_cycles
                    (episode_id, next_variant, updated_at, last_search_at, next_search_at,
                     last_outcome, last_target_type, outcome_streak)
                VALUES (?, ?, ?, ?, NULL, 'manual-success', 'manual', 0)
                ON CONFLICT(episode_id) DO UPDATE SET
                    last_search_at = excluded.last_search_at,
                    next_search_at = NULL,
                    last_outcome = excluded.last_outcome,
                    last_target_type = excluded.last_target_type,
                    outcome_streak = 0
                """,
                (int(episode_id), variant, utcnow(), now),
            )
            conn.commit()

    def record_search_outcome(
        self,
        episode_id: int,
        target_type: str,
        outcome: str,
        delay_hours: float,
        *,
        now_ts: float | None = None,
    ) -> int:
        """Persist an automatic-search outcome and its adaptive next-run time.

        The returned integer is the consecutive streak for the same target/outcome
        pair. Callers can use the previous streak to choose the delay, then persist
        the result here. A different outcome or priority tier starts a fresh streak.
        """
        now = time.time() if now_ts is None else float(now_ts)
        delay_seconds = max(0.0, float(delay_hours)) * 3600.0
        target = str(target_type or "").strip().lower() or "unknown"
        normalized_outcome = str(outcome or "").strip().lower() or "unknown"
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT next_variant, last_outcome, last_target_type, outcome_streak
                FROM search_cycles WHERE episode_id = ?
                """,
                (int(episode_id),),
            ).fetchone()
            variant = int(row["next_variant"]) if row else 0
            previous_streak = int(row["outcome_streak"] or 0) if row else 0
            same = bool(
                row
                and str(row["last_outcome"] or "") == normalized_outcome
                and str(row["last_target_type"] or "") == target
            )
            streak = previous_streak + 1 if same else 1
            conn.execute(
                """
                INSERT INTO search_cycles
                    (episode_id, next_variant, updated_at, last_search_at, next_search_at,
                     last_outcome, last_target_type, outcome_streak)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(episode_id) DO UPDATE SET
                    last_search_at = excluded.last_search_at,
                    next_search_at = excluded.next_search_at,
                    last_outcome = excluded.last_outcome,
                    last_target_type = excluded.last_target_type,
                    outcome_streak = excluded.outcome_streak,
                    updated_at = excluded.updated_at
                """,
                (
                    int(episode_id), variant, utcnow(), now, now + delay_seconds,
                    normalized_outcome, target, streak,
                ),
            )
            conn.commit()
        return streak

    def outcome_streak(self, episode_id: int, target_type: str, outcome: str) -> int:
        """Return the current streak when it matches ``target_type``/``outcome``."""
        target = str(target_type or "").strip().lower() or "unknown"
        normalized_outcome = str(outcome or "").strip().lower() or "unknown"
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT last_outcome, last_target_type, outcome_streak
                FROM search_cycles WHERE episode_id = ?
                """,
                (int(episode_id),),
            ).fetchone()
        if row is None:
            return 0
        if str(row["last_outcome"] or "") != normalized_outcome:
            return 0
        if str(row["last_target_type"] or "") != target:
            return 0
        return int(row["outcome_streak"] or 0)

    def last_search_times(self, episode_ids: list[int]) -> dict[int, float | None]:
        """Return persisted last-search times for priority sorting in one query."""
        ids = sorted({int(value) for value in episode_ids})
        if not ids:
            return {}
        found: dict[int, float | None] = {}
        with self._lock, self._connect() as conn:
            for start in range(0, len(ids), 900):
                chunk = ids[start:start + 900]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"SELECT episode_id, last_search_at FROM search_cycles WHERE episode_id IN ({placeholders})",
                    chunk,
                ).fetchall()
                for row in rows:
                    found[int(row["episode_id"])] = (
                        None if row["last_search_at"] is None else float(row["last_search_at"])
                    )
        return {episode_id: found.get(episode_id) for episode_id in ids}

    def clear_search_cooldown(self, episode_id: int) -> None:
        """Make an episode immediately eligible for another automatic search.

        Keep its currently selected search variant; only the last successful
        search timestamp is cleared.
        """
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE search_cycles
                SET last_search_at = NULL, next_search_at = NULL, last_outcome = NULL,
                    last_target_type = NULL, outcome_streak = 0, updated_at = ?
                WHERE episode_id = ?
                """,
                (utcnow(), int(episode_id)),
            )

    def reset_adaptive_cooldowns(self, episode_ids: list[int]) -> None:
        """Make episodes immediately due without erasing their search age/order.

        Re-enabling a TV show should discard stale adaptive backoff from before
        the show was disabled, but its historical ``last_search_at`` still helps
        the normal priority scheduler decide which work is oldest. Setting an
        explicit zero ``next_search_at`` makes the row due immediately while
        preserving the selected query variant and last-search timestamp.
        """
        ids = sorted({int(value) for value in episode_ids if int(value) > 0})
        if not ids:
            return
        now = utcnow()
        with self._lock, self._connect() as conn:
            for start in range(0, len(ids), 900):
                chunk = ids[start:start + 900]
                placeholders = ",".join("?" for _ in chunk)
                conn.execute(
                    f"""
                    UPDATE search_cycles
                    SET next_search_at = 0, last_outcome = NULL, last_target_type = NULL,
                        outcome_streak = 0, updated_at = ?
                    WHERE episode_id IN ({placeholders})
                    """,
                    (now, *chunk),
                )

    def queue_series_enable_priority(self, series_id: int, *, now_ts: float | None = None) -> None:
        """Queue a one-time next-cycle priority turn for a newly enabled show."""
        queued_at = time.time() if now_ts is None else float(now_ts)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO auto_series_enable_priority
                    (series_id, queued_at, cooldowns_reset, updated_at)
                VALUES (?, ?, 0, ?)
                ON CONFLICT(series_id) DO UPDATE SET
                    queued_at = excluded.queued_at,
                    cooldowns_reset = 0,
                    updated_at = excluded.updated_at
                """,
                (int(series_id), queued_at, utcnow()),
            )

    def pending_series_enable_priorities(
        self, *, before_ts: float | None = None
    ) -> list[tuple[int, bool]]:
        """Return queued show-priority turns in FIFO order.

        ``before_ts`` lets an automatic cycle snapshot only toggles that existed
        when that cycle started. A show enabled midway through a running cycle is
        therefore guaranteed to lead the *next* cycle instead of joining the
        partially built current one.
        """
        params: tuple[float, ...] = ()
        where = ""
        if before_ts is not None:
            where = "WHERE queued_at <= ?"
            params = (float(before_ts),)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT series_id, cooldowns_reset
                FROM auto_series_enable_priority
                {where}
                ORDER BY queued_at ASC, series_id ASC
                """,
                params,
            ).fetchall()
        return [(int(row["series_id"]), bool(row["cooldowns_reset"])) for row in rows]

    def mark_series_enable_cooldowns_reset(self, series_id: int) -> None:
        """Remember that stale per-episode backoff was cleared for this turn."""
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE auto_series_enable_priority
                SET cooldowns_reset = 1, updated_at = ?
                WHERE series_id = ?
                """,
                (utcnow(), int(series_id)),
            )

    def clear_series_enable_priority(self, series_id: int) -> None:
        """Remove a queued show-priority turn after a real search or disable."""
        with self._lock, self._connect() as conn:
            conn.execute(
                "DELETE FROM auto_series_enable_priority WHERE series_id = ?",
                (int(series_id),),
            )

    def series_enable_priority_pending(self, series_id: int) -> bool:
        """Return whether a show still owns a one-time enable-priority turn."""
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM auto_series_enable_priority WHERE series_id = ? LIMIT 1",
                (int(series_id),),
            ).fetchone()
        return row is not None

    def search_due(
        self, episode_id: int, cooldown_hours: float, *, now_ts: float | None = None
    ) -> bool:
        """Return whether an automatic search may run for this episode now."""
        return self.search_cooldown_until(episode_id, cooldown_hours, now_ts=now_ts) is None

    def search_cooldown_until(
        self, episode_id: int, cooldown_hours: float, *, now_ts: float | None = None
    ) -> float | None:
        """Return the active automatic-search cooldown deadline as a Unix timestamp."""
        cooldown = max(0.0, float(cooldown_hours)) * 3600.0
        if cooldown <= 0:
            return None
        now = time.time() if now_ts is None else float(now_ts)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT last_search_at, next_search_at FROM search_cycles WHERE episode_id = ?",
                (int(episode_id),),
            ).fetchone()
        if row is None:
            return None
        if row["next_search_at"] is not None:
            until = float(row["next_search_at"])
        elif row["last_search_at"] is not None:
            until = float(row["last_search_at"]) + cooldown
        else:
            return None
        return until if until > now else None


    def auto_series_order(self, series_ids: list[int]) -> list[int]:
        """Return eligible series in round-robin order without moving the cursor.

        The persistent cursor represents the last TV show that launched a real
        Soulseek search. Merely inspecting a show that has no due target must not
        consume its turn, otherwise enough empty/cooldown shows can make two real
        searches land on the same TV show back-to-back.
        """
        choices = sorted({int(series_id) for series_id in series_ids})
        if not choices:
            return []

        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT last_series_id FROM auto_search_state WHERE singleton = 1"
            ).fetchone()
        last = None if row is None or row["last_series_id"] is None else int(row["last_series_id"])
        if last is None:
            return choices

        split = next((idx for idx, series_id in enumerate(choices) if series_id > last), 0)
        return choices[split:] + choices[:split]

    def record_auto_series_search(self, series_id: int) -> None:
        """Persist the TV show that actually launched an automatic Soulseek search."""
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO auto_search_state (singleton, last_series_id, updated_at)
                VALUES (1, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    last_series_id = excluded.last_series_id,
                    updated_at = excluded.updated_at
                """,
                (int(series_id), utcnow()),
            )
            conn.commit()

    def auto_season_order(self, series_id: int, season_numbers: list[int]) -> list[int]:
        """Return a show's eligible seasons after its last real automatic search.

        This is a second round-robin cursor nested inside the TV-show cursor. If
        the last automatic search for a show was in season 1, the next turn for
        that show offers the other seasons in ring order but does not offer
        season 1 again in that same turn. If none of those seasons is searchable,
        the show yields its turn rather than immediately repeating season 1.
        Merely inspecting or skipping a season does not move the cursor.
        """
        choices = sorted({int(season_number) for season_number in season_numbers})
        if not choices:
            return []

        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT last_season_number FROM auto_series_season_state WHERE series_id = ?",
                (int(series_id),),
            ).fetchone()
        last = None if row is None else int(row["last_season_number"])
        if last is None:
            return choices

        split = next((idx for idx, season_number in enumerate(choices) if season_number > last), 0)
        ordered = choices[split:] + choices[:split]
        # A true round-robin turn must not fall all the way through and choose
        # the same season again while the show has another season in its ring.
        # The caller may skip seasons whose episodes are cooling down; keeping
        # the previous season at the end of this list allowed that skip process
        # to wrap back to the same season and pin upgrade searches there.
        if len(choices) > 1 and last in choices:
            ordered = [season_number for season_number in ordered if season_number != last]
        return ordered

    def record_auto_season_search(self, series_id: int, season_number: int) -> None:
        """Persist the season that actually launched an automatic Soulseek search."""
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO auto_series_season_state (series_id, last_season_number, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(series_id) DO UPDATE SET
                    last_season_number = excluded.last_season_number,
                    updated_at = excluded.updated_at
                """,
                (int(series_id), int(season_number), utcnow()),
            )
            conn.commit()

    def auto_episode_order(
        self,
        series_id: int,
        season_number: int,
        episode_ids: list[int],
    ) -> list[int]:
        """Return one season's episode ring after its last real automatic search.

        The caller supplies the ring order (sequential or stable-random). This
        method rotates that ring after the last searched Sonarr episode id and,
        when alternatives exist, does not offer that same episode again in the
        same turn. Merely inspecting/skipping an episode does not move the cursor.
        """
        choices: list[int] = []
        seen: set[int] = set()
        for episode_id in episode_ids:
            value = int(episode_id)
            if value in seen:
                continue
            seen.add(value)
            choices.append(value)
        if not choices:
            return []

        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT last_episode_id
                FROM auto_series_season_episode_state
                WHERE series_id = ? AND season_number = ?
                """,
                (int(series_id), int(season_number)),
            ).fetchone()
        last = None if row is None else int(row["last_episode_id"])
        if last is None or last not in choices:
            return choices
        split = choices.index(last) + 1
        if split >= len(choices):
            split = 0
        ordered = choices[split:] + choices[:split]
        # As with seasons, do not wrap back to the episode that was searched on
        # the previous turn when the active episode ring contains alternatives.
        # This matters when every other weaker-score episode is temporarily on
        # cooldown: repeating the last episode would defeat the episode ring.
        if len(choices) > 1:
            ordered = [episode_id for episode_id in ordered if episode_id != last]
        return ordered

    def record_auto_episode_search(
        self,
        series_id: int,
        season_number: int,
        episode_id: int,
    ) -> None:
        """Persist the episode that actually launched an automatic Soulseek search."""
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO auto_series_season_episode_state
                    (series_id, season_number, last_episode_id, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(series_id, season_number) DO UPDATE SET
                    last_episode_id = excluded.last_episode_id,
                    updated_at = excluded.updated_at
                """,
                (int(series_id), int(season_number), int(episode_id), utcnow()),
            )
            conn.commit()

    def next_auto_series_id(self, series_ids: list[int]) -> int | None:
        """Legacy choose-and-persist helper retained for compatibility/tests."""
        ordered = self.auto_series_order(series_ids)
        if not ordered:
            return None
        selected = ordered[0]
        self.record_auto_series_search(selected)
        return selected

    def take_next_variant(self, episode_id: int, variant_count: int) -> int:
        """Legacy rotate-on-read helper retained for compatibility with old callers."""
        index = self.current_variant(episode_id, variant_count)
        self.advance_variant(episode_id, variant_count, expected_index=index)
        return index

    def record_completed_search(
        self,
        search_id: str,
        slskd_url: str,
        *,
        retention_seconds: float = 3600.0,
        now_ts: float | None = None,
    ) -> None:
        """Remember a Soulseekarr-created slskd search for delayed deletion."""
        now = time.time() if now_ts is None else float(now_ts)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO slskd_search_history (search_id, slskd_url, completed_at, delete_after)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(search_id) DO UPDATE SET
                    slskd_url = excluded.slskd_url,
                    completed_at = excluded.completed_at,
                    delete_after = excluded.delete_after
                """,
                (search_id, slskd_url.rstrip("/"), now, now + retention_seconds),
            )

    def due_searches(self, slskd_url: str, *, now_ts: float | None = None, limit: int = 100) -> list[str]:
        now = time.time() if now_ts is None else float(now_ts)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT search_id FROM slskd_search_history
                WHERE slskd_url = ? AND delete_after <= ?
                ORDER BY delete_after ASC
                LIMIT ?
                """,
                (slskd_url.rstrip("/"), now, int(limit)),
            ).fetchall()
        return [str(row["search_id"]) for row in rows]

    def forget_search(self, search_id: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM slskd_search_history WHERE search_id = ?", (search_id,))
