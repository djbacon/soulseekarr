from __future__ import annotations

import asyncio
from pathlib import PurePosixPath
from typing import Any

import httpx

from .config import normalize_api_key, normalize_service_url

from .models import MissingEpisode, SearchCandidate
from .pathmap import strip_remote_path_root
from .release_name import build_import_filename


def imported_file_replaced(previous: dict, current: dict, expected_size: int) -> bool:
    """Detect a replacement even when the media database reuses a file ID."""
    old_date = previous.get("dateAdded") or previous.get("DateAdded")
    new_date = current.get("dateAdded") or current.get("DateAdded")
    try:
        size_matches = int(current.get("size") or current.get("Size") or 0) == int(expected_size)
    except (TypeError, ValueError):
        return False
    return bool(old_date and new_date and old_date != new_date and expected_size > 0 and size_matches)


def language_names(languages: Any) -> list[str]:
    """Return normalized Sonarr language names from a serialized language list."""
    if not isinstance(languages, list):
        return []
    names: list[str] = []
    for item in languages:
        if isinstance(item, dict):
            value = item.get("name") if item.get("name") is not None else item.get("Name")
            if value is None:
                raw_id = item.get("id") if item.get("id") is not None else item.get("Id")
                try:
                    language_id = int(raw_id)
                except (TypeError, ValueError):
                    language_id = None
                if language_id == 0:
                    value = "Unknown"
                elif language_id == 1:
                    value = "English"
                elif language_id == -2:
                    value = "Original"
        elif isinstance(item, str):
            value = item
        else:
            value = None
        name = str(value or "").strip()
        if name and name.casefold() not in {existing.casefold() for existing in names}:
            names.append(name)
    return names


def release_languages_allowed(languages: Any) -> bool:
    """Allow releases with no declared language or with English among them.

    Sonarr represents an unspecified release language as Unknown. Foreign-only
    releases are rejected. Multi-language releases remain eligible whenever
    English is one of the parsed languages.
    """
    names = language_names(languages)
    if not names:
        return True
    folded = {name.casefold() for name in names}
    if "english" in folded:
        return True
    return folded <= {"unknown"}


def parsed_release_languages(parsed: Any) -> Any:
    """Return languages from either mapped or parser-only Sonarr responses."""
    if not isinstance(parsed, dict):
        return None
    direct = parsed.get("languages") if parsed.get("languages") is not None else parsed.get("Languages")
    if direct is not None:
        return direct
    info = parsed.get("parsedEpisodeInfo") or parsed.get("ParsedEpisodeInfo") or {}
    if isinstance(info, dict):
        return info.get("languages") if info.get("languages") is not None else info.get("Languages")
    return None


def quality_resolution(quality_model: Any) -> int:
    """Return Sonarr's numeric resolution from a serialized QualityModel."""
    if not isinstance(quality_model, dict):
        return 0
    quality = quality_model.get("quality") or quality_model.get("Quality") or quality_model
    if not isinstance(quality, dict):
        return 0
    raw = quality.get("resolution")
    if raw is None:
        raw = quality.get("Resolution")
    try:
        return max(0, int(raw or 0))
    except (TypeError, ValueError):
        return 0


def quality_name(quality_model: Any) -> str | None:
    """Return Sonarr's exact quality label from a serialized QualityModel."""
    if not isinstance(quality_model, dict):
        return None
    quality = quality_model.get("quality") or quality_model.get("Quality") or quality_model
    if not isinstance(quality, dict):
        return None
    raw = quality.get("name")
    if raw is None:
        raw = quality.get("Name")
    name = str(raw or "").strip()
    return name or None


def quality_source_rank(quality_model: Any) -> int:
    """Return a conservative source-quality rank for a Sonarr quality.

    Resolution is intentionally handled separately. This rank only compares the
    release *source* so same-resolution improvements such as HDTV -> WEBDL can
    qualify without allowing a source downgrade to sneak through merely because
    the candidate has more pixels. Unknown sources rank at zero.
    """
    if isinstance(quality_model, str):
        name = quality_model.strip()
    else:
        name = quality_name(quality_model) or ""
    normalized = "".join(ch for ch in name.casefold() if ch.isalnum())
    if not normalized:
        return 0
    if "remux" in normalized:
        return 60
    if "bluray" in normalized:
        return 50
    if "webdl" in normalized:
        return 40
    if "webrip" in normalized:
        return 30
    if "hdtv" in normalized:
        return 20
    if "sdtv" in normalized or "dvd" in normalized:
        return 10
    # Sonarr normally reports WEBDL rather than plain WEB, but keep a generic
    # WEB fallback for older/alternate parser responses. WEBRip was handled
    # above, so this maps only otherwise-ambiguous WEB source labels.
    if "web" in normalized:
        return 40
    return 0


class SonarrClient:
    def __init__(self, base_url: str, api_key: str, timeout: float = 20.0) -> None:
        self.base_url = normalize_service_url(base_url)
        self.headers = {"X-Api-Key": normalize_api_key(api_key, field_name="Sonarr API key")}
        self.timeout = timeout
        # Parsing the same result can be needed by both language filtering and
        # upgrade scoring in a single search. Cache it for the lifetime of this
        # client so Sonarr is not asked to parse the same release twice.
        self._parse_cache: dict[str, dict[str, Any]] = {}

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=False, trust_env=False) as client:
            response = await client.request(method, f"{self.base_url}{path}", headers=self.headers, **kwargs)
            response.raise_for_status()
            if response.status_code == 204 or not response.content:
                return None
            return response.json()

    async def test(self) -> dict[str, Any]:
        return await self._request("GET", "/api/v3/system/status")

    async def series(self) -> list[dict[str, Any]]:
        """Return every TV series known to Sonarr, monitored or not."""
        payload = await self._request("GET", "/api/v3/series")
        return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []

    @staticmethod
    def _season_monitored(series: dict[str, Any], season_number: int, default: bool = True) -> bool:
        """Return Sonarr's monitor state for one season from a SeriesResource."""
        seasons = series.get("seasons") or series.get("Seasons") or []
        if not isinstance(seasons, list):
            return default
        for season in seasons:
            if not isinstance(season, dict):
                continue
            try:
                number = int(season.get("seasonNumber", season.get("SeasonNumber")))
            except (TypeError, ValueError):
                continue
            if number == int(season_number):
                return bool(season.get("monitored", season.get("Monitored", default)))
        return default

    async def queue_records(self, page_size: int = 1000) -> list[dict[str, Any]]:
        """Return all current Sonarr queue records from the v3 API."""
        page = 1
        records: list[dict[str, Any]] = []
        while True:
            params = {
                "page": page,
                "pageSize": page_size,
                "sortKey": "timeleft",
                "sortDirection": "ascending",
                "includeUnknownSeriesItems": "true",
                "includeSeries": "false",
                "includeEpisode": "false",
            }
            payload = await self._request("GET", "/api/v3/queue", params=params)
            if not isinstance(payload, dict):
                break
            batch = payload.get("records") or []
            if isinstance(batch, list):
                records.extend(item for item in batch if isinstance(item, dict))
            try:
                total = int(payload.get("totalRecords") or len(records))
            except (TypeError, ValueError):
                total = len(records)
            if len(records) >= total or not batch:
                break
            page += 1
        return records

    @staticmethod
    def _queue_episode_ids(record: dict[str, Any]) -> set[int]:
        ids: set[int] = set()

        def add(value: Any) -> None:
            try:
                if value is not None:
                    ids.add(int(value))
            except (TypeError, ValueError):
                pass

        add(record.get("episodeId"))
        add(record.get("EpisodeId"))
        for value in record.get("episodeIds") or record.get("EpisodeIds") or []:
            add(value)

        episode = record.get("episode") or record.get("Episode")
        if isinstance(episode, dict):
            add(episode.get("id", episode.get("Id")))

        episodes = record.get("episodes") or record.get("Episodes") or []
        if isinstance(episodes, list):
            for item in episodes:
                if isinstance(item, dict):
                    add(item.get("id", item.get("Id")))
                else:
                    add(item)
        return ids

    @staticmethod
    def _queue_record_blocks_download(record: dict[str, Any]) -> bool:
        # Failed queue entries are terminal and should not prevent Soulseekarr
        # from recovering the still-missing episode. Every other queue state is
        # treated conservatively as Sonarr-owned work, including queued, paused,
        # downloading, delayed, warning, completed-awaiting-import and unknown.
        status = str(record.get("status") or record.get("Status") or "unknown").strip().lower()
        return status != "failed"

    async def queued_episode_ids(self) -> set[int]:
        ids: set[int] = set()
        for record in await self.queue_records():
            if self._queue_record_blocks_download(record):
                ids.update(self._queue_episode_ids(record))
        return ids

    async def queued_episode_ids_excluding_download_ids(
        self, ignored_download_ids: set[str] | None = None
    ) -> set[int]:
        """Return queued episode ids while ignoring selected download-client items.

        Soulseekarr exposes its own active jobs through the read-only qBittorrent
        compatibility API.  UI download-source indicators use this method so an
        episode is marked as Sonarr-downloading only when Sonarr also has work
        owned by another download client.
        """
        ignored = {str(value).casefold() for value in (ignored_download_ids or set()) if value}
        ids: set[int] = set()
        for record in await self.queue_records():
            if not self._queue_record_blocks_download(record):
                continue
            download_id = self._queue_download_id(record).casefold()
            if download_id and download_id in ignored:
                continue
            ids.update(self._queue_episode_ids(record))
        return ids

    async def episode_in_queue(self, episode_id: int) -> bool:
        return int(episode_id) in await self.queued_episode_ids()

    @staticmethod
    def _queue_download_id(record: dict[str, Any]) -> str:
        return str(record.get("downloadId") or record.get("DownloadId") or "").strip()

    async def episode_in_queue_excluding_download_ids(
        self, episode_id: int, ignored_download_ids: set[str] | None = None
    ) -> bool:
        """Check Sonarr queue while ignoring Soulseekarr's own compatibility items.

        Stale-queue retries need to search an episode that is already visible in
        Sonarr Activity through Soulseekarr's read-only qBittorrent facade.  The
        ignored ids are the fake torrent hashes for the episode's current
        Soulseekarr jobs; any other non-failed Sonarr queue item still blocks a
        retry so a real Sonarr download client is never duplicated.
        """
        wanted = int(episode_id)
        ignored = {str(value).casefold() for value in (ignored_download_ids or set()) if value}
        for record in await self.queue_records():
            if not self._queue_record_blocks_download(record):
                continue
            if wanted not in self._queue_episode_ids(record):
                continue
            download_id = self._queue_download_id(record).casefold()
            if download_id and download_id in ignored:
                continue
            return True
        return False

    async def episode(self, episode_id: int) -> dict[str, Any]:
        """Return Sonarr's current record for one episode."""
        payload = await self._request("GET", f"/api/v3/episode/{int(episode_id)}")
        return payload if isinstance(payload, dict) else {}

    async def missing_episode_resources(
        self,
        episode_ids: list[int] | set[int] | tuple[int, ...],
        series_rows: list[dict[str, Any]] | None = None,
        *,
        max_concurrency: int = 6,
    ) -> list[MissingEpisode]:
        """Resolve specific episode ids directly and keep those still missing.

        Sonarr's Wanted/Missing view can lag or temporarily omit an episode after
        a download-client item disappears.  This bounded direct lookup is used
        only for recent Soulseekarr terminal-job recovery hints; it is not a full
        library scan.
        """
        ids = []
        seen: set[int] = set()
        for value in episode_ids:
            try:
                episode_id = int(value)
            except (TypeError, ValueError):
                continue
            if episode_id <= 0 or episode_id in seen:
                continue
            seen.add(episode_id)
            ids.append(episode_id)
        if not ids:
            return []

        if series_rows is None:
            series_rows = await self.series()
        series_by_id: dict[int, dict[str, Any]] = {}
        for row in series_rows or []:
            if not isinstance(row, dict):
                continue
            try:
                series_by_id[int(row.get("id", row.get("Id")))] = row
            except (TypeError, ValueError):
                continue

        semaphore = asyncio.Semaphore(max(1, min(16, int(max_concurrency or 1))))

        async def load(episode_id: int) -> MissingEpisode | None:
            async with semaphore:
                item = await self.episode(episode_id)
            if not isinstance(item, dict) or not item:
                return None
            if bool(item.get("hasFile", item.get("HasFile", False))):
                return None
            try:
                series_id = int(item.get("seriesId", item.get("SeriesId")) or 0)
                season_number = int(item.get("seasonNumber", item.get("SeasonNumber")) or 0)
                episode_number = int(item.get("episodeNumber", item.get("EpisodeNumber")) or 0)
            except (TypeError, ValueError):
                return None
            if series_id <= 0 or episode_number <= 0:
                return None

            series = item.get("series") or item.get("Series") or series_by_id.get(series_id) or {}
            if not isinstance(series, dict):
                series = {}
            series_row = series_by_id.get(series_id) or series
            title = str(
                series_row.get("title", series_row.get("Title"))
                or series.get("title", series.get("Title"))
                or f"Series {series_id}"
            )
            try:
                year = int(
                    series_row.get("year", series_row.get("Year"))
                    or series.get("year", series.get("Year"))
                    or 0
                ) or None
            except (TypeError, ValueError):
                year = None
            series_monitored = bool(
                series_row.get("monitored", series_row.get("Monitored", True))
            )
            season_monitored = self._season_monitored(series_row, season_number, True)
            episode_monitored = bool(item.get("monitored", item.get("Monitored", True)))

            return MissingEpisode(
                id=episode_id,
                series_id=series_id,
                series_title=title,
                season_number=season_number,
                episode_number=episode_number,
                title=str(item.get("title", item.get("Title")) or ""),
                air_date_utc=item.get("airDateUtc", item.get("AirDateUtc")),
                series_year=year,
                series_monitored=series_monitored,
                season_monitored=season_monitored,
                episode_monitored=episode_monitored,
                has_file=False,
                episode_file_id=None,
            )

        loaded = await asyncio.gather(*(load(episode_id) for episode_id in ids), return_exceptions=True)
        episodes = [item for item in loaded if isinstance(item, MissingEpisode)]
        return sorted(
            episodes,
            key=lambda ep: (
                ep.air_date_utc or "", ep.series_title.casefold(),
                ep.season_number, ep.episode_number, ep.id,
            ),
        )

    async def season_episodes(
        self,
        series_id: int,
        season_number: int,
        series_title: str,
        series_year: int | None = None,
        monitored_only: bool = True,
    ) -> list[MissingEpisode]:
        """Return every Sonarr episode in one season, including episodes with files.

        Season-folder expansion needs more than Wanted/Missing: an episode that
        already has a file may still be worth downloading because Sonarr can later
        score the new candidate as a Custom Format upgrade.
        """
        payload = await self._request(
            "GET",
            "/api/v3/episode",
            params={
                "seriesId": int(series_id),
                "seasonNumber": int(season_number),
                "includeSeries": "true",
                "includeEpisodeFile": "true",
            },
        )
        rows = payload if isinstance(payload, list) else []
        episodes: list[MissingEpisode] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            episode_monitored = bool(item.get("monitored", item.get("Monitored", True)))
            series = item.get("series") or item.get("Series") or {}
            season_monitored = self._season_monitored(series if isinstance(series, dict) else {}, season_number, True)
            if monitored_only and (not episode_monitored or not season_monitored):
                continue
            try:
                episode_id = int(item.get("id", item.get("Id")))
                item_series_id = int(item.get("seriesId", item.get("SeriesId")) or series_id)
                item_season = int(item.get("seasonNumber", item.get("SeasonNumber")) or 0)
                item_episode = int(item.get("episodeNumber", item.get("EpisodeNumber")) or 0)
            except (TypeError, ValueError):
                continue
            if item_season != int(season_number) or item_episode <= 0:
                continue
            episode_file = item.get("episodeFile") or item.get("EpisodeFile") or {}
            current_score = None
            current_resolution = None
            current_quality_name = None
            if isinstance(episode_file, dict) and episode_file:
                raw_score = episode_file.get("customFormatScore")
                if raw_score is None:
                    raw_score = episode_file.get("CustomFormatScore")
                try:
                    current_score = int(raw_score) if raw_score is not None else None
                except (TypeError, ValueError):
                    current_score = None
                quality_model = episode_file.get("quality") or episode_file.get("Quality")
                current_resolution = quality_resolution(quality_model) or None
                current_quality_name = quality_name(quality_model)
            episodes.append(
                MissingEpisode(
                    id=episode_id,
                    series_id=item_series_id,
                    series_title=series_title,
                    season_number=item_season,
                    episode_number=item_episode,
                    title=str(item.get("title", item.get("Title")) or ""),
                    air_date_utc=item.get("airDateUtc", item.get("AirDateUtc")),
                    series_year=series_year,
                    series_monitored=bool(series.get("monitored", series.get("Monitored", True))) if isinstance(series, dict) else True,
                    season_monitored=season_monitored,
                    episode_monitored=episode_monitored,
                    has_file=bool(item.get("hasFile", item.get("HasFile", False))),
                    episode_file_id=(
                        int(item.get("episodeFileId", item.get("EpisodeFileId")) or 0) or None
                    ),
                    current_custom_format_score=current_score,
                    current_quality_resolution=current_resolution,
                    current_quality_name=current_quality_name,
                )
            )
        return episodes

    async def series_episodes_all(
        self,
        series_id: int,
        series_title: str,
        series_year: int | None = None,
        monitored_only: bool = True,
    ) -> list[MissingEpisode]:
        """Return all episodes for one Sonarr series, including existing files.

        Automatic upgrade discovery needs an episode target even when Wanted /
        Missing is empty (or every missing episode is on repeat-search
        cooldown). Fetching the series episode list with EpisodeFile data lets
        Soulseekarr include installed episodes in the same season/episode
        round-robin used for missing targets while retaining their current
        profile for strict upgrade validation.
        """
        payload = await self._request(
            "GET",
            "/api/v3/episode",
            params={
                "seriesId": int(series_id),
                "includeSeries": "true",
                "includeEpisodeFile": "true",
            },
        )
        rows = payload if isinstance(payload, list) else []
        episodes: list[MissingEpisode] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            series = item.get("series") or item.get("Series") or {}
            try:
                episode_id = int(item.get("id", item.get("Id")))
                item_series_id = int(item.get("seriesId", item.get("SeriesId")) or series_id)
                item_season = int(item.get("seasonNumber", item.get("SeasonNumber")) or 0)
                item_episode = int(item.get("episodeNumber", item.get("EpisodeNumber")) or 0)
            except (TypeError, ValueError):
                continue
            if item_episode <= 0:
                continue

            episode_monitored = bool(item.get("monitored", item.get("Monitored", True)))
            season_monitored = self._season_monitored(
                series if isinstance(series, dict) else {}, item_season, True
            )
            if monitored_only and (not episode_monitored or not season_monitored):
                continue

            episode_file = item.get("episodeFile") or item.get("EpisodeFile") or {}
            current_score = None
            current_resolution = None
            current_quality_name = None
            if isinstance(episode_file, dict) and episode_file:
                raw_score = episode_file.get("customFormatScore")
                if raw_score is None:
                    raw_score = episode_file.get("CustomFormatScore")
                try:
                    current_score = int(raw_score) if raw_score is not None else None
                except (TypeError, ValueError):
                    current_score = None
                quality_model = episode_file.get("quality") or episode_file.get("Quality")
                current_resolution = quality_resolution(quality_model) or None
                current_quality_name = quality_name(quality_model)

            episodes.append(
                MissingEpisode(
                    id=episode_id,
                    series_id=item_series_id,
                    series_title=series_title,
                    season_number=item_season,
                    episode_number=item_episode,
                    title=str(item.get("title", item.get("Title")) or ""),
                    air_date_utc=item.get("airDateUtc", item.get("AirDateUtc")),
                    series_year=series_year,
                    series_monitored=bool(
                        series.get("monitored", series.get("Monitored", True))
                    ) if isinstance(series, dict) else True,
                    season_monitored=season_monitored,
                    episode_monitored=episode_monitored,
                    has_file=bool(item.get("hasFile", item.get("HasFile", False))),
                    episode_file_id=(
                        int(item.get("episodeFileId", item.get("EpisodeFileId")) or 0) or None
                    ),
                    current_custom_format_score=current_score,
                    current_quality_resolution=current_resolution,
                    current_quality_name=current_quality_name,
                )
            )
        return episodes

    async def episode_file(self, episode_file_id: int) -> dict[str, Any]:
        """Return one Sonarr EpisodeFile, including Custom Format score."""
        payload = await self._request("GET", f"/api/v3/episodefile/{int(episode_file_id)}")
        return payload if isinstance(payload, dict) else {}

    async def installed_episode_quality_profile(self, episode_id: int) -> tuple[int, int, int] | None:
        """Return ``(CF score, resolution, source rank)`` for the installed file."""
        episode = await self.episode(episode_id)
        if not bool(episode.get("hasFile") or episode.get("HasFile")):
            return None
        try:
            file_id = int(episode.get("episodeFileId") or episode.get("EpisodeFileId") or 0)
        except (TypeError, ValueError):
            file_id = 0
        if file_id <= 0:
            return None

        episode_file = await self.episode_file(file_id)
        raw_score = episode_file.get("customFormatScore")
        if raw_score is None:
            raw_score = episode_file.get("CustomFormatScore")
        try:
            score = int(raw_score)
        except (TypeError, ValueError):
            return None
        quality_model = episode_file.get("quality") or episode_file.get("Quality")
        resolution = self.quality_resolution(quality_model)
        if int(resolution or 0) <= 0:
            return None
        return score, int(resolution), quality_source_rank(quality_model)

    async def installed_episode_profile(self, episode_id: int) -> tuple[int, int] | None:
        """Compatibility profile containing only Custom Format score/resolution."""
        profile = await self.installed_episode_quality_profile(episode_id)
        return (profile[0], profile[1]) if profile is not None else None

    async def candidate_release_quality_profile_for_episode_id(
        self, episode_id: int, remote_path: str
    ) -> tuple[int, int, int] | None:
        """Return ``(CF score, resolution, source rank)`` for an active release."""
        try:
            parsed = await self.parse_release(remote_path)
        except Exception:
            return None
        if not release_languages_allowed(parsed_release_languages(parsed)):
            return None

        parsed_episode_ids: set[int] = set()
        for row in parsed.get("episodes") or parsed.get("Episodes") or []:
            try:
                value = row.get("id", row.get("Id")) if isinstance(row, dict) else row
                parsed_episode_ids.add(int(value))
            except (TypeError, ValueError):
                continue
        if parsed_episode_ids != {int(episode_id)}:
            return None

        raw_score = parsed.get("customFormatScore")
        if raw_score is None:
            raw_score = parsed.get("CustomFormatScore")
        try:
            score = int(raw_score or 0)
        except (TypeError, ValueError):
            score = 0
        parsed_info = parsed.get("parsedEpisodeInfo") or parsed.get("ParsedEpisodeInfo") or {}
        quality_model = parsed_info.get("quality") if isinstance(parsed_info, dict) else None
        resolution = self.quality_resolution(quality_model)
        if int(resolution or 0) <= 0:
            return None
        return score, int(resolution), quality_source_rank(quality_model)

    async def candidate_release_profile_for_episode_id(
        self, episode_id: int, remote_path: str
    ) -> tuple[int, int] | None:
        """Compatibility profile containing only Custom Format score/resolution."""
        profile = await self.candidate_release_quality_profile_for_episode_id(episode_id, remote_path)
        return (profile[0], profile[1]) if profile is not None else None

    async def wait_for_episode_file(
        self,
        episode_id: int,
        timeout_seconds: int = 600,
        poll_seconds: float = 2.0,
    ) -> dict[str, Any]:
        """Wait until Sonarr confirms the episode has an EpisodeFile record.

        A completed ManualImport command only means the command handler finished;
        individual imports can still have failed without failing the command itself.
        """
        import asyncio

        deadline = asyncio.get_running_loop().time() + timeout_seconds
        latest: dict[str, Any] = {}
        while asyncio.get_running_loop().time() < deadline:
            latest = await self.episode(episode_id)
            has_file = bool(latest.get("hasFile") or latest.get("HasFile"))
            try:
                file_id = int(latest.get("episodeFileId") or latest.get("EpisodeFileId") or 0)
            except (TypeError, ValueError):
                file_id = 0
            if has_file and file_id > 0:
                return latest
            await asyncio.sleep(poll_seconds)
        return latest

    async def wait_for_episode_file_change(
        self,
        episode_id: int,
        previous_file_id: int,
        timeout_seconds: int = 600,
        poll_seconds: float = 2.0,
        previous_file: dict | None = None,
        expected_size: int = 0,
    ) -> dict[str, Any]:
        """Wait until an upgrade replaces the episode's previous EpisodeFile."""
        import asyncio

        deadline = asyncio.get_running_loop().time() + timeout_seconds
        latest: dict[str, Any] = {}
        while asyncio.get_running_loop().time() < deadline:
            latest = await self.episode(episode_id)
            has_file = bool(latest.get("hasFile") or latest.get("HasFile"))
            try:
                file_id = int(latest.get("episodeFileId") or latest.get("EpisodeFileId") or 0)
            except (TypeError, ValueError):
                file_id = 0
            if has_file and file_id > 0 and file_id != int(previous_file_id):
                return latest
            if has_file and file_id > 0 and previous_file:
                current_file = await self.episode_file(file_id)
                if imported_file_replaced(previous_file, current_file, expected_size):
                    return latest
            await asyncio.sleep(poll_seconds)
        return latest

    async def _missing_episodes_for_monitor_state(
        self, monitored: bool, page_size: int = 1000
    ) -> list[MissingEpisode]:
        """Return one Sonarr Wanted/Missing monitor partition.

        Sonarr's ``monitored`` query flag is a partition, not a simple filter:
        ``true`` returns episodes where both the series and episode are monitored,
        while ``false`` returns rows where either is unmonitored. Fetching both is
        therefore required when Soulseekarr is allowed to opt unmonitored series in.
        """
        page = 1
        result: list[MissingEpisode] = []
        while True:
            params = {
                "page": page,
                "pageSize": page_size,
                "sortKey": "airDateUtc",
                "sortDirection": "ascending",
                "includeSeries": "true",
                "monitored": "true" if monitored else "false",
            }
            payload = await self._request("GET", "/api/v3/wanted/missing", params=params)
            if not isinstance(payload, dict):
                break
            records = payload.get("records") or []
            if not isinstance(records, list):
                records = []
            for item in records:
                if not isinstance(item, dict):
                    continue
                series = item.get("series") or {}
                try:
                    episode_id = int(item["id"])
                    series_id = int(item["seriesId"])
                except (KeyError, TypeError, ValueError):
                    continue
                season_number = int(item.get("seasonNumber", 0) or 0)
                season_monitored = self._season_monitored(series if isinstance(series, dict) else {}, season_number, True)
                result.append(
                    MissingEpisode(
                        id=episode_id,
                        series_id=series_id,
                        series_title=series.get("title") or f"Series {series_id}",
                        season_number=season_number,
                        episode_number=int(item.get("episodeNumber", 0) or 0),
                        title=item.get("title") or "",
                        air_date_utc=item.get("airDateUtc"),
                        series_year=int(series.get("year") or 0) or None,
                        series_monitored=bool(series.get("monitored", True)),
                        season_monitored=season_monitored,
                        episode_monitored=bool(item.get("monitored", item.get("Monitored", True))),
                        has_file=bool(item.get("hasFile", item.get("HasFile", False))),
                        episode_file_id=(
                            int(item.get("episodeFileId", item.get("EpisodeFileId")) or 0) or None
                        ),
                    )
                )
            try:
                total = int(payload.get("totalRecords") or len(result))
            except (TypeError, ValueError):
                total = len(result)
            if page * page_size >= total or not records:
                break
            page += 1
        return result

    async def missing_special_episodes(
        self,
        series_rows: list[dict[str, Any]] | None = None,
        *,
        max_concurrency: int = 6,
    ) -> list[MissingEpisode]:
        """Return no-file Season 00 episodes directly from Sonarr's episode API.

        Sonarr's ``/wanted/missing`` endpoint does not reliably include specials
        whose air date is unknown. Soulseekarr therefore cannot discover those
        rows merely by relaxing its own air-date eligibility rule: the episodes
        never enter the Wanted/Missing result set in the first place.

        Query only series that actually expose a Season 00 entry and fetch that
        season with ``includeEpisodeFile``. Both dated and undated no-file
        specials are returned so this method also covers Sonarr versions that
        omit other specials from Wanted/Missing. Monitoring state is preserved on
        each :class:`MissingEpisode`; callers can apply their normal Soulseekarr
        per-series/season/episode policy afterwards.
        """
        if series_rows is None:
            series_rows = await self.series()

        targets: list[tuple[int, str, int | None]] = []
        for row in series_rows:
            if not isinstance(row, dict):
                continue
            try:
                series_id = int(row.get("id", row.get("Id")))
            except (TypeError, ValueError):
                continue
            seasons = row.get("seasons") or row.get("Seasons") or []
            has_specials = False
            if isinstance(seasons, list):
                for season in seasons:
                    if not isinstance(season, dict):
                        continue
                    try:
                        number = int(season.get("seasonNumber", season.get("SeasonNumber")))
                    except (TypeError, ValueError):
                        continue
                    if number == 0:
                        has_specials = True
                        break
            if not has_specials:
                continue
            title = str(row.get("title", row.get("Title")) or f"Series {series_id}")
            try:
                year = int(row.get("year", row.get("Year")) or 0) or None
            except (TypeError, ValueError):
                year = None
            targets.append((series_id, title, year))

        if not targets:
            return []

        semaphore = asyncio.Semaphore(max(1, min(16, int(max_concurrency or 1))))

        async def load(target: tuple[int, str, int | None]) -> list[MissingEpisode]:
            series_id, title, year = target
            async with semaphore:
                rows = await self.season_episodes(
                    series_id, 0, title, year, monitored_only=False
                )
            return [ep for ep in rows if int(ep.season_number) == 0 and not ep.has_file]

        groups = await asyncio.gather(
            *(load(target) for target in targets),
            return_exceptions=True,
        )
        combined: dict[int, MissingEpisode] = {}
        for group in groups:
            if isinstance(group, Exception):
                # A single broken/removed series should not hide specials from
                # every other TV show. Normal Wanted/Missing results still work.
                continue
            for ep in group:
                combined[int(ep.id)] = ep
        return sorted(
            combined.values(),
            key=lambda ep: (ep.series_title.casefold(), ep.episode_number, ep.id),
        )

    async def cutoff_unmet_episode_ids(self, page_size: int = 1000) -> set[int]:
        """Return Sonarr episode ids currently below their quality-profile cutoff.

        Fetch both monitored partitions for the same reason as Wanted/Missing:
        Soulseekarr can explicitly enable an otherwise unmonitored series. The
        caller treats this endpoint as an optimization/priority signal; normal
        upgrade discovery still works if a Sonarr version does not expose it.
        """
        found: set[int] = set()
        for monitored in (True, False):
            page = 1
            while True:
                params = {
                    "page": page,
                    "pageSize": page_size,
                    "sortKey": "airDateUtc",
                    "sortDirection": "ascending",
                    "includeSeries": "true",
                    "monitored": "true" if monitored else "false",
                }
                payload = await self._request("GET", "/api/v3/wanted/cutoff", params=params)
                if not isinstance(payload, dict):
                    break
                records = payload.get("records") or []
                if not isinstance(records, list):
                    records = []
                for item in records:
                    if not isinstance(item, dict):
                        continue
                    raw_id = item.get("id", item.get("Id"))
                    try:
                        found.add(int(raw_id))
                    except (TypeError, ValueError):
                        continue
                try:
                    total = int(payload.get("totalRecords") or len(found))
                except (TypeError, ValueError):
                    total = len(found)
                if page * page_size >= total or not records:
                    break
                page += 1
        return found

    async def missing_episodes_all(self, page_size: int = 1000) -> list[MissingEpisode]:
        """Return all missing episodes, including unmonitored series/episodes."""
        monitored_rows, unmonitored_rows = await asyncio.gather(
            self._missing_episodes_for_monitor_state(True, page_size),
            self._missing_episodes_for_monitor_state(False, page_size),
        )
        combined = {ep.id: ep for ep in monitored_rows}
        combined.update({ep.id: ep for ep in unmonitored_rows})
        return sorted(
            combined.values(),
            key=lambda ep: (ep.air_date_utc or "", ep.series_title.casefold(), ep.season_number, ep.episode_number),
        )

    async def missing_episodes(self, monitored_only: bool = True, page_size: int = 1000) -> list[MissingEpisode]:
        """Return monitored-only missing episodes, or all when ``monitored_only`` is false."""
        if monitored_only:
            return await self._missing_episodes_for_monitor_state(True, page_size)
        return await self.missing_episodes_all(page_size)


    @staticmethod
    def quality_resolution(quality_model: Any) -> int:
        """Compatibility wrapper around the module-level resolution helper."""
        return quality_resolution(quality_model)

    async def parse_release(self, remote_path: str) -> dict[str, Any]:
        """Ask Sonarr to map/score a release name without downloading it."""
        # slskd strips remote share roots (for example ``G:\\``) when it
        # derives the local destination. Feed Sonarr the same rootless relative
        # path so a peer's Windows drive letter cannot become part of Sonarr's
        # series/path interpretation.
        normalized = strip_remote_path_root(remote_path)
        if normalized in self._parse_cache:
            return self._parse_cache[normalized]
        title = PurePosixPath(normalized).name
        payload = await self._request(
            "GET",
            "/api/v3/parse",
            params={"title": title, "path": normalized},
        )
        parsed = payload if isinstance(payload, dict) else {}
        self._parse_cache[normalized] = parsed
        return parsed

    async def eligible_language_candidates(
        self, candidates: list[SearchCandidate], max_concurrency: int = 8
    ) -> list[SearchCandidate]:
        """Keep only language-safe releases, preserving the input ranking.

        Unknown/unspecified language is accepted. English-only and multi-language
        releases containing English are accepted. A release explicitly parsed as
        foreign-language only is discarded before it can be queued in slskd.
        """
        if not candidates:
            return []

        semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))

        async def allowed(candidate: SearchCandidate) -> bool:
            try:
                async with semaphore:
                    parsed = await self.parse_release(candidate.filename)
            except Exception:
                # Language is a hard pre-download eligibility rule. If Sonarr
                # cannot inspect a candidate, do not guess and spend bandwidth.
                return False
            return release_languages_allowed(parsed_release_languages(parsed))

        decisions = await asyncio.gather(*(allowed(candidate) for candidate in candidates))
        return [candidate for candidate, keep in zip(candidates, decisions) if keep]

    async def candidate_language_allowed(self, candidate: SearchCandidate) -> bool:
        """Check one candidate using the same pre-download language policy."""
        allowed = await self.eligible_language_candidates([candidate], max_concurrency=1)
        return bool(allowed)

    async def eligible_language_candidates_for_episode(
        self,
        episode: MissingEpisode,
        candidates: list[SearchCandidate],
        max_concurrency: int = 8,
    ) -> list[SearchCandidate]:
        """Language-check candidates using the filename Sonarr will import.

        Title-only Season 00 shares often cannot be parsed by Sonarr in their raw
        Soulseek form even though Soulseekarr can identify them by series/title.
        Soulseekarr normalizes every downloaded file to an ``S00E##`` filename
        before ManualImport, so use that exact planned basename for the Sonarr
        language parse as well. This also makes ordinary-episode preflight match
        the release name Sonarr will actually see at import time.
        """
        if not candidates:
            return []
        semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))

        async def allowed(candidate: SearchCandidate) -> bool:
            final_filename = build_import_filename(
                series_title=episode.series_title,
                series_year=episode.series_year,
                episode_code=episode.episode_code,
                episode_title=episode.title,
                remote_filename=candidate.filename,
            )
            try:
                async with semaphore:
                    parsed = await self.parse_release(final_filename)
            except Exception:
                return False
            return release_languages_allowed(parsed_release_languages(parsed))

        decisions = await asyncio.gather(*(allowed(candidate) for candidate in candidates))
        return [candidate for candidate, keep in zip(candidates, decisions) if keep]

    async def candidate_language_allowed_for_episode(
        self, episode: MissingEpisode, candidate: SearchCandidate
    ) -> bool:
        """Check one candidate against the exact planned import filename."""
        allowed = await self.eligible_language_candidates_for_episode(
            episode, [candidate], max_concurrency=1
        )
        return bool(allowed)

    async def candidate_release_quality_profile(
        self, episode: MissingEpisode, remote_path: str
    ) -> tuple[int, int, int] | None:
        """Return ``(CF score, resolution, source rank)`` for one exact release."""
        try:
            parsed = await self.parse_release(remote_path)
        except Exception:
            return None

        if not release_languages_allowed(parsed_release_languages(parsed)):
            return None

        series = parsed.get("series") or parsed.get("Series") or {}
        try:
            parsed_series_id = int(series.get("id") or series.get("Id") or 0)
        except (AttributeError, TypeError, ValueError):
            parsed_series_id = 0
        if parsed_series_id != int(episode.series_id):
            return None

        parsed_episode_ids: set[int] = set()
        for row in parsed.get("episodes") or parsed.get("Episodes") or []:
            try:
                value = row.get("id", row.get("Id")) if isinstance(row, dict) else row
                parsed_episode_ids.add(int(value))
            except (TypeError, ValueError):
                continue
        if parsed_episode_ids != {int(episode.id)}:
            return None

        try:
            score = int(
                parsed.get("customFormatScore")
                if parsed.get("customFormatScore") is not None
                else parsed.get("CustomFormatScore") or 0
            )
        except (TypeError, ValueError):
            score = 0

        parsed_info = parsed.get("parsedEpisodeInfo") or parsed.get("ParsedEpisodeInfo") or {}
        quality_model = parsed_info.get("quality") if isinstance(parsed_info, dict) else None
        resolution = self.quality_resolution(quality_model)
        return score, int(resolution or 0), quality_source_rank(quality_model)

    async def candidate_release_profile(
        self, episode: MissingEpisode, remote_path: str
    ) -> tuple[int, int] | None:
        """Compatibility profile containing only Custom Format score/resolution."""
        profile = await self.candidate_release_quality_profile(episode, remote_path)
        return (profile[0], profile[1]) if profile is not None else None

    async def eligible_upgrade_candidates(
        self,
        episode: MissingEpisode,
        candidates: list[SearchCandidate],
    ) -> list[SearchCandidate]:
        """Return only releases that are real upgrades for an existing episode.

        A candidate must map back to exactly this Sonarr episode/series, be no
        worse in Custom Format score, resolution, or source quality, and be
        strictly better in at least one of those dimensions. Eligible releases
        are ordered by Sonarr Custom Format score first, then resolution, source
        quality, and finally Soulseekarr's normal peer/file score.
        """
        if not candidates:
            return []
        if not episode.has_file:
            return candidates

        file_id = int(episode.episode_file_id or 0)
        if file_id <= 0:
            current_episode = await self.episode(episode.id)
            try:
                file_id = int(
                    current_episode.get("episodeFileId")
                    or current_episode.get("EpisodeFileId")
                    or 0
                )
            except (TypeError, ValueError):
                file_id = 0
        if file_id <= 0:
            # The episode claimed to have a file but Sonarr cannot identify it;
            # do not guess and download a possible downgrade.
            return []

        existing_file = await self.episode_file(file_id)
        try:
            existing_score = int(
                existing_file.get("customFormatScore")
                if existing_file.get("customFormatScore") is not None
                else existing_file.get("CustomFormatScore")
            )
        except (TypeError, ValueError):
            return []
        existing_quality = existing_file.get("quality") or existing_file.get("Quality")
        existing_resolution = self.quality_resolution(existing_quality)
        existing_source_rank = quality_source_rank(existing_quality)

        eligible: list[tuple[int, int, int, float, SearchCandidate]] = []
        for candidate in candidates:
            final_filename = build_import_filename(
                series_title=episode.series_title,
                series_year=episode.series_year,
                episode_code=episode.episode_code,
                episode_title=episode.title,
                remote_filename=candidate.filename,
            )
            try:
                # Score the exact normalized basename that will be presented to
                # Sonarr after the download, not the raw Soulseek path.
                parsed = await self.parse_release(final_filename)
            except Exception:
                # Pre-download upgrade filtering is intentionally conservative:
                # if Sonarr cannot score the release, do not spend bandwidth on it.
                continue

            if not release_languages_allowed(parsed_release_languages(parsed)):
                continue

            series = parsed.get("series") or parsed.get("Series") or {}
            try:
                parsed_series_id = int(series.get("id") or series.get("Id") or 0)
            except (AttributeError, TypeError, ValueError):
                parsed_series_id = 0
            if parsed_series_id != int(episode.series_id):
                continue

            parsed_episode_ids: set[int] = set()
            for row in parsed.get("episodes") or parsed.get("Episodes") or []:
                try:
                    value = row.get("id", row.get("Id")) if isinstance(row, dict) else row
                    parsed_episode_ids.add(int(value))
                except (TypeError, ValueError):
                    continue
            if parsed_episode_ids != {int(episode.id)}:
                continue

            try:
                candidate_score = int(
                    parsed.get("customFormatScore")
                    if parsed.get("customFormatScore") is not None
                    else parsed.get("CustomFormatScore") or 0
                )
            except (TypeError, ValueError):
                candidate_score = 0
            if candidate_score < existing_score:
                continue

            parsed_info = parsed.get("parsedEpisodeInfo") or parsed.get("ParsedEpisodeInfo") or {}
            candidate_quality = parsed_info.get("quality") if isinstance(parsed_info, dict) else None
            candidate_resolution = self.quality_resolution(candidate_quality)
            candidate_source_rank = quality_source_rank(candidate_quality)
            if existing_resolution > 0 and (
                candidate_resolution <= 0 or candidate_resolution < existing_resolution
            ):
                continue
            if candidate_source_rank < existing_source_rank:
                continue

            if (
                candidate_score == existing_score
                and candidate_resolution == existing_resolution
                and candidate_source_rank == existing_source_rank
            ):
                continue

            eligible.append((
                candidate_score, candidate_resolution, candidate_source_rank,
                float(candidate.score), candidate
            ))

        eligible.sort(key=lambda row: (row[0], row[1], row[2], row[3]), reverse=True)
        return [row[4] for row in eligible]


    async def command_status(self, command_id: int) -> dict[str, Any]:
        payload = await self._request("GET", f"/api/v3/command/{command_id}")
        return payload if isinstance(payload, dict) else {}

    async def commands(self) -> list[dict[str, Any]]:
        """Return Sonarr's current command queue/history snapshot."""
        payload = await self._request("GET", "/api/v3/command")
        return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []

    @staticmethod
    def _command_name(command: dict[str, Any]) -> str:
        body = command.get("body") if isinstance(command.get("body"), dict) else {}
        return str(command.get("name") or body.get("name") or "").strip()

    @staticmethod
    def _command_state(command: dict[str, Any]) -> str:
        return str(command.get("status") or command.get("state") or "").strip().casefold()

    async def active_import_commands(self) -> list[dict[str, Any]]:
        """Return queued/running Sonarr commands that can perform media imports.

        Sonarr exposes command activity through ``GET /api/v3/command``.  Manual
        imports use ``ManualImport`` while download-client completion processing
        uses ``DownloadedEpisodesScan`` (older builds may expose the singular
        ``DownloadedEpisodeScan`` name).  Only commands which are still queued or
        running count as active; completed/failed historical rows must not block
        Soulseekarr staging copies.
        """
        import_names = {
            "manualimport",
            "downloadedepisodesscan",
            "downloadedepisodescan",
        }
        active_states = {"queued", "started", "running"}
        active: list[dict[str, Any]] = []
        for command in await self.commands():
            if self._command_name(command).casefold() not in import_names:
                continue
            if self._command_state(command) not in active_states:
                continue
            active.append(command)
        return active

    async def wait_for_command(self, command_id: int, timeout_seconds: int = 600) -> dict[str, Any]:
        import asyncio
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        latest: dict[str, Any] = {}
        while asyncio.get_running_loop().time() < deadline:
            latest = await self.command_status(command_id)
            status = str(latest.get("status") or "").lower()
            if status in {"completed", "failed"}:
                return latest
            await asyncio.sleep(2)
        return latest

    async def manual_import_candidates(self, folder: str) -> list[dict[str, Any]]:
        params = {"folder": folder, "filterExistingFiles": "true"}
        payload = await self._request("GET", "/api/v3/manualimport", params=params)
        return payload if isinstance(payload, list) else []

    @staticmethod
    def _episode_ids(item: dict[str, Any]) -> set[int]:
        ids: set[int] = set()
        for ep in item.get("episodes") or []:
            try:
                ids.add(int(ep["id"]))
            except (KeyError, TypeError, ValueError):
                pass
        return ids

    async def evaluate_import_candidate(
        self, file_path: str, expected_episode_id: int, expected_series_id: int
    ) -> dict[str, Any]:
        """Ask Sonarr to parse a candidate without starting an import."""
        folder = str(PurePosixPath(file_path).parent)
        candidates = await self.manual_import_candidates(folder)
        normalized = str(PurePosixPath(file_path))

        match = None
        for item in candidates:
            path = item.get("path") or ""
            if str(PurePosixPath(path)) == normalized:
                match = item
                break

        if not match:
            return {"ok": False, "reason": "Sonarr did not return this file as a manual-import candidate."}

        episode_ids = self._episode_ids(match)
        if episode_ids != {expected_episode_id}:
            return {
                "ok": False,
                "reason": "Sonarr did not recognize exactly the requested episode.",
                "recognizedEpisodeIds": sorted(episode_ids),
                "recognized": match,
            }

        series = match.get("series") or {}
        if int(series.get("id") or 0) != int(expected_series_id):
            return {
                "ok": False,
                "reason": "Sonarr recognized the file as a different series.",
                "recognizedSeriesId": series.get("id"),
                "recognized": match,
            }

        rejections = match.get("rejections") or []
        if rejections:
            return {
                "ok": False,
                "reason": "Sonarr rejected the file.",
                "rejections": rejections,
                "recognized": match,
            }

        return {"ok": True, "recognized": match}

    async def submit_manual_import(self, recognized: dict[str, Any]) -> dict[str, Any]:
        """Submit a previously evaluated Sonarr manual-import candidate."""
        episode_ids = sorted(self._episode_ids(recognized))
        series = recognized.get("series") or {}
        file_obj = {
            "path": recognized.get("path"),
            "folderName": recognized.get("folderName"),
            "seriesId": series.get("id"),
            "episodeIds": episode_ids,
            "quality": recognized.get("quality"),
            "languages": recognized.get("languages") or [],
            "releaseGroup": recognized.get("releaseGroup") or "",
            "indexerFlags": int(recognized.get("indexerFlags") or 0),
            "releaseType": recognized.get("releaseType"),
        }
        command = {
            "name": "ManualImport",
            "files": [file_obj],
            "importMode": "move",
        }
        response = await self._request("POST", "/api/v3/command", json=command)
        return response if isinstance(response, dict) else {}

    async def import_if_recognized(self, file_path: str, expected_episode_id: int, expected_series_id: int) -> dict[str, Any]:
        """Compatibility helper: evaluate a file and immediately submit it."""
        result = await self.evaluate_import_candidate(file_path, expected_episode_id, expected_series_id)
        if not result.get("ok"):
            return result
        recognized = result.get("recognized") or {}
        response = await self.submit_manual_import(recognized)
        return {"ok": True, "command": response, "recognized": recognized}
