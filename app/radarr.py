"""Radarr v3 adapter for the shared target rotation and import pipeline.

The scheduler's historical episode/series fields also represent movies. Every
movie is one target in its own library entry, with a disjoint persisted ID range.
Native IDs are restored only at the Radarr API boundary.
"""
from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any

from .models import MissingEpisode, RADARR_ID_BASE, SearchCandidate
from .sonarr import SonarrClient, quality_name, quality_resolution
from .release_name import build_import_filename


def movie_key(native_id: int) -> int:
    value = int(native_id)
    if not 0 < value < 2**31:
        raise ValueError("Invalid Radarr ID")
    return RADARR_ID_BASE + value


def native_id(key: int) -> int:
    value = int(key) - RADARR_ID_BASE
    if not 0 < value < 2**31:
        raise ValueError("Invalid namespaced Radarr ID")
    return value


class RadarrClient(SonarrClient):
    """Translate movie resources, retaining the shared score/upgrade policy."""

    async def movies(self) -> list[dict[str, Any]]:
        rows = await self._request("GET", "/api/v3/movie")
        if not isinstance(rows, list):
            raise ValueError("Radarr returned an invalid movie catalog")
        self._movies_by_id = {int(row["id"]): row for row in rows}
        return rows

    @staticmethod
    def _library_row(movie: dict[str, Any]) -> dict[str, Any]:
        return {**movie, "id": movie_key(movie["id"]), "media_type": "movie",
                "seasons": [{"seasonNumber": 1, "monitored": movie.get("monitored", False)}]}

    @staticmethod
    def _target(movie: dict[str, Any]) -> MissingEpisode:
        file = movie.get("movieFile") or {}
        release = movie.get("digitalRelease") or movie.get("physicalRelease")
        return MissingEpisode(
            id=movie_key(movie["id"]), series_id=movie_key(movie["id"]),
            series_title=movie.get("title") or "Unknown movie", series_year=movie.get("year"),
            season_number=1, episode_number=1, title=movie.get("title") or "Unknown movie",
            air_date_utc=release, series_monitored=bool(movie.get("monitored")),
            season_monitored=bool(movie.get("monitored")), episode_monitored=bool(movie.get("monitored")),
            has_file=bool(movie.get("hasFile") or movie.get("movieFileId") or file.get("id")),
            episode_file_id=movie_key(file.get("id") or movie["movieFileId"]) if (file.get("id") or movie.get("movieFileId")) else None,
            current_custom_format_score=file.get("customFormatScore"),
            current_quality_resolution=quality_resolution(file.get("quality")) or None,
            current_quality_name=quality_name(file.get("quality")),
        )

    async def series(self) -> list[dict[str, Any]]:
        return [self._library_row(movie) for movie in await self.movies()]

    async def episode(self, episode_id: int) -> dict[str, Any]:
        movie = await self._request("GET", f"/api/v3/movie/{native_id(episode_id)}")
        target = self._target(movie)
        return {**movie, "id": target.id, "seriesId": target.series_id,
                "seasonNumber": 1, "episodeNumber": 1,
                "episodeFileId": target.episode_file_id or 0, "hasFile": target.has_file}

    async def episode_file(self, episode_file_id: int) -> dict[str, Any]:
        return await self._request("GET", f"/api/v3/moviefile/{native_id(episode_file_id)}")

    async def series_episodes_all(self, series_id: int, *args, monitored_only: bool = True, **kwargs) -> list[MissingEpisode]:
        movie = getattr(self, "_movies_by_id", {}).get(native_id(series_id))
        if movie is None:
            movie = await self._request("GET", f"/api/v3/movie/{native_id(series_id)}")
        return [self._target(movie)] if not monitored_only or movie.get("monitored") else []

    async def season_episodes(self, series_id: int, *args, **kwargs) -> list[MissingEpisode]:
        return await self.series_episodes_all(series_id, monitored_only=False)

    async def missing_episodes(self, monitored_only: bool = True, page_size: int = 1000) -> list[MissingEpisode]:
        return [self._target(movie) for movie in await self.movies()
                if not self._target(movie).has_file and (not monitored_only or movie.get("monitored"))]

    async def missing_episodes_all(self, page_size: int = 1000) -> list[MissingEpisode]:
        return await self.missing_episodes(False, page_size)

    async def missing_special_episodes(self, *args, **kwargs) -> list[MissingEpisode]:
        return []

    async def _paged(self, path: str, page_size: int = 1000, **params) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = await self._request("GET", path, params={"page": page, "pageSize": page_size, **params})
            batch = payload.get("records") or []
            records.extend(batch)
            if not batch or len(records) >= int(payload.get("totalRecords") or len(records)):
                return records
            page += 1

    async def cutoff_unmet_episode_ids(self, page_size: int = 1000) -> set[int]:
        found = set()
        for monitored in ("true", "false"):
            found.update(movie_key(row["id"]) for row in await self._paged(
                "/api/v3/wanted/cutoff", page_size, monitored=monitored))
        return found

    async def queue_records(self, page_size: int = 1000) -> list[dict[str, Any]]:
        rows = await self._paged("/api/v3/queue", page_size)
        return [{**row, "episodeId": movie_key(row["movieId"])} for row in rows if row.get("movieId")]

    async def parse_release(self, remote_path: str) -> dict[str, Any]:
        title = build_import_filename(series_title="", series_year=None, episode_code="Movie",
                                      episode_title="", remote_filename=remote_path)
        if title not in self._parse_cache:
            parsed = await self._request("GET", "/api/v3/parse", params={"title": title})
            movie = (parsed or {}).get("movie") or {}
            if not movie.get("id") or parsed.get("customFormatScore") is None:
                raise ValueError("Radarr could not recognize and score the movie release")
            # Adapt the resource shape so Sonarr's exact-identity, language,
            # resolution, source and CF comparisons are used unchanged.
            key = movie_key(movie["id"])
            self._parse_cache[title] = {
                **parsed, "series": {"id": key}, "episodes": [{"id": key}],
                "parsedEpisodeInfo": parsed.get("parsedMovieInfo") or {},
            }
        return self._parse_cache[title]

    async def eligible_language_candidates_for_episode(self, episode: MissingEpisode, candidates: list[SearchCandidate], max_concurrency: int = 8) -> list[SearchCandidate]:
        # Missing movies need exact native identity and scoring too. CF ranking
        # is shared with upgrades; peer ranking remains the final tie-breaker.
        ranked = []
        for candidate in candidates:
            profile = await self.candidate_release_quality_profile(episode, candidate.filename)
            if profile is not None:
                candidate.raw["customFormatScore"] = profile[0]
                ranked.append((*profile, candidate.score, candidate))
        ranked.sort(key=lambda row: row[:4], reverse=True)
        return [row[4] for row in ranked]

    async def active_import_commands(self) -> list[dict[str, Any]]:
        return [command for command in await self.commands()
                if self._command_name(command).casefold() in {"manualimport", "downloadedmoviesscan"}
                and self._command_state(command) in {"queued", "started", "running"}]

    async def evaluate_import_candidate(self, file_path: str, expected_episode_id: int, expected_series_id: int) -> dict[str, Any]:
        if expected_episode_id != expected_series_id:
            return {"ok": False, "reason": "Movie identity mismatch"}
        rows = await self.manual_import_candidates(str(PurePosixPath(file_path).parent))
        match = next((row for row in rows if str(PurePosixPath(row.get("path") or "")) == str(PurePosixPath(file_path))), None)
        if not match:
            return {"ok": False, "reason": "Radarr did not return this file as an import candidate."}
        if int((match.get("movie") or {}).get("id") or 0) != native_id(expected_episode_id):
            return {"ok": False, "reason": "Radarr recognized a different movie.", "recognized": match}
        if match.get("rejections") or match.get("customFormatScore") is None:
            return {"ok": False, "reason": "Radarr rejected or could not score the file.",
                    "recognized": match, "rejections": match.get("rejections") or []}
        return {"ok": True, "recognized": match}

    async def submit_manual_import(self, recognized: dict[str, Any]) -> dict[str, Any]:
        file = {key: recognized.get(key) for key in (
            "path", "folderName", "quality", "languages", "releaseGroup", "indexerFlags", "edition")}
        file["movieId"] = recognized["movie"]["id"]
        file["languages"] = recognized.get("languages") or []
        file["indexerFlags"] = int(recognized.get("indexerFlags") or 0)
        return await self._request("POST", "/api/v3/command", json={
            "name": "ManualImport", "files": [file], "importMode": "move"})
