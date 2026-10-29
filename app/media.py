"""Route shared library operations to configured Sonarr and Radarr clients."""
from __future__ import annotations

import asyncio
from dataclasses import replace

from .models import is_movie_id
from .radarr import RadarrClient
from .sonarr import SonarrClient


def media_configured(settings) -> bool:
    return bool((settings.sonarr_url and settings.sonarr_api_key)
                or (settings.radarr_url and settings.radarr_api_key))


def media_client(settings, sonarr_factory=SonarrClient):
    sonarr = sonarr_factory(settings.sonarr_url, settings.sonarr_api_key) if settings.sonarr_url and settings.sonarr_api_key else None
    if settings.radarr_url and settings.radarr_api_key:
        return MediaClient(sonarr, RadarrClient(settings.radarr_url, settings.radarr_api_key))
    return sonarr


def settings_for_target(settings, target_id: int):
    """Select service-visible paths without mutating the saved settings."""
    if not is_movie_id(target_id):
        return settings
    # A missing Radarr mapping uses staging, never a guessed Sonarr path.
    mappings = [{**row, "sonarr_path": row["radarr_path"]}
                for row in settings.folder_mappings if row.get("radarr_path")]
    return replace(settings, sonarr_import_path=settings.radarr_import_path, folder_mappings=mappings)


class MediaClient:
    # Each method's first argument identifies its owner. Command/import calls
    # are intentionally excluded: the runner selects an owner for the whole batch.
    TARGET_METHODS = frozenset({
        "episode", "episode_file", "series_episodes_all", "season_episodes",
        "installed_episode_profile", "installed_episode_quality_profile",
        "candidate_release_profile_for_episode_id", "candidate_release_quality_profile_for_episode_id",
        "candidate_release_profile", "candidate_release_quality_profile",
        "eligible_upgrade_candidates", "eligible_language_candidates_for_episode",
        "candidate_language_allowed_for_episode", "episode_in_queue",
        "episode_in_queue_excluding_download_ids", "wait_for_episode_file", "wait_for_episode_file_change",
    })

    def __init__(self, sonarr, radarr):
        self.sonarr = sonarr
        self.radarr = radarr
        self.base_url = "|".join(client.base_url for client in (sonarr, radarr) if client is not None)

    def for_target(self, target_id: int):
        client = self.radarr if is_movie_id(target_id) else self.sonarr
        if client is None:
            raise ValueError("The media service for this target is not configured")
        return client

    def __getattr__(self, name):
        if name not in self.TARGET_METHODS:
            raise AttributeError(name)

        async def routed(target, *args, **kwargs):
            target_id = target.id if hasattr(target, "id") else target
            return await getattr(self.for_target(target_id), name)(target, *args, **kwargs)
        return routed

    async def _combined(self, method, *args, **kwargs):
        results = await asyncio.gather(*(getattr(client, method)(*args, **kwargs)
                                       for client in (self.sonarr, self.radarr) if client is not None))
        return [item for rows in results for item in rows]

    async def series(self):
        return await self._combined("series")

    async def missing_episodes(self, *args, **kwargs):
        return await self._combined("missing_episodes", *args, **kwargs)

    async def missing_episodes_all(self, *args, **kwargs):
        return await self._combined("missing_episodes_all", *args, **kwargs)

    async def missing_special_episodes(self, series_rows, *args, **kwargs):
        if self.sonarr is None:
            return []
        return await self.sonarr.missing_special_episodes(
            [row for row in series_rows if not is_movie_id(row["id"])], *args, **kwargs)

    async def missing_episode_resources(self, episode_ids, series_rows, *args, **kwargs):
        if self.sonarr is None:
            return []
        return await self.sonarr.missing_episode_resources(
            [key for key in episode_ids if not is_movie_id(key)],
            [row for row in series_rows if not is_movie_id(row["id"])], *args, **kwargs)

    async def queued_episode_ids(self):
        return set(await self._combined("queued_episode_ids"))

    async def queued_episode_ids_excluding_download_ids(self, *args, **kwargs):
        return set(await self._combined("queued_episode_ids_excluding_download_ids", *args, **kwargs))

    async def cutoff_unmet_episode_ids(self):
        return set(await self._combined("cutoff_unmet_episode_ids"))

    async def active_import_commands(self):
        return await self._combined("active_import_commands")
