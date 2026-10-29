from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# Namespace Radarr IDs above the 32-bit IDs used by both upstream APIs.
# Values remain exact in SQLite and JavaScript; existing Sonarr IDs are unchanged.
RADARR_ID_BASE = 1 << 40


def is_movie_id(value: int) -> bool:
    return int(value) >= RADARR_ID_BASE


VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".m4v", ".ts", ".m2ts", ".webm"}


@dataclass
class MissingEpisode:
    id: int
    series_id: int
    series_title: str
    season_number: int
    episode_number: int
    title: str
    air_date_utc: str | None = None
    series_year: int | None = None
    series_monitored: bool = True
    season_monitored: bool = True
    episode_monitored: bool = True
    # season_episodes() also returns episodes that already have files so
    # Soulseekarr can evaluate peer-folder releases as possible upgrades.
    has_file: bool = False
    episode_file_id: int | None = None
    current_custom_format_score: int | None = None
    current_quality_resolution: int | None = None
    current_quality_name: str | None = None

    @property
    def is_movie(self) -> bool:
        return is_movie_id(self.id)

    @property
    def episode_code(self) -> str:
        if self.is_movie:
            return "Movie"
        return f"S{self.season_number:02d}E{self.episode_number:02d}"

    @property
    def label(self) -> str:
        if self.is_movie:
            return f"{self.series_title} ({self.series_year})" if self.series_year else self.series_title
        return f"{self.series_title} {self.episode_code} - {self.title}"


@dataclass
class SearchCandidate:
    username: str
    filename: str
    size: int
    upload_speed: int = 0
    queue_length: int = 0
    is_locked: bool = False
    score: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def basename(self) -> str:
        return self.filename.replace("\\", "/").split("/")[-1]

    @property
    def extension(self) -> str:
        return Path(self.basename).suffix.lower()

    @property
    def match_text(self) -> str:
        """Full remote path, including parent folders that often contain release metadata."""
        return self.filename.replace("\\", "/")
