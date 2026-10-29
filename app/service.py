from __future__ import annotations

import hashlib
import math
import random
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Settings
from .models import MissingEpisode, SearchCandidate, VIDEO_EXTENSIONS
from .slskd import SlskdClient
from .sonarr import quality_source_rank



DEFAULT_EPISODE_SEARCH_DELAY_HOURS = 48


def episode_search_available_at(ep: MissingEpisode, delay_hours: float = DEFAULT_EPISODE_SEARCH_DELAY_HOURS) -> datetime | None:
    """Return the UTC instant when an episode becomes eligible for Soulseek search.

    Sonarr provides ``airDateUtc``. Soulseekarr waits the configured number of
    hours after that timestamp before searching/downloading so future and
    just-aired episodes can be left to the normal release ecosystem first.
    Missing or invalid timestamps normally remain ineligible. Season 00 specials
    are the exception: Sonarr often has no release date for them, so a special
    with no ``airDateUtc`` is allowed immediately by :func:`episode_search_eligible`,
    as is a movie whose known year is at least two calendar years ago.
    """
    value = str(ep.air_date_utc or "").strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    try:
        delay = max(0.0, float(delay_hours))
    except (TypeError, ValueError, OverflowError):
        delay = float(DEFAULT_EPISODE_SEARCH_DELAY_HOURS)
    return parsed + timedelta(hours=delay)


def episode_search_eligible(
    ep: MissingEpisode,
    now: datetime | None = None,
    delay_hours: float = DEFAULT_EPISODE_SEARCH_DELAY_HOURS,
) -> bool:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    available_at = episode_search_available_at(ep, delay_hours)
    if available_at is None:
        # Only genuinely missing dates get an exception; malformed dates remain
        # ineligible. Movie years provide a fallback without inventing a date.
        if str(ep.air_date_utc or "").strip():
            return False
        if ep.is_movie:
            return isinstance(ep.series_year, int) and 1 <= ep.series_year <= current.year - 2
        return int(ep.season_number) == 0
    return current >= available_at

NOISE = {
    "the", "and", "a", "an", "of", "to", "in", "us", "uk",
    "1080p", "720p", "2160p", "web", "webrip", "webdl", "bluray", "hdtv",
}

# Connector words are useful when checking a short title as a phrase, but they
# are too weak to count as evidence of series identity by themselves.  In
# particular, a title such as "Go On" must never match another series merely
# because that other episode description contains the word "on".
SERIES_CONNECTORS = {
    "the", "and", "a", "an", "of", "to", "in", "on", "at", "for",
    "from", "by", "with", "without", "into", "onto", "as", "is", "are",
}



def order_episodes_for_auto(episodes: list[MissingEpisode], mode: str) -> list[MissingEpisode]:
    """Return missing episodes in the configured automatic-search order.

    Sequential is intentionally deterministic: series title, season, episode, then
    Sonarr episode id. Random returns a shuffled copy and never mutates the input.
    """
    ordered = list(episodes)
    if str(mode or "").lower() == "random":
        random.shuffle(ordered)
        return ordered
    return sorted(
        ordered,
        key=lambda ep: (
            ep.series_title.casefold(),
            int(ep.season_number),
            int(ep.episode_number),
            int(ep.id),
        ),
    )


def order_round_robin_episodes(
    episodes: list[MissingEpisode],
    mode: str,
    series_id: int,
    season_number: int,
) -> list[MissingEpisode]:
    """Return one season's episodes in a stable ring order.

    Sequential walks episode number then Sonarr episode id. Random uses a stable
    pseudo-random permutation for the series/season, so the persistent episode
    cursor can still visit every eligible episode before wrapping instead of
    re-shuffling into accidental repeats on every scheduler pass.
    """
    ordered = sorted(
        list(episodes),
        key=lambda ep: (int(ep.episode_number), int(ep.id)),
    )
    if str(mode or "").lower() != "random":
        return ordered

    def random_key(ep: MissingEpisode) -> bytes:
        value = f"soulseekarr|{int(series_id)}|{int(season_number)}|{int(ep.id)}"
        return hashlib.sha256(value.encode("utf-8")).digest()

    return sorted(ordered, key=random_key)


def order_season_folder_episodes(episodes: list[MissingEpisode]) -> list[MissingEpisode]:
    """Order season-folder siblings by episode, with missing files first.

    Existing files remain upgrade candidates, but their current Custom Format
    score or resolution no longer determines which episode is evaluated first.
    Candidate releases are still individually checked by Sonarr before download.
    """
    missing = sorted(
        (ep for ep in episodes if not ep.has_file),
        key=lambda ep: (int(ep.episode_number), int(ep.id)),
    )
    upgrades = sorted(
        (ep for ep in episodes if ep.has_file),
        key=lambda ep: (int(ep.episode_number), int(ep.id)),
    )
    return missing + upgrades


def order_upgrade_search_episodes(episodes: list[MissingEpisode]) -> list[MissingEpisode]:
    """Return installed episodes in stable season/episode order.

    This is the full upgrade catalog. Per-season target selection is handled by
    :func:`order_upgrade_round_robin_episodes`, which can temporarily narrow the
    ring to episodes below that season's current best Custom Format score.
    """
    return sorted(
        (ep for ep in episodes if ep.has_file),
        key=lambda ep: (int(ep.season_number), int(ep.episode_number), int(ep.id)),
    )


def adaptive_search_delay_hours(
    target_type: str,
    outcome: str,
    streak: int,
    base_hours: float,
) -> float:
    """Return the next automatic-search delay for one outcome.

    ``base_hours`` remains the user's master cooldown control. Setting it to zero
    disables adaptive delays entirely. Missing episodes retry aggressively at
    first and back off when repeated searches stay empty. Upgrade searches are
    intentionally quieter, especially exploratory upgrades that are already at
    or near the season's current quality/CF frontier.
    """
    try:
        base = max(0.0, float(base_hours))
    except (TypeError, ValueError, OverflowError):
        base = 24.0
    if base <= 0:
        return 0.0

    target = str(target_type or "").strip().lower()
    result = str(outcome or "").strip().lower()
    attempt = max(1, int(streak or 1))

    # Transport/API failures are not evidence that Soulseek lacks the release.
    # Retry them soon, but never faster than 15 minutes.
    if result == "error":
        return max(0.25, min(base, 0.5))

    # A Sonarr queue item appearing after the search should be revisited later,
    # but it does not justify the long no-result backoff.
    if result == "external-queue":
        return max(1.0, min(base, 6.0))

    if target == "missing":
        if result == "no-results":
            schedule = (4.0, 8.0, base, max(base, 48.0), max(base, 168.0))
            return schedule[min(attempt - 1, len(schedule) - 1)]
        if result == "no-usable":
            schedule = (min(base, 8.0), base, max(base, 48.0), max(base, 168.0))
            return schedule[min(attempt - 1, len(schedule) - 1)]
        if result == "not-queueable":
            schedule = (min(base, 6.0), min(base, 12.0), base)
            return schedule[min(attempt - 1, len(schedule) - 1)]
        return base

    if target == "upgrade-cutoff":
        if result in {"no-results", "no-usable"}:
            schedule = (max(base, 48.0), max(base, 96.0), max(base, 168.0))
            return schedule[min(attempt - 1, len(schedule) - 1)]
        if result == "not-queueable":
            return max(base, 24.0)
        return max(base, 48.0)

    if target in {"upgrade-weak", "upgrade-quality"}:
        if result in {"no-results", "no-usable"}:
            schedule = (max(base, 72.0), max(base, 168.0))
            return schedule[min(attempt - 1, len(schedule) - 1)]
        if result == "not-queueable":
            return max(base, 24.0)
        return max(base, 72.0)

    # Fully exploratory upgrade checks are the lowest-value searches. One pass
    # per week is enough unless the user explicitly disables cooldowns.
    return max(base, 168.0)


def upgrade_priority_tiers(
    episodes: list[MissingEpisode],
    cutoff_unmet_ids: set[int] | None = None,
) -> dict[str, list[MissingEpisode]]:
    """Split installed episodes into cutoff, CF-weak, quality-weak and exploratory priorities.

    Weak means the episode trails another installed episode in the same season
    by known Custom Format score, resolution, or source quality. Source quality
    uses the conservative order HDTV < WEBRip < WEBDL < Bluray < Remux. Unknown
    metadata is not guessed to be weak and therefore remains exploratory unless
    Sonarr itself reports the episode below cutoff.
    """
    cutoff_ids = {int(value) for value in (cutoff_unmet_ids or set())}
    installed = [ep for ep in episodes if ep.has_file]
    by_season: dict[tuple[int, int], list[MissingEpisode]] = {}
    for ep in installed:
        by_season.setdefault((int(ep.series_id), int(ep.season_number)), []).append(ep)

    weak_score_ids: set[int] = set()
    weak_quality_ids: set[int] = set()
    for rows in by_season.values():
        known_scores = [
            int(ep.current_custom_format_score)
            for ep in rows if ep.current_custom_format_score is not None
        ]
        known_resolutions = [
            int(ep.current_quality_resolution)
            for ep in rows if ep.current_quality_resolution is not None
        ]
        known_source_ranks = [
            quality_source_rank(ep.current_quality_name)
            for ep in rows if quality_source_rank(ep.current_quality_name) > 0
        ]
        best_score = max(known_scores) if known_scores else None
        best_resolution = max(known_resolutions) if known_resolutions else None
        best_source_rank = max(known_source_ranks) if known_source_ranks else None
        for ep in rows:
            weaker_score = (
                best_score is not None
                and ep.current_custom_format_score is not None
                and int(ep.current_custom_format_score) < best_score
            )
            weaker_resolution = (
                best_resolution is not None
                and ep.current_quality_resolution is not None
                and int(ep.current_quality_resolution) < best_resolution
            )
            source_rank = quality_source_rank(ep.current_quality_name)
            weaker_source = (
                best_source_rank is not None
                and source_rank > 0
                and source_rank < best_source_rank
            )
            if weaker_score:
                weak_score_ids.add(int(ep.id))
            elif weaker_resolution or weaker_source:
                weak_quality_ids.add(int(ep.id))

    cutoff: list[MissingEpisode] = []
    weak: list[MissingEpisode] = []
    quality: list[MissingEpisode] = []
    exploratory: list[MissingEpisode] = []
    for ep in installed:
        episode_id = int(ep.id)
        if episode_id in cutoff_ids:
            cutoff.append(ep)
        elif episode_id in weak_score_ids:
            weak.append(ep)
        elif episode_id in weak_quality_ids:
            quality.append(ep)
        else:
            exploratory.append(ep)

    key = lambda ep: (
        ep.series_title.casefold(), int(ep.season_number), int(ep.episode_number), int(ep.id)
    )
    return {
        "upgrade-cutoff": sorted(cutoff, key=key),
        "upgrade-weak": sorted(weak, key=key),
        "upgrade-quality": sorted(quality, key=key),
        "upgrade-exploratory": sorted(exploratory, key=key),
    }


def order_upgrade_round_robin_episodes(episodes: list[MissingEpisode]) -> list[MissingEpisode]:
    """Return the episode ring for one season's proactive upgrade searches.

    When Sonarr reports different installed Custom Format scores in a season,
    only episodes below the highest known score take upgrade-search turns. Those
    weaker episodes rotate in episode-number order. Once every known score is the
    same, the whole season rotates in episode-number order.

    Unknown scores are not guessed to be weaker. If there is no *known* lower
    score, the full season is used so an episode with incomplete metadata is not
    permanently excluded.
    """
    ordered = sorted(
        (ep for ep in episodes if ep.has_file),
        key=lambda ep: (int(ep.episode_number), int(ep.id)),
    )
    known_scores = [
        int(ep.current_custom_format_score)
        for ep in ordered
        if ep.current_custom_format_score is not None
    ]
    if not known_scores:
        return ordered

    highest = max(known_scores)
    weaker = [
        ep for ep in ordered
        if ep.current_custom_format_score is not None
        and int(ep.current_custom_format_score) < highest
    ]
    return weaker or ordered


def normalize_words(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", value.lower())
        if len(token) > 1 and token not in NOISE
    }


def _tokens(value: str) -> list[str]:
    return [token for token in re.findall(r"[a-z0-9]+", value.lower()) if token]


def _series_words(value: str) -> list[str]:
    """Return distinctive title tokens while retaining one-character IDs such as X."""
    return [
        token
        for token in _tokens(value)
        if token not in SERIES_CONNECTORS
        and token not in NOISE
        and token not in {"1080", "720", "2160"}
    ]


def _episode_marker_start(ep: MissingEpisode, value: str) -> int | None:
    """Locate this exact episode marker so text after it cannot identify the series."""
    patterns = [
        rf"(?<!\d)s0*{ep.season_number}[ ._-]*e0*{ep.episode_number}(?!\d)",
        rf"(?<!\d)0*{ep.season_number}[ ._-]*x[ ._-]*0*{ep.episode_number}(?!\d)",
    ]
    starts = []
    for pattern in patterns:
        match = re.search(pattern, value, re.IGNORECASE)
        if match:
            starts.append(match.start())
    return min(starts) if starts else None


def _series_context(ep: MissingEpisode, value: str) -> str:
    """Keep only path text that can plausibly describe the series.

    Search-result episode titles can contain common words from the requested
    series.  Only the path/folder/release prefix before the requested episode
    marker is allowed to contribute to series recognition.
    """
    normalized = value.replace("\\", "/")
    marker = _episode_marker_start(ep, normalized)
    if marker is not None:
        return normalized[:marker]

    parent, _, basename = normalized.rpartition("/")
    season_in_parent = re.search(
        rf"(?:^|[/ ._-])(?:season[ ._-]*0*{ep.season_number}|s0*{ep.season_number})(?:$|[/ ._-])",
        parent,
        re.IGNORECASE,
    )
    episode_only = re.search(
        rf"(?:^|[ ._-])(?:e|ep|episode)[ ._-]*0*{ep.episode_number}(?!\d)",
        basename,
        re.IGNORECASE,
    )
    if season_in_parent and episode_only:
        return f"{parent}/{basename[:episode_only.start()]}"
    return normalized


def _contains_token_phrase(haystack: list[str], needle: list[str]) -> bool:
    if not needle or len(needle) > len(haystack):
        return False
    width = len(needle)
    return any(haystack[i:i + width] == needle for i in range(len(haystack) - width + 1))


def _token_phrase_start(value: str, phrase_tokens: list[str]) -> int | None:
    """Locate a normalized token phrase while allowing release-name punctuation."""
    if not phrase_tokens:
        return None
    pattern = (
        r"(?<![a-z0-9])"
        + r"[^a-z0-9]+".join(re.escape(token) for token in phrase_tokens)
        + r"(?![a-z0-9])"
    )
    match = re.search(pattern, value, re.IGNORECASE)
    return match.start() if match else None


def _special_title_phrases(ep: MissingEpisode) -> list[list[str]]:
    """Return title-token phrases that can identify a Season 00 special.

    Sonarr special titles sometimes redundantly begin with the series title
    (for example ``Scrubs Funniest Moments``). Soulseek shares commonly drop
    that repeated prefix and store the file as ``Scrubs/Specials/Funniest
    Moments.mkv``. Keep the full Sonarr title as the strongest phrase, but also
    allow the episode-specific remainder when the title starts with the exact
    series-title token sequence. Series identity is validated independently by
    :func:`series_match_strength`, so this does not let another show match merely
    because it has a similarly named special.
    """
    if int(ep.season_number) != 0:
        return []
    title_tokens = _tokens(ep.title)
    if not title_tokens:
        return []
    phrases = [title_tokens]
    series_tokens = _tokens(ep.series_title)
    if (
        series_tokens
        and len(title_tokens) > len(series_tokens)
        and title_tokens[:len(series_tokens)] == series_tokens
    ):
        remainder = title_tokens[len(series_tokens):]
        if remainder:
            phrases.append(remainder)
    return phrases


def _special_title_start(ep: MissingEpisode, value: str) -> int | None:
    starts = [
        start
        for phrase in _special_title_phrases(ep)
        if (start := _token_phrase_start(value, phrase)) is not None
    ]
    return min(starts) if starts else None


SERIES_RELEASE_METADATA = {
    "tv", "series", "season", "episode", "ep",
    "amzn", "amazon", "nf", "netflix", "dsnp", "disney",
    "atvp", "apple", "hmax", "hbo", "hulu", "pcok",
    "peacock", "pmtp", "paramount", "web", "webdl", "webrip", "dl",
    "bluray", "bdrip", "brrip", "remux", "hdtv", "dvd",
    "x264", "x265", "h264", "h265", "hevc", "avc", "av1",
    "aac", "ac3", "eac3", "ddp", "dts", "flac", "opus",
    "hdr", "hdr10", "dv", "10bit", "8bit",
    "480p", "576p", "720p", "1080p", "1080i", "2160p", "4320p",
}


def _explicit_series_prefix(ep: MissingEpisode, value: str) -> str | None:
    """Return explicit series text immediately before this episode marker.

    When the basename itself says ``Some Show S01E05`` that text is stronger
    evidence than a parent folder.  This prevents a mislabeled/near-spelling
    basename such as ``The Mich S01E05`` from being accepted for ``The Mick``
    merely because some parent directory happens to contain the wanted title.
    """
    normalized = value.replace("\\", "/")
    for segment in reversed([part for part in normalized.split("/") if part]):
        marker = _episode_marker_start(ep, segment)
        if marker is None:
            # A titled special acts like an episode marker for identifying an
            # explicit series prefix in the basename. This prevents a wrong
            # basename series from being accepted merely because a matching
            # parent directory exists.
            marker = _special_title_start(ep, segment)
        if marker is None:
            continue
        prefix = segment[:marker].strip(" ._-[]()")
        if not prefix:
            return None

        # A numeric/metadata-only prefix is not a series declaration.  Ignore
        # common release/provider tokens so names such as ``AMZN S01E05`` do
        # not become a false conflicting title.
        tokens = _tokens(prefix)
        meaningful = [t for t in tokens if t not in SERIES_RELEASE_METADATA and not re.fullmatch(r"(?:19|20)\d{2}", t)]
        if not meaningful:
            return None
        if all(t.isdigit() for t in meaningful) and not any(t.isdigit() for t in _tokens(ep.series_title)):
            return None
        return prefix
    return None


def _segment_series_strength(ep: MissingEpisode, segment: str) -> float:
    """Score one candidate series segment against the requested Sonarr title."""
    title_tokens = _tokens(ep.series_title)
    distinctive = _series_words(ep.series_title)
    if not distinctive:
        distinctive = [token for token in title_tokens if token not in NOISE]
    if not distinctive:
        return 0.0

    segment_tokens = _tokens(segment)

    # A short title must identify the whole show, not just a word in a
    # different title (Mom versus Teen Mom). Allow only release metadata
    # and years around the complete normalized title phrase.
    if len(set(distinctive)) == 1:
        if not _contains_token_phrase(segment_tokens, title_tokens):
            return 0.0
        identity_tokens = [
            token for token in segment_tokens
            if token in title_tokens or (
                token not in SERIES_RELEASE_METADATA
                and not re.fullmatch(r"(?:19|20)\d{2}", token)
            )
        ]
        if identity_tokens != title_tokens:
            return 0.0
        coverage = 1.0
    else:
        wanted = set(distinctive)
        words = set(_series_words(segment))
        matched = len(wanted & words)
        coverage = matched / len(wanted)
        needed = 2 if len(wanted) <= 3 else max(2, math.ceil(len(wanted) * 0.6))
        if matched < needed:
            return 0.0
        if len(wanted) == 2 and coverage < 1.0:
            return 0.0
        if len(wanted) >= 3 and coverage < 0.6:
            return 0.0

    if ep.series_year:
        years = {int(y) for y in re.findall(r"(?<!\d)((?:19|20)\d{2})(?!\d)", segment)}
        if years and int(ep.series_year) not in years:
            return 0.0
    return coverage

def series_match_strength(ep: MissingEpisode, value: str) -> float:
    """Return series-title coverage, or 0 when the candidate is not this series.

    If the episode filename explicitly names a series immediately before the
    episode marker, that declaration is authoritative and must itself match.
    Parent folders are only used when the basename has no explicit series name.
    """
    explicit_prefix = _explicit_series_prefix(ep, value)
    if explicit_prefix is not None:
        return _segment_series_strength(ep, explicit_prefix)

    context = _series_context(ep, value)
    segments = [segment for segment in context.split("/") if segment.strip()]
    best = 0.0
    for segment in segments:
        best = max(best, _segment_series_strength(ep, segment))
    return best


def _special_title_matches(ep: MissingEpisode, value: str) -> bool:
    """Match a Sonarr special by its episode title when no S00E## marker exists.

    Specials on Soulseek are commonly shared as ``Series Title - Special Title``
    (or under a folder named for the special) rather than as S00E01/S00E02.
    Token-phrase matching keeps punctuation/dot/dash differences harmless while
    still requiring the complete Sonarr episode title in order. Series identity
    is validated separately by ``series_match_strength``.
    """
    return _special_title_start(ep, value.replace("\\", "/")) is not None


def episode_matches(ep: MissingEpisode, value: str) -> bool:
    if ep.is_movie:
        tokens = _tokens(value.replace("\\", "/").rsplit("/", 1)[-1])
        return (_contains_token_phrase(tokens, _tokens(ep.series_title))
                and (not ep.series_year or str(ep.series_year) in tokens)
                and not re.search(r"(?i)\bs\d{1,2}e\d{1,3}\b", value))
    normalized = value.replace("\\", "/")
    season = ep.season_number
    episode = ep.episode_number

    # Accept common zero-padded and non-padded release forms: S03E03, S3E3,
    # S03.E03, 3x03 and 3x3. Boundaries prevent E03 from matching E030.
    direct_patterns = [
        rf"(?<!\d)s0*{season}[ ._-]*e0*{episode}(?!\d)",
        rf"(?<!\d)0*{season}[ ._-]*x[ ._-]*0*{episode}(?!\d)",
    ]
    if any(re.search(pattern, normalized, re.IGNORECASE) for pattern in direct_patterns):
        return True

    # Season 00 releases frequently omit an episode number entirely and use the
    # special's title instead, e.g. ``Example Show - Christmas Special.mkv``.
    # Keep accepting explicit S00E##/0x## forms above, but otherwise match the
    # full Sonarr episode-title phrase.
    if _special_title_matches(ep, normalized):
        return True

    # Some libraries use a season folder plus an episode-only basename, e.g.
    # Season 3/S03E03 is normal, but Season 3/E03.mkv also occurs on Soulseek.
    parent, _, basename = normalized.rpartition("/")
    season_in_parent = re.search(
        rf"(?:^|[/ ._-])(?:season[ ._-]*0*{season}|s0*{season})(?:$|[/ ._-])",
        parent,
        re.IGNORECASE,
    )
    episode_in_name = re.search(
        rf"(?:^|[ ._-])(?:e|ep|episode)[ ._-]*0*{episode}(?!\d)",
        basename,
        re.IGNORECASE,
    )
    return bool(season_in_parent and episode_in_name)


def score_candidate(ep: MissingEpisode, candidate: SearchCandidate, settings: Settings) -> float:
    if candidate.extension not in VIDEO_EXTENSIONS or candidate.is_locked:
        return -math.inf
    size_mb = candidate.size / (1024 * 1024)
    # Season 00 often contains legitimate webisodes, mini-episodes and other
    # short extras that are only a few megabytes. Applying the normal TV-episode
    # minimum size makes those impossible to match (for example a 5-10 MB
    # Community mini episode) even when the series/title/S00E## identity is
    # otherwise exact. Keep a small corruption guard for specials while leaving
    # the configured minimum untouched for ordinary seasons.
    min_size_mb = 1 if int(ep.season_number) == 0 else settings.min_file_size_mb
    if size_mb < min_size_mb or size_mb > settings.max_file_size_mb:
        return -math.inf

    # Soulseek shares often put the series/release metadata in parent folders,
    # so match against the full remote path rather than only the basename.
    name = candidate.match_text.lower()
    if not episode_matches(ep, candidate.match_text):
        return -math.inf

    series_strength = 1.0 if ep.is_movie else series_match_strength(ep, candidate.match_text)
    if series_strength <= 0:
        return -math.inf

    score = 100.0
    score += series_strength * 40.0
    score += min(25.0, math.log10(max(candidate.upload_speed, 1)) * 4.0)

    # A queued peer is still a valid candidate, but longer queues should become
    # progressively less attractive.  The old linear penalty hit its maximum at
    # only ~86 queued files, which made queue lengths such as 100 and 4,000 rank
    # identically.  A logarithmic curve keeps small queues affordable while still
    # distinguishing very long ones; the cap prevents queue length from becoming
    # an absolute rejection criterion.
    queue_length = max(0, int(candidate.queue_length or 0))
    if queue_length:
        score -= min(40.0, 9.0 * math.log10(queue_length + 1.0))

    if ".mkv" in name:
        score += 3.0
    if any(bad in name for bad in ("sample", "trailer", "extras", "featurette")):
        score -= 100.0
    return score


def rank_candidates(ep: MissingEpisode, candidates: list[SearchCandidate], settings: Settings) -> list[SearchCandidate]:
    for candidate in candidates:
        candidate.score = score_candidate(ep, candidate, settings)
    return sorted(
        [c for c in candidates if math.isfinite(c.score)],
        key=lambda c: c.score,
        reverse=True,
    )


def search_queries(ep: MissingEpisode, mode: str = "current") -> list[str]:
    """Return the episode query formats enabled for this TV show.

    Season 00 is special-cased because Soulseek shares are split between
    title-only names and explicit ``S00E##`` names. When Sonarr supplies an
    episode title, keep ``series title + episode title`` as the first variant
    and also expose ``series title + S00E##`` as a fallback variant. Candidate
    matching accepts either the exact special-title phrase or the exact episode
    marker, so both naming styles converge on the same Sonarr episode.

    For ordinary seasons, ``current`` uses Sonarr-style ``S01E01`` only,
    ``legacy`` uses ``1x01`` only, and ``both`` keeps the existing result-driven
    fallback order with the current form first. Invalid/missing preferences fail
    safe to ``current``.
    """
    if ep.is_movie:
        return [f"{ep.series_title} {ep.series_year}".strip() if ep.series_year else ep.series_title]
    if int(ep.season_number) == 0 and str(ep.title or "").strip():
        title = str(ep.title).strip()
        # Do not generate queries such as ``Scrubs Scrubs Funniest Moments``
        # when Sonarr already includes the series title in the special title.
        # The special title by itself is sufficient because it already contains
        # the exact series phrase.
        series_tokens = _tokens(ep.series_title)
        title_tokens = _tokens(title)
        if series_tokens and title_tokens[:len(series_tokens)] == series_tokens:
            title_query = title
        else:
            title_query = f"{ep.series_title} {title}"
        return list(dict.fromkeys([
            title_query,
            f"{ep.series_title} {ep.episode_code}",
        ]))

    current = f"{ep.series_title} {ep.episode_code}"
    legacy = f"{ep.series_title} {ep.season_number}x{ep.episode_number:02d}"
    normalized = str(mode or "current").strip().lower()
    if normalized == "legacy":
        queries = [legacy]
    elif normalized == "both":
        queries = [current, legacy]
    else:
        queries = [current]
    return list(dict.fromkeys(queries))
