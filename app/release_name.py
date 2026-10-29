from __future__ import annotations

import re
from pathlib import Path


_PROVIDER_PATTERNS: list[tuple[str, str]] = [
    (r"(?<![a-z0-9])AMZN(?![a-z0-9])|\bAmazon\b", "AMZN"),
    (r"(?<![a-z0-9])NF(?![a-z0-9])|\bNetflix\b", "NF"),
    (r"(?<![a-z0-9])DSNP(?![a-z0-9])|\bDisney\+?\b", "DSNP"),
    (r"(?<![a-z0-9])ATVP(?![a-z0-9])|\bApple\s*TV\+?\b", "ATVP"),
    (r"(?<![a-z0-9])HMAX(?![a-z0-9])|\bHBO\s*Max\b", "HMAX"),
    (r"(?<![a-z0-9])HULU(?![a-z0-9])", "HULU"),
    (r"(?<![a-z0-9])PCOK(?![a-z0-9])|\bPeacock\b", "PCOK"),
    (r"(?<![a-z0-9])PMTP(?![a-z0-9])|\bParamount\+?\b", "PMTP"),
]

_SOURCE_PATTERNS: list[tuple[str, str]] = [
    (r"\b(?:web[ ._-]?dl|webdl)\b", "WEBDL"),
    (r"\bweb[ ._-]?rip\b", "WEBRip"),
    # Some releases intentionally use only the generic source token ``WEB``
    # (for example ``1080p.NF.WEB.DDP``). Sonarr does not use a plain WEB
    # quality, so normalize that ambiguous token to WEBDL. More-specific
    # WEB-DL/WEBRip patterns stay above this one and retain their exact labels.
    (r"\bweb\b", "WEBDL"),
    (r"\bblu[ ._-]?ray\b|\bbluray\b|\bbdrip\b|\bbrrip\b", "Bluray"),
    (r"\bremux\b", "REMUX"),
    (r"\bhdtv\b", "HDTV"),
    (r"\bdvd(?:rip)?\b", "DVD"),
]

_AUDIO_PATTERNS: list[tuple[str, str]] = [
    (r"\btrue[ ._-]?hd\b", "TrueHD"),
    (r"\bdts[ ._-]?(?:hd[ ._-]?)?ma\b", "DTS-HD MA"),
    (r"\bdts[ ._-]?x\b", "DTS-X"),
    (r"\be[ ._-]?ac[ ._-]?3\b|\bddp\b|\bdd\+\b", "EAC3"),
    (r"\bac[ ._-]?3\b|\bdd\b", "AC3"),
    (r"\baac\b", "AAC"),
    (r"\bflac\b", "FLAC"),
    (r"\bopus\b", "Opus"),
]


def _first_match(text: str, patterns: list[tuple[str, str]]) -> str | None:
    for pattern, value in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return value
    return None


def _safe_component(value: str) -> str:
    # Keep normal release-name punctuation but remove characters that are unsafe
    # as filename components on common filesystems / network shares.
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "-", value)
    value = re.sub(r"\s+", " ", value).strip(" .-")
    return value or "Unknown"


_RELEASE_GROUP_RESERVED = {
    "dl", "rip", "ray", "web", "webrip", "webdl", "web-dl", "hdtv",
    "remux", "bluray", "blu-ray", "bdrip", "brrip", "dvd", "dvdrip",
    "amzn", "nf", "dsnp", "atvp", "hmax", "hulu", "pcok", "pmtp",
    "x264", "x265", "h264", "h265", "hevc", "avc", "av1",
    "aac", "ac3", "eac3", "ddp", "dd", "dts", "flac", "opus",
    "truehd", "atmos", "ma", "480p", "576p", "720p", "1080p",
    "1080i", "2160p", "4320p", "8bit", "10bit", "hdr", "hdr10",
    "hdr10+", "dv", "dovi", "sdr", "5.1", "7.1", "2.0", "2.1", "1.0",
}


def _is_release_group_candidate(value: str, metadata_values: set[str]) -> bool:
    candidate = str(value or "").strip().strip("[](){}")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{1,30}", candidate):
        return False
    lowered = candidate.lower()
    if lowered in _RELEASE_GROUP_RESERVED or lowered in metadata_values:
        return False
    if re.fullmatch(r"(?:19|20)\d{2}", candidate):
        return False
    return True


def _release_group(remote_filename: str, metadata_values: set[str]) -> str | None:
    normalized = remote_filename.replace("\\", "/")
    parts = [part for part in normalized.split("/") if part]
    basename = parts[-1] if parts else normalized
    stem = Path(basename).stem

    # The standard scene/P2P suffix is the strongest signal: ...-SiGMA.mkv
    match = re.search(r"-([A-Za-z0-9][A-Za-z0-9._]{1,30})$", stem)
    if match and _is_release_group_candidate(match.group(1), metadata_values):
        return match.group(1)

    # Some library naming schemes put all release metadata in a final
    # parenthesized block and use the final token as the release group, e.g.
    # ``(1080p NF WEB-DL x265 Panda)``. Only apply this heuristic when the
    # block clearly contains technical release metadata so ordinary episode
    # title suffixes such as ``(Part 2)`` are never mistaken for a group.
    parenthesized = re.search(r"\(([^()]*)\)\s*$", stem)
    if parenthesized:
        block = parenthesized.group(1).strip()
        has_release_metadata = bool(
            re.search(
                r"(?i)(?:\b(?:4320p|2160p|1080p|1080i|720p|576p|480p)\b|"
                r"\b(?:web[ ._-]?dl|web[ ._-]?rip|bluray|blu[ ._-]?ray|hdtv|remux|dvdrip|bdrip|brrip)\b|"
                r"\b(?:x264|x265|h264|h265|hevc|avc|av1)\b|"
                r"\b(?:AMZN|NF|DSNP|ATVP|HMAX|HULU|PCOK|PMTP)\b)",
                block,
            )
        )
        if has_release_metadata:
            tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9._+-]*", block)
            if tokens:
                candidate = tokens[-1].rstrip("+")
                # Also understand compact final tokens such as x265-Panda.
                codec_group = re.fullmatch(
                    r"(?i)(?:x264|x265|h264|h265|hevc|avc|av1)-([A-Za-z0-9][A-Za-z0-9._-]{1,30})",
                    candidate,
                )
                if codec_group:
                    candidate = codec_group.group(1)
                if _is_release_group_candidate(candidate, metadata_values):
                    return candidate

    # Some Soulseek libraries keep the group in a final [Group] token.
    for segment in reversed(parts):
        segment_stem = Path(segment).stem
        match = re.search(r"\[([A-Za-z0-9][A-Za-z0-9._-]{1,30})\]\s*$", segment_stem)
        if match and _is_release_group_candidate(match.group(1), metadata_values):
            return match.group(1)

    # Pack folders commonly end their metadata block with " - Group)".
    for segment in reversed(parts[:-1]):
        match = re.search(r"\s-\s*([A-Za-z0-9][A-Za-z0-9._-]{1,30})\)\s*(?:\[[^\]]+\])?\s*$", segment)
        if match and _is_release_group_candidate(match.group(1), metadata_values):
            return match.group(1)
    return None



def extract_quality(remote_filename: str) -> str:
    """Return a compact Sonarr-style quality label from a full release path.

    The full path is intentionally inspected because Soulseek pack/season folders
    often carry the source and resolution while the episode basename is plain.
    """
    context = str(remote_filename or "").replace("\\", "/")
    source = _first_match(context, _SOURCE_PATTERNS)
    resolution_match = re.search(
        r"(?<!\d)(4320p|2160p|1080p|1080i|720p|576p|480p)(?!\d)",
        context,
        re.IGNORECASE,
    )
    resolution = resolution_match.group(1).lower() if resolution_match else None
    is_remux = bool(re.search(r"\bremux\b", context, re.IGNORECASE))
    if source and resolution:
        quality = f"{source}-{resolution}"
        # Sonarr distinguishes Blu-ray remuxes from ordinary Blu-ray encodes.
        # Preserve that modifier in the planned filename so pre-enqueue scoring
        # and the eventual imported filename describe the same quality.
        if source == "Bluray" and is_remux:
            quality += " Remux"
        return quality
    return source or resolution or "Unknown"

def repair_planned_filename_quality(remote_filename: str, planned_filename: str) -> str:
    """Repair source/quality detail lost by older planned-filename builders.

    Planned names are persisted on active jobs. Upgrading Soulseekarr therefore
    should preserve source detail discovered in the original Soulseek path,
    including generic ``WEB`` releases normalized for Sonarr and the Blu-ray
    Remux modifier.
    """
    planned = str(planned_filename or "")
    if not planned:
        return planned
    quality = extract_quality(remote_filename)
    if quality == "Unknown":
        return planned

    # Older builds could preserve the Blu-ray source/resolution but lose only
    # the Remux modifier.
    if quality.casefold().endswith(" remux"):
        base_quality = quality[: -len(" Remux")]
        pattern = re.compile(rf"\[{re.escape(base_quality)}\]", re.IGNORECASE)
        if pattern.search(planned):
            return pattern.sub(f"[{quality}]", planned, count=1)

    # v0.3.145 preserved standalone WEB literally, but Sonarr does not use a
    # plain WEB quality. Normalize persisted [WEB-1080p]-style tags to the
    # current Sonarr-compatible [WEBDL-1080p] form before preparation/import.
    if quality.casefold().startswith("webdl-"):
        resolution = quality.split("-", 1)[1]
        legacy_web = re.compile(rf"\[WEB-{re.escape(resolution)}\]", re.IGNORECASE)
        if legacy_web.search(planned):
            return legacy_web.sub(f"[{quality}]", planned, count=1)

    # Older builds that did not recognize a source token persisted a
    # resolution-only tag such as [1080p]. Upgrade that tag to the full
    # source-qualified quality while the job is still safe to rename.
    quality_match = re.fullmatch(
        r".+-(4320p|2160p|1080p|1080i|720p|576p|480p)(?: Remux)?",
        quality,
        re.IGNORECASE,
    )
    if quality_match:
        resolution = quality_match.group(1)
        pattern = re.compile(rf"\[{re.escape(resolution)}\]", re.IGNORECASE)
        if pattern.search(planned):
            return pattern.sub(f"[{quality}]", planned, count=1)

    return planned


def build_import_filename(
    *,
    series_title: str,
    series_year: int | None,
    episode_code: str,
    episode_title: str,
    remote_filename: str,
) -> str:
    """Build a Sonarr-friendly filename using metadata from the full Soulseek path.

    Parent directories are intentionally part of the metadata source because many
    Soulseek shares use short episode filenames inside richly named season/pack folders.
    """
    # Keep movie release tokens intact and only supply a missing release group.
    # The raw release must be independently recognized by Radarr before queueing.
    if episode_code == "Movie":
        name = _safe_component(remote_filename.replace("\\", "/").rsplit("/", 1)[-1])
        if not _release_group(name, set()):
            extension = Path(name).suffix
            stem = name[:-len(extension)] if extension else name
            name = f"{stem}-SLSKD{extension}"
        return name

    context = remote_filename.replace("\\", "/")
    basename = context.rsplit("/", 1)[-1]
    extension = Path(basename).suffix.lower()
    if not extension or len(extension) > 8:
        extension = ".mkv"

    provider = _first_match(context, _PROVIDER_PATTERNS)
    source = _first_match(context, _SOURCE_PATTERNS)
    resolution_match = re.search(r"(?<!\d)(4320p|2160p|1080p|1080i|720p|576p|480p)(?!\d)", context, re.IGNORECASE)
    resolution = resolution_match.group(1).lower() if resolution_match else None

    audio = _first_match(context, _AUDIO_PATTERNS)
    channels_match = re.search(r"(?<!\d)(7\.1|5\.1|2\.1|2\.0|1\.0)(?!\d)", context)
    channels = channels_match.group(1) if channels_match else None
    if audio and channels:
        audio = f"{audio} {channels}"

    video = None
    if re.search(r"\b(?:x|h[ .]?)265\b|\bhevc\b", context, re.IGNORECASE):
        video = "h265"
    elif re.search(r"\b(?:x|h[ .]?)264\b|\bavc\b", context, re.IGNORECASE):
        video = "h264"
    elif re.search(r"\bav1\b", context, re.IGNORECASE):
        video = "AV1"

    bit_depth = "10bit" if re.search(r"\b10[ ._-]?bit\b", context, re.IGNORECASE) else None
    hdr = None
    if re.search(r"\bhdr10\+\b", context, re.IGNORECASE):
        hdr = "HDR10+"
    elif re.search(r"\bhdr10\b", context, re.IGNORECASE):
        hdr = "HDR10"
    elif re.search(r"\b(?:dolby[ ._-]?vision|dovi|dv)\b", context, re.IGNORECASE):
        hdr = "DV"

    quality = extract_quality(context)
    if quality == "Unknown":
        quality = None

    metadata_values = {
        value.lower()
        for value in (provider, source, resolution, audio, video, bit_depth, hdr, quality)
        if value
    }
    group = _release_group(remote_filename, metadata_values) or "SLSKD"

    if not series_year:
        year_match = re.search(rf"{re.escape(series_title)}[^\n]{{0,24}}\((19\d{{2}}|20\d{{2}})\)", context, re.IGNORECASE)
        if year_match:
            series_year = int(year_match.group(1))

    title = _safe_component(series_title)
    if series_year and not re.search(rf"\({int(series_year)}\)\s*$", title):
        title += f" ({int(series_year)})"
    episode = _safe_component(episode_title)
    base = f"{title} - {episode_code} - {episode}"

    tags = [value for value in (provider, quality, audio, video, bit_depth, hdr) if value]
    if tags:
        base += " " + "".join(f"[{_safe_component(tag)}]" for tag in tags)
    if group:
        base += f"-{_safe_component(group)}"
    return base + extension
