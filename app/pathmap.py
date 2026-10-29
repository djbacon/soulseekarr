from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
import posixpath
import re
from typing import Iterable


@dataclass(frozen=True)
class ResolvedPathMapping:
    slskd_path: str
    soulseekarr_path: str
    sonarr_path: str
    local_file: str
    sonarr_file: str


def strip_remote_path_root(value: str) -> str:
    """Strip remote share roots that slskd itself discards for local destinations.

    Soulseek peers can expose Windows drive-letter paths (``G:\\TV\\...``),
    UNC roots, or SoulseekQt ``@@share`` prefixes. slskd strips those roots when
    deriving local download destinations, so Soulseekarr must do the same when
    translating the remote path into a relative on-disk lookup path. The
    original remote path must still be used for slskd transfer identity/API calls.
    """
    raw = str(value or '').strip().replace('\\', '/')
    if not raw:
        return ''

    # Windows drive root: G:, G:/, G:\\ (after slash normalization).
    raw = re.sub(r'^[A-Za-z]:(?:/+)?', '', raw, count=1)

    # UNC root: //server/share/... -> share/... (matching slskd StripPathRoot).
    if raw.startswith('//'):
        parts = [part for part in raw[2:].split('/') if part]
        raw = '/'.join(parts[1:]) if len(parts) > 1 else ''

    # SoulseekQt share root: @@abcde/... -> ...
    raw = re.sub(r'^@@[A-Za-z0-9]{5,}(?:/+)?', '', raw, count=1)
    # Repeated separators can arrive from escaped API/test strings; slskd
    # localizes them to ordinary path separators.
    raw = re.sub(r'/+', '/', raw)
    return raw.lstrip('/')



def normalize_remote_transfer_path(value: str) -> str:
    """Normalize a remote Soulseek filename for transfer identity matching.

    slskd may surface equivalent Windows paths with different slash styles or
    casing (for example ``G:\\TV\\Show`` vs ``g:/tv/show``).  Windows drive
    and UNC paths are therefore compared case-insensitively, while ordinary
    Soulseek/Linux paths keep case sensitivity so distinct case-sensitive files
    are not collapsed.  The path root is intentionally preserved; this helper
    is for correlating transfer records, not for deriving local destinations.
    """
    raw = str(value or "")
    if not raw:
        return ""

    windows_style = bool(re.match(r"^[A-Za-z]:", raw)) or raw.startswith("\\\\") or raw.startswith("//")
    normalized = raw.replace("\\", "/")
    if normalized.startswith("//"):
        normalized = "//" + re.sub(r"/+", "/", normalized[2:])
    else:
        normalized = re.sub(r"/+", "/", normalized)
    normalized = normalized.rstrip("/")
    return normalized.casefold() if windows_style else normalized


def remote_transfer_key(username: str, remote_filename: str, size: int) -> tuple[str, str, int]:
    """Return a stable correlation key for a Soulseekarr job and slskd transfer."""
    try:
        normalized_size = int(size)
    except (TypeError, ValueError):
        normalized_size = 0
    return (
        str(username or "").casefold(),
        normalize_remote_transfer_path(remote_filename),
        normalized_size,
    )


def remote_transfer_fallback_key(username: str, remote_filename: str, size: int) -> tuple[str, str, int]:
    """Return a tolerant transfer key used only after exact matching fails.

    slskd can persist/restore the same Soulseek transfer with a slightly
    different representation of the remote path (notably a stripped Windows
    drive/UNC/share root).  The primary transfer identity deliberately preserves
    that root so ordinary distinct paths are not collapsed.  Recovery code may
    use this rootless/case-insensitive key *only when the match is unique*.
    """
    try:
        normalized_size = int(size)
    except (TypeError, ValueError):
        normalized_size = 0
    rootless = strip_remote_path_root(remote_filename)
    normalized = re.sub(r"/+", "/", str(rootless or "").replace("\\", "/")).strip("/").casefold()
    return (str(username or "").casefold(), normalized, normalized_size)

def normalize_container_path(value: str) -> str:
    """Normalize a Linux/container path without allowing lexical traversal.

    ``posixpath.normpath`` collapses ``.``/``..`` before prefix checks. This is
    important for folder mappings because a raw value such as
    ``/downloads/../etc/passwd`` must never be treated as a child of
    ``/downloads``.
    """
    raw = str(value or "").strip().replace("\\", "/")
    if not raw or any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in raw):
        return ""
    normalized = posixpath.normpath(raw)
    if raw.startswith("/"):
        normalized = "/" + normalized.lstrip("/")
    return normalized.rstrip("/") or "/"


def remap_path(path: str, source_root: str, target_root: str) -> str | None:
    """Translate *path* between absolute roots with containment enforcement."""
    normalized_path = normalize_container_path(path)
    source = normalize_container_path(source_root)
    target = normalize_container_path(target_root)
    if not normalized_path or not source or not target:
        return None
    if not (normalized_path.startswith("/") and source.startswith("/") and target.startswith("/")):
        return None

    try:
        relative = posixpath.relpath(normalized_path, source)
    except ValueError:
        return None
    if relative == ".":
        return target
    if relative == ".." or relative.startswith("../"):
        return None

    mapped = normalize_container_path(posixpath.join(target, relative))
    if mapped == target or mapped.startswith(target.rstrip("/") + "/"):
        return mapped
    return None


def clean_mappings(mappings: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    cleaned: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        slskd_path = normalize_container_path(mapping.get("slskd_path", ""))
        soulseekarr_path = normalize_container_path(mapping.get("soulseekarr_path", ""))
        sonarr_path = normalize_container_path(mapping.get("sonarr_path", ""))
        radarr_path = normalize_container_path(mapping.get("radarr_path", ""))
        if radarr_path and (not radarr_path.startswith("/") or radarr_path == "/"):
            radarr_path = ""
        if not (slskd_path and soulseekarr_path and (sonarr_path or radarr_path)):
            continue
        if not all(path.startswith("/") for path in (slskd_path, soulseekarr_path, sonarr_path) if path):
            continue
        # Mapping a service or local data root to '/' turns every absolute path
        # into an eligible file target. It is both unnecessary for normal
        # container layouts and too broad for safe cleanup/discovery.
        if "/" in (slskd_path, soulseekarr_path, sonarr_path):
            continue
        key = (slskd_path, soulseekarr_path, sonarr_path, radarr_path)
        if key in seen:
            continue
        seen.add(key)
        cleaned.append({
            "slskd_path": slskd_path,
            "soulseekarr_path": soulseekarr_path,
            "sonarr_path": sonarr_path,
            **({"radarr_path": radarr_path} if radarr_path else {}),
        })
    return cleaned


def resolve_mapping(slskd_file: str, mappings: Iterable[dict[str, str]]) -> ResolvedPathMapping | None:
    """Resolve a slskd-side file path to Soulseekarr and Sonarr paths.

    The longest matching slskd prefix wins, which allows a broad mapping plus
    a more specific override for a subdirectory.
    """
    normalized_file = normalize_container_path(slskd_file)
    candidates: list[tuple[int, ResolvedPathMapping]] = []
    for mapping in clean_mappings(mappings):
        local_file = remap_path(normalized_file, mapping["slskd_path"], mapping["soulseekarr_path"])
        sonarr_file = remap_path(normalized_file, mapping["slskd_path"], mapping["sonarr_path"])
        if local_file is None or sonarr_file is None:
            continue
        resolved = ResolvedPathMapping(
            slskd_path=mapping["slskd_path"],
            soulseekarr_path=mapping["soulseekarr_path"],
            sonarr_path=mapping["sonarr_path"],
            local_file=local_file,
            sonarr_file=sonarr_file,
        )
        candidates.append((len(mapping["slskd_path"]), resolved))

    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]
