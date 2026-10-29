"""Age-based cleanup of disposable import staging entries."""
from __future__ import annotations

import logging
import os
from pathlib import Path
import shutil
import stat
import time
from collections.abc import Iterable

logger = logging.getLogger(__name__)
IMPORT_RETENTION_SECONDS = 48 * 60 * 60


def _stale_tree(path: Path, cutoff: float, device: int) -> bool:
    info = path.lstat()
    if info.st_mtime > cutoff or info.st_dev != device or path.is_mount():
        return False
    if stat.S_ISLNK(info.st_mode):
        return False
    if stat.S_ISREG(info.st_mode):
        return True
    if not stat.S_ISDIR(info.st_mode):
        return False
    return all(_stale_tree(child, cutoff, device) for child in path.iterdir())


def cleanup_import_directory(
    root: Path, protected_paths: Iterable[Path] = (), *, now: float | None = None
) -> int:
    """Remove top-level entries inactive for 48 hours, never the root itself.

    A folder is eligible only when all descendants are stale. Symlinks,
    mounted directories and trees that cannot be inspected are left alone.
    """
    if root.is_symlink():
        return 0
    root = root.resolve()
    if root == Path(root.anchor):
        return 0
    cutoff = (time.time() if now is None else now) - IMPORT_RETENTION_SECONDS
    protected = [Path(os.path.abspath(path)) for path in protected_paths]
    removed = 0
    try:
        device = root.stat().st_dev
        entries = list(root.iterdir())
    except OSError:
        logger.warning("Unable to scan import directory %s", root, exc_info=True)
        return 0
    for entry in entries:
        if any(path == entry or entry in path.parents or path in entry.parents for path in protected):
            continue
        try:
            if not _stale_tree(entry, cutoff, device):
                continue
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()
            removed += 1
            logger.info("Removed stale import entry %s", entry)
        except OSError:
            logger.warning("Unable to clean import entry %s", entry, exc_info=True)
    return removed
