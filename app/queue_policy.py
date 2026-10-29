from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


QUEUE_STRIKE_INTERVAL_SECONDS = 60 * 60
QUEUE_STRIKE_LIMIT = 7 * 24
QUEUE_STRIKE_MAX_PROBES_PER_PASS = 6


def _value(transfer: dict[str, Any] | None, *names: str) -> Any:
    if not transfer:
        return None
    for name in names:
        if name in transfer and transfer[name] is not None:
            return transfer[name]
    return None


def transfer_state(transfer: dict[str, Any] | None) -> str:
    if not transfer:
        return ""
    status_obj = transfer.get("status") if isinstance(transfer.get("status"), dict) else {}
    return str(
        transfer.get("state")
        or transfer.get("State")
        or status_obj.get("state")
        or status_obj.get("State")
        or ""
    )


def transfer_bytes(transfer: dict[str, Any] | None) -> int:
    raw = _value(transfer, "bytesTransferred", "BytesTransferred")
    try:
        return max(0, int(raw or 0))
    except (TypeError, ValueError):
        return 0


def transfer_started_at(transfer: dict[str, Any] | None) -> str | None:
    value = _value(transfer, "startedAt", "StartedAt")
    return str(value) if value else None


def transfer_has_started(transfer: dict[str, Any] | None) -> bool:
    if not transfer:
        return False
    if transfer_started_at(transfer) or transfer_bytes(transfer) > 0:
        return True
    state = transfer_state(transfer).casefold()
    return "initializing" in state or "inprogress" in state or "in progress" in state


def transfer_is_terminal(transfer: dict[str, Any] | None) -> bool:
    """Return True when slskd reports that the transfer attempt has ended.

    ``endedAt`` is not reliable across every slskd/Soulseek.NET state shape, so
    the state text must also be authoritative.  In particular, restored
    transfers can retain non-zero ``bytesTransferred``/``startedAt`` values even
    after they have moved back to a remote queue.
    """
    if not transfer:
        return False
    if _value(transfer, "endedAt", "EndedAt"):
        return True
    state = transfer_state(transfer).casefold().replace(" ", "")
    if not state:
        return False
    return (
        "completed" in state
        or "cancelled" in state
        or "canceled" in state
        or "aborted" in state
        or "errored" in state
        or "timedout" in state
        or "rejected" in state
    )


def transfer_is_queued(transfer: dict[str, Any] | None) -> bool:
    """Return True while slskd is waiting for a peer/remote queue slot.

    Queue state takes precedence over historical progress.  A partially
    downloaded transfer restored after an slskd restart can have both a prior
    ``startedAt``/non-zero byte count *and* a current state of
    ``Queued, Remotely``.  Treating historical progress as proof of current I/O
    inflates the dynamic active-download count and incorrectly applies running
    download timeouts to queued resumes.
    """
    if not transfer or transfer_is_terminal(transfer):
        return False
    state = transfer_state(transfer).casefold()
    return "queued" in state or "requested" in state


def transfer_is_actively_downloading(transfer: dict[str, Any] | None) -> bool:
    """Return True only for a live network-transfer state, not queued/history rows."""
    if not transfer or transfer_is_terminal(transfer) or transfer_is_queued(transfer):
        return False

    state = transfer_state(transfer).casefold().replace(" ", "")
    if any(marker in state for marker in ("inprogress", "initializing", "downloading")):
        return True

    # Be tolerant of older/partial slskd payloads that omit a recognizable state
    # but still carry evidence of an unended started transfer.  Do not count a
    # byte-complete row merely because slskd has not populated endedAt yet.
    if not transfer_has_started(transfer):
        return False
    size = _value(transfer, "size", "Size")
    try:
        total = max(0, int(size or 0))
    except (TypeError, ValueError):
        total = 0
    if total > 0 and transfer_bytes(transfer) >= total:
        return False
    return True


def transfer_queue_position(transfer: dict[str, Any] | None) -> int | None:
    """Return a positive remote queue place, or ``None`` when none was reported."""
    raw = _value(transfer, "placeInQueue", "PlaceInQueue")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def transfer_queue_started_at(transfer: dict[str, Any] | None, fallback: str | None = None) -> datetime | None:
    value = _value(transfer, "enqueuedAt", "EnqueuedAt", "requestedAt", "RequestedAt")
    return parse_utc(str(value) if value else fallback)


def transfer_queue_age_seconds(
    transfer: dict[str, Any] | None,
    fallback: str | None = None,
    *,
    now: datetime | None = None,
) -> float | None:
    started = transfer_queue_started_at(transfer, fallback)
    if started is None:
        return None
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return max(0.0, (current.astimezone(timezone.utc) - started).total_seconds())


def transfer_started_age_seconds(
    transfer: dict[str, Any] | None,
    *,
    now: datetime | None = None,
) -> float | None:
    started = parse_utc(transfer_started_at(transfer))
    if started is None:
        return None
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return max(0.0, (current.astimezone(timezone.utc) - started).total_seconds())
