from __future__ import annotations

import asyncio
import base64
import json
import logging
import uuid
from typing import Any, Callable
from urllib.parse import quote

import httpx

from .config import normalize_api_key, normalize_service_url

from .models import SearchCandidate
from .pathmap import remote_transfer_fallback_key, remote_transfer_key, strip_remote_path_root


logger = logging.getLogger(__name__)


class SlskdPeerDirectoryUnavailable(RuntimeError):
    """Raised when slskd cannot retrieve a peer directory after brief retries."""


# Shared per event-loop and slskd URL so manual searches, auto searches, and
# separate client instances cannot create a burst together.
_SEARCH_GATES: dict[tuple[int, str], dict[str, Any]] = {}

# slskd serializes transfer enqueue operations internally and can return HTTP 429
# when season-folder expansion starts several JobRunner tasks at once. Keep one
# shared enqueue lane per event-loop/slskd instance and retry transient 429s.
_ENQUEUE_GATES: dict[tuple[int, str], asyncio.Lock] = {}

# Asking for a queued download's position is not a passive metadata read: slskd
# sends a queue-position request to the remote Soulseek peer.  Downloads pages
# can refresh every few seconds and Sonarr can poll the qBittorrent-compatible
# API independently, so queue-position checks need their own shared throttle.
#
# A queued transfer is checked at most once per minute, and only one position
# request is allowed every five seconds across the whole slskd instance.  With
# a large queue this intentionally becomes a slow round-robin refresh instead
# of a burst of remote requests.
QUEUE_POSITION_REFRESH_SECONDS = 60.0
QUEUE_POSITION_MIN_INTERVAL_SECONDS = 5.0
_QUEUE_POSITION_GATES: dict[tuple[int, str], dict[str, Any]] = {}


class SlskdClient:
    def __init__(self, base_url: str, api_key: str, timeout: float = 20.0) -> None:
        self.base_url = normalize_service_url(base_url)
        self.headers = {"X-API-Key": normalize_api_key(api_key, field_name="slskd API key")}
        self.timeout = timeout
        self._sleep = asyncio.sleep
        self.last_search_diagnostics: dict[str, int | str | bool] = {}

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float | None:
        value = response.headers.get("Retry-After")
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except (TypeError, ValueError):
            return None

    async def _create_search(self, payload: dict[str, Any]) -> None:
        """Create a slskd search without bursting through its rate limit."""
        loop = asyncio.get_running_loop()
        key = (id(loop), self.base_url)
        gate = _SEARCH_GATES.get(key)
        if gate is None:
            gate = {"lock": asyncio.Lock(), "next": 0.0}
            _SEARCH_GATES[key] = gate

        async with gate["lock"]:
            # A small spacing avoids the common burst-limit case even before a 429.
            initial_wait = float(gate["next"]) - loop.time()
            if initial_wait > 0:
                await self._sleep(initial_wait)

            last_error: httpx.HTTPStatusError | None = None
            for attempt in range(6):
                try:
                    await self._request("POST", "/api/v0/searches", json=payload)
                    gate["next"] = loop.time() + 1.25
                    return
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code != 429:
                        raise
                    last_error = exc
                    # Prefer slskd's Retry-After when it supplies one. Otherwise use
                    # bounded exponential backoff. Holding the shared lock stops
                    # sibling/manual/automatic queries from creating a retry storm.
                    retry_after = self._retry_after_seconds(exc.response)
                    fallback = min(8.0, 1.5 * (2 ** attempt))
                    delay = max(retry_after or 0.0, fallback)
                    gate["next"] = loop.time() + delay
                    if attempt >= 5:
                        raise
                    await self._sleep(delay)

            if last_error is not None:
                raise last_error
            raise RuntimeError("slskd search could not be created")

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=False, trust_env=False) as client:
            response = await client.request(method, f"{self.base_url}{path}", headers=self.headers, **kwargs)
            response.raise_for_status()
            if response.status_code == 204 or not response.content:
                return None
            # Some slskd action endpoints return JSON true; tolerate plain text too.
            try:
                return response.json()
            except ValueError:
                return response.text

    async def test(self) -> Any:
        return await self._request("GET", "/api/v0/application")

    @staticmethod
    def _response_items(payload: Any) -> list[dict[str, Any]]:
        """Normalize slskd search-response payloads across API versions.

        Current slskd returns an array of peer response objects from
        ``/searches/{id}/responses``.  Be deliberately permissive here because
        older builds and proxies have returned wrappers, PascalCase property
        names, username-keyed objects, and occasionally double-encoded JSON.
        """
        if payload is None:
            return []

        # A proxy or compatibility layer can double-encode the JSON document.
        if isinstance(payload, str):
            value = payload.strip()
            if value and value[:1] in {"[", "{"}:
                try:
                    return SlskdClient._response_items(json.loads(value))
                except (TypeError, ValueError):
                    return []
            return []

        if isinstance(payload, list):
            items: list[dict[str, Any]] = []
            for item in payload:
                if not isinstance(item, dict):
                    continue
                files = item.get("files", item.get("Files"))
                filename = item.get("filename", item.get("Filename"))
                if isinstance(files, list) or filename:
                    items.append(item)
                else:
                    items.extend(SlskdClient._response_items(item))
            return items

        if not isinstance(payload, dict):
            return []

        files = payload.get("files", payload.get("Files"))
        filename = payload.get("filename", payload.get("Filename"))
        if isinstance(files, list) or filename:
            return [payload]

        for key in ("responses", "Responses", "items", "Items", "results", "Results", "data", "Data", "value", "Value"):
            if key in payload:
                nested = SlskdClient._response_items(payload.get(key))
                if nested:
                    return nested

        # Some wrappers key peer responses by username. Preserve that username
        # if the nested object does not repeat it.
        keyed: list[dict[str, Any]] = []
        for key, value in payload.items():
            if not isinstance(value, dict):
                continue
            nested_files = value.get("files", value.get("Files"))
            if isinstance(nested_files, list):
                item = dict(value)
                item.setdefault("username", item.get("Username") or str(key))
                keyed.append(item)
        return keyed

    @staticmethod
    def _candidates_from_payload(payload: Any) -> tuple[list[SearchCandidate], int]:
        responses = SlskdClient._response_items(payload)
        candidates: list[SearchCandidate] = []
        for response in responses:
            # Tolerate flattened rows and both System.Text.Json camelCase and
            # PascalCase compatibility payloads.
            direct_filename = response.get("filename", response.get("Filename"))
            if direct_filename:
                file_rows = [response]
            else:
                file_rows = list(response.get("files", response.get("Files")) or [])
                locked_rows = response.get("lockedFiles", response.get("LockedFiles")) or []
                for locked in locked_rows:
                    if isinstance(locked, dict):
                        locked_copy = dict(locked)
                        locked_copy["isLocked"] = True
                        file_rows.append(locked_copy)

            username = str(response.get("username", response.get("Username")) or "")
            speed = int(response.get("uploadSpeed", response.get("UploadSpeed")) or 0)
            queue = int(response.get("queueLength", response.get("QueueLength")) or 0)
            for file in file_rows:
                if not isinstance(file, dict):
                    continue
                filename = str(
                    file.get("filename", file.get("Filename"))
                    or file.get("fullName", file.get("FullName"))
                    or ""
                )
                if not filename:
                    continue

                # Some slskd/API compatibility shapes return only the basename
                # on the file row and keep its remote folder separately. Preserve
                # that directory context whenever it is available: TV matching
                # intentionally inspects multiple ancestor folders because the
                # immediate parent may just be "Mini Episodes", "Featurettes",
                # "Other", etc., while the real series name sits higher up.
                if "\\" not in filename and "/" not in filename:
                    directory = ""
                    for source in (file, response):
                        directory = str(
                            source.get("directory", source.get("Directory"))
                            or source.get("folder", source.get("Folder"))
                            or ""
                        ).strip()
                        if directory:
                            break
                    if directory:
                        filename = f"{directory.rstrip('/\\')}\\{filename.lstrip('/\\')}"
                candidates.append(
                    SearchCandidate(
                        username=str(file.get("username", file.get("Username")) or username),
                        filename=filename,
                        size=int(file.get("size", file.get("Size")) or 0),
                        upload_speed=int(file.get("uploadSpeed", file.get("UploadSpeed")) or speed),
                        queue_length=int(file.get("queueLength", file.get("QueueLength")) or queue),
                        is_locked=bool(file.get("isLocked", file.get("IsLocked")) or False),
                        raw=file,
                    )
                )
        return candidates, len(responses)

    async def search_responses(self, search_id: str) -> Any:
        return await self._request("GET", f"/api/v0/searches/{search_id}/responses")

    async def search(
        self,
        text: str,
        search_timeout_seconds: int = 15,
        response_limit: int = 200,
        on_finished: Callable[[str], None] | None = None,
    ) -> list[SearchCandidate]:
        search_id = str(uuid.uuid4())
        payload = {
            "id": search_id,
            "searchText": text,
            # slskd's HTTP DTO describes this value as seconds, but it passes
            # the integer straight through to Soulseek.NET SearchOptions, where
            # SearchTimeout is milliseconds (default 15000). Keep Soulseekarr's
            # setting in human-friendly seconds and convert at the API boundary.
            # The timeout is measured from the last received response.
            "searchTimeout": int(search_timeout_seconds) * 1000,
            "fileLimit": 10000,
            "responseLimit": response_limit,
            "filterResponses": True,
            "maximumPeerQueueLength": 1000000,
            "minimumPeerUploadSpeed": 0,
            "minimumResponseFileCount": 1,
        }
        created = False
        self.last_search_diagnostics = {
            "stateResponseCount": 0,
            "stateFileCount": 0,
            "responseObjects": 0,
            "collectedFiles": 0,
            "endpointFetches": 0,
            "endpointErrors": 0,
            "completeSeen": False,
            "finalizedSeen": False,
            "finalizationPolls": 0,
            "waitTimedOut": False,
        }

        collected: dict[tuple[str, str, int], SearchCandidate] = {}
        response_object_count = 0
        last_endpoint_error: httpx.HTTPStatusError | None = None
        successful_endpoint_fetch = False

        def merge_payload(raw: Any) -> None:
            nonlocal response_object_count
            candidates, response_objects = self._candidates_from_payload(raw)
            response_object_count = max(response_object_count, response_objects)
            for candidate in candidates:
                key = (candidate.username, candidate.filename, candidate.size)
                collected[key] = candidate

        async def fetch_endpoint() -> None:
            nonlocal last_endpoint_error, successful_endpoint_fetch
            try:
                raw = await self.search_responses(search_id)
                self.last_search_diagnostics["endpointFetches"] = int(self.last_search_diagnostics["endpointFetches"]) + 1
                successful_endpoint_fetch = True
                last_endpoint_error = None
                merge_payload(raw)
            except httpx.HTTPStatusError as exc:
                self.last_search_diagnostics["endpointErrors"] = int(self.last_search_diagnostics["endpointErrors"]) + 1
                last_endpoint_error = exc
                # During startup/finalization the response endpoint may briefly be
                # unavailable. Keep polling the search state instead of treating
                # these statuses as a final search failure.
                if exc.response.status_code not in {404, 409, 425, 429, 503}:
                    raise

        try:
            await self._create_search(payload)
            created = True

            # SearchTimeout in slskd is an inactivity timeout (time since the last
            # response), so an active search can legitimately run longer than the
            # configured value. Poll until slskd writes EndedAt, which is its
            # finalization marker, instead of returning on our own short timer.
            poll_interval = 0.75
            max_wait_seconds = max(60.0, min(300.0, float(search_timeout_seconds) * 6.0))
            max_polls = max(1, int(max_wait_seconds / poll_interval) + 1)
            state: dict[str, Any] = {}

            for poll_no in range(1, max_polls + 1):
                raw_state = await self._request(
                    "GET",
                    f"/api/v0/searches/{search_id}",
                    params={"includeResponses": "false"},
                )
                state = raw_state if isinstance(raw_state, dict) else {}
                self.last_search_diagnostics["finalizationPolls"] = poll_no

                response_count = state.get("responseCount", state.get("ResponseCount"))
                file_count = state.get("fileCount", state.get("FileCount"))
                self.last_search_diagnostics["stateResponseCount"] = int(response_count or 0)
                self.last_search_diagnostics["stateFileCount"] = int(file_count or 0)

                if state.get("isComplete", state.get("IsComplete")):
                    self.last_search_diagnostics["completeSeen"] = True

                ended_at = state.get("endedAt", state.get("EndedAt"))
                if ended_at:
                    self.last_search_diagnostics["finalizedSeen"] = True
                    break

                if poll_no < max_polls:
                    await self._sleep(poll_interval)
            else:
                self.last_search_diagnostics["waitTimedOut"] = True
                logger.warning(
                    "slskd search did not finalize query=%r within %.1fs stateResponses=%s stateFiles=%s",
                    text,
                    max_wait_seconds,
                    self.last_search_diagnostics["stateResponseCount"],
                    self.last_search_diagnostics["stateFileCount"],
                )
                raise TimeoutError(
                    f"slskd search did not finish within {int(max_wait_seconds)} seconds; "
                    "Soulseekarr did not return a premature empty result set."
                )

            # The search is now finalized. Fetch the persisted rows from both
            # supported sources and allow a small database/proxy settle window.
            # We never return 'no results' merely because an in-progress search
            # had not persisted its responses yet.
            stable_polls = 0
            previous_collected = -1
            for attempt in range(10):
                try:
                    raw_state = await self._request(
                        "GET",
                        f"/api/v0/searches/{search_id}",
                        params={"includeResponses": "true"},
                    )
                    if isinstance(raw_state, dict):
                        state = raw_state
                        merge_payload(state)
                        response_count = state.get("responseCount", state.get("ResponseCount"))
                        file_count = state.get("fileCount", state.get("FileCount"))
                        self.last_search_diagnostics["stateResponseCount"] = int(response_count or 0)
                        self.last_search_diagnostics["stateFileCount"] = int(file_count or 0)
                        if state.get("endedAt", state.get("EndedAt")):
                            self.last_search_diagnostics["finalizedSeen"] = True
                except httpx.HTTPStatusError:
                    pass

                await fetch_endpoint()

                current_collected = len(collected)
                if current_collected == previous_collected:
                    stable_polls += 1
                else:
                    stable_polls = 0
                previous_collected = current_collected

                expected_files = int(self.last_search_diagnostics["stateFileCount"])
                if current_collected > 0 and stable_polls >= 2:
                    # The API's fileCount may include rows that later filtering or
                    # locking semantics represent differently, so stability is the
                    # primary stop condition rather than requiring exact equality.
                    break
                if expected_files == 0 and stable_polls >= 2:
                    break
                if attempt < 9:
                    await self._sleep(0.5)

            self.last_search_diagnostics["responseObjects"] = response_object_count
            self.last_search_diagnostics["collectedFiles"] = len(collected)

            logger.info(
                "slskd search finalized query=%r stateResponses=%s stateFiles=%s responseObjects=%s files=%s endpointFetches=%s endpointErrors=%s polls=%s",
                text,
                self.last_search_diagnostics["stateResponseCount"],
                self.last_search_diagnostics["stateFileCount"],
                self.last_search_diagnostics["responseObjects"],
                self.last_search_diagnostics["collectedFiles"],
                self.last_search_diagnostics["endpointFetches"],
                self.last_search_diagnostics["endpointErrors"],
                self.last_search_diagnostics["finalizationPolls"],
            )

            expected_files = int(self.last_search_diagnostics["stateFileCount"])
            if expected_files > 0 and not collected:
                if last_endpoint_error is not None and not successful_endpoint_fetch:
                    raise last_endpoint_error
                raise RuntimeError(
                    f"slskd finalized the search and reports {expected_files} file(s), "
                    "but the response rows are not available yet. Try the search again."
                )

            return list(collected.values())
        finally:
            if created and on_finished is not None:
                # Keep completed searches visible in slskd for one hour. The caller
                # persists the search ID and a background cleanup removes it later.
                try:
                    on_finished(search_id)
                except Exception:
                    pass

    async def delete_search(self, search_id: str) -> None:
        try:
            await self._request("DELETE", f"/api/v0/searches/{search_id}")
        except httpx.HTTPStatusError as exc:
            # A missing search is already clean.
            if exc.response.status_code != 404:
                raise

    async def _enqueue_payload(self, user: str, payload: Any) -> Any:
        """Serialize slskd enqueue calls and retry its transient HTTP 429.

        slskd protects download enqueue operations with a small concurrency
        limiter. Season-folder expansion intentionally starts several jobs at
        once, so without a shared gate those jobs can race each other and one
        receives ``429 Too Many Requests`` even though nothing is actually
        wrong with the peer or file.
        """
        loop = asyncio.get_running_loop()
        key = (id(loop), self.base_url)
        lock = _ENQUEUE_GATES.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _ENQUEUE_GATES[key] = lock

        async with lock:
            last_error: httpx.HTTPStatusError | None = None
            for attempt in range(7):
                try:
                    return await self._request(
                        "POST",
                        f"/api/v0/transfers/downloads/{user}",
                        json=payload,
                    )
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code != 429:
                        raise
                    last_error = exc
                    retry_after = self._retry_after_seconds(exc.response)
                    fallback = min(8.0, 0.75 * (2 ** attempt))
                    delay = max(retry_after or 0.0, fallback)
                    if attempt >= 6:
                        raise
                    await self._sleep(delay)

            if last_error is not None:
                raise last_error
            raise RuntimeError("slskd download could not be enqueued")

    async def enqueue(self, candidate: SearchCandidate) -> Any:
        file_obj = {"filename": candidate.filename, "size": candidate.size}
        user = quote(candidate.username, safe="")
        try:
            return await self._enqueue_payload(user, [file_obj])
        except httpx.HTTPStatusError as exc:
            # Older/alternate slskd builds have accepted the single-file object directly.
            if exc.response.status_code not in {400, 415, 422}:
                raise
            return await self._enqueue_payload(user, file_obj)

    async def directory_candidates(self, username: str, remote_filename: str) -> list[SearchCandidate]:
        """Return files from the selected file's exact remote Soulseek directory.

        slskd exposes Soulseek.NET's GetDirectoryContents call through
        ``POST /api/v0/users/{username}/directory``.  Using that endpoint is much
        cheaper than browsing a peer's entire share and lets Soulseekarr expand a
        selected episode into the other episodes stored beside it (typically a
        season folder).
        """
        normalized = str(remote_filename or "").replace("/", "\\")
        directory, sep, _basename = normalized.rpartition("\\")
        if not sep or not directory:
            return []

        user = quote(username, safe="")
        # slskd maps HTTP 404 on this endpoint to a peer that is currently
        # unavailable (or locally blacklisted), not to a missing API route. A
        # search response can arrive immediately before the peer disconnects,
        # so give the direct directory request a few short retries before
        # abandoning best-effort season expansion.
        payload: Any = None
        retry_delays = (1.0, 2.0, 4.0)
        last_404: httpx.HTTPStatusError | None = None
        for attempt in range(len(retry_delays) + 1):
            try:
                payload = await self._request(
                    "POST",
                    f"/api/v0/users/{user}/directory",
                    json={"directory": directory},
                )
                last_404 = None
                break
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 404:
                    raise
                last_404 = exc
                if attempt >= len(retry_delays):
                    break
                await self._sleep(retry_delays[attempt])

        if last_404 is not None:
            raise SlskdPeerDirectoryUnavailable(
                f"Peer {username} became unavailable before Soulseekarr could read "
                f"the season folder after {len(retry_delays) + 1} attempts."
            ) from last_404

        directories: list[dict[str, Any]] = []
        if isinstance(payload, list):
            directories = [item for item in payload if isinstance(item, dict)]
        elif isinstance(payload, dict):
            # Current slskd returns IEnumerable<Directory>, but tolerate a single
            # Directory object and compatibility wrappers from older builds.
            if isinstance(payload.get("files", payload.get("Files")), list):
                directories = [payload]
            else:
                for key in ("directories", "Directories", "items", "Items", "data", "Data"):
                    value = payload.get(key)
                    if isinstance(value, list):
                        directories.extend(item for item in value if isinstance(item, dict))

        candidates: list[SearchCandidate] = []
        seen: set[tuple[str, int]] = set()
        for directory_row in directories:
            row_directory = str(
                directory_row.get("name")
                or directory_row.get("Name")
                or directory
            ).replace("/", "\\").rstrip("\\")
            files = directory_row.get("files", directory_row.get("Files")) or []
            if not isinstance(files, list):
                continue
            for file_row in files:
                if not isinstance(file_row, dict):
                    continue
                name = str(file_row.get("filename", file_row.get("Filename")) or "")
                if not name:
                    continue
                try:
                    size = int(file_row.get("size", file_row.get("Size")) or 0)
                except (TypeError, ValueError):
                    size = 0
                if size <= 0:
                    continue

                full_name = name.replace("/", "\\")
                if "\\" not in full_name:
                    full_name = f"{row_directory}\\{full_name}" if row_directory else full_name
                key = (full_name.casefold(), size)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(
                    SearchCandidate(
                        username=username,
                        filename=full_name,
                        size=size,
                        raw=file_row,
                    )
                )
        return candidates


    async def downloads_directory(self, recursive: bool = True, subdirectory: str | None = None) -> dict[str, Any]:
        path = "/api/v0/files/downloads/directories"
        if subdirectory:
            token = base64.b64encode(str(subdirectory).replace("\\", "/").strip("/").encode("utf-8")).decode("ascii")
            path += f"/{quote(token, safe='')}"
        payload = await self._request(
            "GET",
            path,
            params={"recursive": str(bool(recursive)).lower()},
        )
        return payload if isinstance(payload, dict) else {}

    async def incomplete_directory(self, recursive: bool = True, subdirectory: str | None = None) -> dict[str, Any]:
        path = "/api/v0/files/incomplete/directories"
        if subdirectory:
            token = base64.b64encode(str(subdirectory).replace("\\", "/").strip("/").encode("utf-8")).decode("ascii")
            path += f"/{quote(token, safe='')}"
        payload = await self._request(
            "GET",
            path,
            params={"recursive": str(bool(recursive)).lower()},
        )
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _download_file_records(directory: dict[str, Any]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for item in directory.get("files") or directory.get("Files") or []:
            if isinstance(item, dict):
                records.append(item)
        for child in directory.get("directories") or directory.get("Directories") or []:
            if isinstance(child, dict):
                records.extend(SlskdClient._download_file_records(child))
        return records

    @staticmethod
    def _tree_file_match(directory: dict[str, Any], username: str, remote_filename: str, size: int | None = None) -> str | None:
        remote_parts = [x for x in strip_remote_path_root(remote_filename).split("/") if x]
        if not remote_parts:
            return None
        basename = remote_parts[-1].casefold()
        username_key = str(username or "").casefold()
        matches: list[tuple[int, str]] = []

        for item in SlskdClient._download_file_records(directory):
            name = str(item.get("name") or item.get("Name") or "").replace("\\", "/").split("/")[-1]
            full_name = str(item.get("fullName") or item.get("FullName") or "")
            if not full_name or name.casefold() != basename:
                continue
            if size is not None:
                try:
                    length = int(item.get("length", item.get("Length")) or 0)
                except (TypeError, ValueError):
                    length = 0
                if length != int(size):
                    continue

            local_parts = [x.casefold() for x in full_name.replace("\\", "/").split("/") if x]
            # Incomplete files are rooted under a sanitized username directory.
            # Requiring the actual username component avoids deleting another
            # peer's partial file when two peers offered the same release name.
            if username_key and username_key not in local_parts:
                continue

            trailing_match = 0
            for remote_part, local_part in zip(reversed(remote_parts), reversed(local_parts)):
                if remote_part.casefold() != local_part:
                    break
                trailing_match += 1
            matches.append((trailing_match, full_name))

        if not matches:
            return None
        matches.sort(key=lambda item: item[0], reverse=True)
        return matches[0][1]

    async def find_incomplete_file(self, username: str, remote_filename: str) -> str | None:
        tree = await self.incomplete_directory(recursive=True)
        return self._tree_file_match(tree, username, remote_filename, size=None)

    async def find_downloaded_file(self, remote_filename: str, size: int) -> str | None:
        """Return slskd's local fullName for the completed file when available."""
        tree = await self.downloads_directory(recursive=True)
        remote_parts = [x.lower() for x in strip_remote_path_root(remote_filename).split("/") if x]
        basename = remote_parts[-1] if remote_parts else ""
        matches: list[tuple[int, str]] = []
        for item in self._download_file_records(tree):
            name = str(item.get("name") or "").replace("\\", "/").split("/")[-1]
            full_name = str(item.get("fullName") or "")
            try:
                length = int(item.get("length") or 0)
            except (TypeError, ValueError):
                length = 0
            if name.lower() != basename or length != int(size) or not full_name:
                continue

            local_parts = [x.lower() for x in full_name.replace("\\", "/").split("/") if x]
            trailing_match = 0
            for remote_part, local_part in zip(reversed(remote_parts), reversed(local_parts)):
                if remote_part != local_part:
                    break
                trailing_match += 1
            matches.append((trailing_match, full_name))

        if not matches:
            return None
        matches.sort(key=lambda item: item[0], reverse=True)
        return matches[0][1]

    async def _download_relative_name(self, full_name: str) -> str:
        """Normalize a Files API fullName to a path relative to downloads root."""
        full_norm = str(full_name or "").replace("\\", "/").rstrip("/")
        if not full_norm:
            raise ValueError("downloaded file path is empty")

        # Current slskd Files API returns fullName relative to the requested root.
        # Older builds exposed an absolute fullName, so preserve compatibility by
        # stripping the downloads root when it is available.
        if not full_norm.startswith("/") and not (len(full_norm) > 2 and full_norm[1:3] == ":/"):
            return full_norm.strip("/")

        root = await self.downloads_directory(recursive=False)
        root_name = str(root.get("fullName") or root.get("FullName") or "")
        root_norm = root_name.replace("\\", "/").rstrip("/")
        if not root_norm:
            raise RuntimeError("slskd returned an absolute file path but no downloads-directory root")
        prefix = root_norm + "/"
        if not full_norm.casefold().startswith(prefix.casefold()):
            raise ValueError(f"downloaded file is outside slskd downloads directory: {full_name}")
        return full_norm[len(prefix):]

    async def delete_downloaded_file(self, full_name: str) -> None:
        """Delete one completed-download file through slskd's Files API."""
        relative = await self._download_relative_name(full_name)
        encoded = self._relative_file_token(relative)
        try:
            await self._request("DELETE", f"/api/v0/files/downloads/files/{encoded}")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise

    async def _delete_empty_download_parents(self, full_name: str) -> int:
        relative = await self._download_relative_name(full_name)
        normalized = relative.replace("\\", "/").strip("/")
        parts = [part for part in normalized.split("/") if part]
        removed = 0
        for depth in range(len(parts) - 1, 0, -1):
            parent = "/".join(parts[:depth])
            try:
                contents = await self.downloads_directory(recursive=False, subdirectory=parent)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    continue
                raise
            files = contents.get("files") or contents.get("Files") or []
            directories = contents.get("directories") or contents.get("Directories") or []
            if files or directories:
                break
            token = base64.b64encode(parent.encode("utf-8")).decode("ascii")
            try:
                await self._request(
                    "DELETE",
                    f"/api/v0/files/downloads/directories/{quote(token, safe='')}",
                )
                removed += 1
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 404:
                    raise
        return removed

    async def delete_downloaded_file_and_empty_parents(self, full_name: str) -> int:
        await self.delete_downloaded_file(full_name)
        return await self._delete_empty_download_parents(full_name)

    @staticmethod
    def _relative_file_token(path: str) -> str:
        relative = str(path or "").replace("\\", "/").strip("/")
        if not relative:
            raise ValueError("file path is empty")
        token = base64.b64encode(relative.encode("utf-8")).decode("ascii")
        return quote(token, safe="")

    async def delete_incomplete_file(self, relative_name: str) -> None:
        """Delete one partial slskd file from the incomplete directory."""
        encoded = self._relative_file_token(relative_name)
        try:
            await self._request("DELETE", f"/api/v0/files/incomplete/files/{encoded}")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise

    async def _delete_empty_incomplete_parents(self, relative_name: str) -> int:
        """Remove empty parent folders of an incomplete file without touching siblings."""
        normalized = str(relative_name or "").replace("\\", "/").strip("/")
        parts = [part for part in normalized.split("/") if part]
        removed = 0
        # Never remove the incomplete root itself. Check each directory is empty
        # immediately before asking slskd to delete it.
        for depth in range(len(parts) - 1, 0, -1):
            parent = "/".join(parts[:depth])
            try:
                contents = await self.incomplete_directory(recursive=False, subdirectory=parent)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    continue
                raise
            files = contents.get("files") or contents.get("Files") or []
            directories = contents.get("directories") or contents.get("Directories") or []
            if files or directories:
                break
            token = base64.b64encode(parent.encode("utf-8")).decode("ascii")
            try:
                await self._request(
                    "DELETE",
                    f"/api/v0/files/incomplete/directories/{quote(token, safe='')}",
                )
                removed += 1
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 404:
                    raise
        return removed

    async def delete_incomplete_file_and_empty_parents(self, relative_name: str) -> int:
        await self.delete_incomplete_file(relative_name)
        return await self._delete_empty_incomplete_parents(relative_name)

    async def clear_completed_downloads(self) -> None:
        """Remove terminal download records from slskd without deleting files.

        slskd's dedicated endpoint soft-removes every completed transfer record,
        including succeeded, errored, cancelled, timed out, rejected, and aborted
        downloads. Requested/queued/initializing/in-progress transfers are left
        untouched. This keeps slskd's Downloads view focused on active work while
        Soulseekarr retains its own persistent job/history records.
        """
        await self._request("DELETE", "/api/v0/transfers/downloads/all/completed")

    async def cancel_download(self, username: str, transfer_id: str, *, remove: bool = True) -> None:
        """Cancel one slskd download and optionally remove its transfer record."""
        user = quote(str(username), safe="")
        transfer = quote(str(transfer_id), safe="")
        await self._request(
            "DELETE",
            f"/api/v0/transfers/downloads/{user}/{transfer}",
            params={"remove": str(bool(remove)).lower()},
        )

    async def all_downloads(self) -> list[dict[str, Any]]:
        payload = await self._request("GET", "/api/v0/transfers/downloads")
        return payload if isinstance(payload, list) else []

    async def queue_position(
        self,
        username: str,
        transfer_id: str,
        *,
        wait_for_slot: bool = False,
        return_cached_on_error: bool = True,
    ) -> dict[str, Any] | None:
        """Best-effort, rate-limited refresh of one remote queue position.

        slskd's ``.../{id}/position`` route actively asks the remote peer for
        the current place in its upload queue.  Keep a cache shared by all
        SlskdClient instances using the same event loop/server so frequent UI
        and Sonarr polling cannot fan out into repeated peer requests.

        If another position request is inside the global spacing window, the
        default UI-oriented behavior returns the last cached response (if any)
        immediately rather than building up a backlog. Background queue-health
        maintenance may pass ``wait_for_slot=True`` so its hourly observation
        eventually performs a real, globally spaced request instead of being
        skipped merely because a page refresh used the same endpoint moments
        earlier.
        """
        username = str(username or "").strip()
        transfer_id = str(transfer_id or "").strip()
        if not username or not transfer_id:
            return None

        loop = asyncio.get_running_loop()
        gate_key = (id(loop), self.base_url)
        gate = _QUEUE_POSITION_GATES.get(gate_key)
        if gate is None:
            gate = {
                "lock": asyncio.Lock(),
                "next": 0.0,
                "cache": {},
            }
            _QUEUE_POSITION_GATES[gate_key] = gate

        cache: dict[tuple[str, str], dict[str, Any]] = gate["cache"]
        cache_key = (username, transfer_id)

        def cached_transfer(now: float) -> tuple[dict[str, Any] | None, bool]:
            entry = cache.get(cache_key)
            if not entry:
                return None, False
            transfer = entry.get("transfer")
            attempted_at = float(entry.get("attempted_at") or 0.0)
            fresh = now - attempted_at < QUEUE_POSITION_REFRESH_SECONDS
            return transfer if isinstance(transfer, dict) else None, fresh

        now = loop.time()
        cached, fresh = cached_transfer(now)
        if fresh:
            return cached
        if not wait_for_slot and (gate["lock"].locked() or float(gate["next"]) > now):
            return cached

        async with gate["lock"]:
            now = loop.time()
            cached, fresh = cached_transfer(now)
            if fresh:
                return cached
            if float(gate["next"]) > now:
                if not wait_for_slot:
                    return cached
                await asyncio.sleep(max(0.0, float(gate["next"]) - now))
                now = loop.time()

            # Record the attempt before making the request.  A failed peer or
            # transient slskd error therefore cannot be hammered every refresh.
            cache[cache_key] = {"attempted_at": now, "transfer": cached}
            gate["next"] = now + QUEUE_POSITION_MIN_INTERVAL_SECONDS

            user = quote(username, safe="")
            transfer = quote(transfer_id, safe="")
            try:
                payload = await self._request(
                    "GET",
                    f"/api/v0/transfers/downloads/{user}/{transfer}/position",
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    return cached if return_cached_on_error else None
                if exc.response.status_code == 429:
                    retry_after = self._retry_after_seconds(exc.response) or 0.0
                    gate["next"] = max(
                        float(gate["next"]),
                        loop.time() + max(QUEUE_POSITION_MIN_INTERVAL_SECONDS, retry_after),
                    )
                    return cached if return_cached_on_error else None
                raise

            if isinstance(payload, dict):
                cache[cache_key] = {"attempted_at": loop.time(), "transfer": payload}

                # Avoid retaining transfer identities forever on long-running
                # installations with substantial churn.
                if len(cache) > 2048:
                    cutoff = loop.time() - 24 * 60 * 60
                    for key, entry in list(cache.items()):
                        if float(entry.get("attempted_at") or 0.0) < cutoff:
                            cache.pop(key, None)
                return payload
            return cached

    @staticmethod
    def flatten_downloads(groups: Any) -> list[dict[str, Any]]:
        """Flatten slskd's username/directory transfer tree into file rows.

        slskd groups download transfers by peer and directory.  Keep the peer
        username on each flattened transfer so callers can correlate it with a
        Soulseekarr job without making one API request per job.
        """
        if not isinstance(groups, list):
            return []

        rows: list[dict[str, Any]] = []

        def walk_directory(directory: Any, username: str) -> None:
            if not isinstance(directory, dict):
                return
            for file in directory.get("files") or directory.get("Files") or []:
                if not isinstance(file, dict):
                    continue
                row = dict(file)
                row.setdefault("username", row.get("Username") or username)
                rows.append(row)
            for child in directory.get("directories") or directory.get("Directories") or []:
                walk_directory(child, username)

        for group in groups:
            if not isinstance(group, dict):
                continue
            username = str(group.get("username") or group.get("Username") or "")
            for file in group.get("files") or group.get("Files") or []:
                if isinstance(file, dict):
                    row = dict(file)
                    row.setdefault("username", row.get("Username") or username)
                    rows.append(row)
            for directory in group.get("directories") or group.get("Directories") or []:
                walk_directory(directory, username)
        return rows

    async def download_by_id(self, username: str, transfer_id: str) -> dict[str, Any] | None:
        """Return one slskd download by its persisted transfer id.

        A restored slskd transfer can reappear with a differently-normalized
        filename after restart.  The transfer id, when slskd preserved it, is a
        stronger recovery identity than re-parsing the path.
        """
        user = quote(str(username or ""), safe="")
        transfer = quote(str(transfer_id or ""), safe="")
        if not user or not transfer:
            return None
        try:
            payload = await self._request(
                "GET", f"/api/v0/transfers/downloads/{user}/{transfer}"
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            raise
        return payload if isinstance(payload, dict) else None

    async def find_download(self, username: str, filename: str, size: int) -> dict[str, Any] | None:
        groups = await self.all_downloads()
        rows = self.flatten_downloads(groups)
        wanted = remote_transfer_key(username, filename, size)
        for file in rows:
            file_username = str(file.get("username") or file.get("Username") or "")
            file_name = str(file.get("filename") or file.get("Filename") or "")
            try:
                file_size = int(file.get("size", file.get("Size")) or 0)
            except (TypeError, ValueError):
                file_size = 0
            if remote_transfer_key(file_username, file_name, file_size) == wanted:
                return file

        # slskd persistence/restore has historically normalized some remote path
        # roots differently (for example G:\\TV\... becoming TV\...).  Use a
        # rootless fallback only when it identifies exactly one live transfer; an
        # ambiguous fallback is safer to treat as no match than to attach the
        # wrong Soulseekarr job.
        fallback = remote_transfer_fallback_key(username, filename, size)
        matches: list[dict[str, Any]] = []
        for file in rows:
            file_username = str(file.get("username") or file.get("Username") or "")
            file_name = str(file.get("filename") or file.get("Filename") or "")
            try:
                file_size = int(file.get("size", file.get("Size")) or 0)
            except (TypeError, ValueError):
                file_size = 0
            if remote_transfer_fallback_key(file_username, file_name, file_size) == fallback:
                matches.append(file)
        return matches[0] if len(matches) == 1 else None
