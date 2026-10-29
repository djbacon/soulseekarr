# Soulseekarr v0.8.4



## v0.8.4

- Fixed an additional cause of completed upgrades staying in `importing`: confirmation now recognizes a replacement when the media database reuses the previous file ID, provided its import date changed and its size matches the downloaded candidate. This check applies both while waiting for the command and during final library confirmation; an unchanged existing file still cannot count as a successful upgrade. Import status details now include the command ID.

## v0.8.3

- Fixed older movies being skipped when Radarr has no digital or physical release date. Movies with a known year at least two calendar years before the current UTC year are now searchable without a release date (for example, 2024 or earlier in 2026). Movies with no year or a more recent year remain ineligible without a release date; known release dates still respect the shared release delay.

## v0.8.2

- Fixed short TV series titles matching words inside other show names, such as Mom matching Teen Mom. Extra show-name words now reject the candidate before download; exact titles with years, provider names, and quality tags remain eligible.

## v0.8.1

- Fixed imports remaining in `importing` while Sonarr command bookkeeping lags behind the library. Soulseekarr now checks for a newly installed episode file while waiting for the command; upgrades still require the previous file ID to change.

## v0.8.0

- Hovering over an active download’s status icon on the dashboard now shows its planned/final filename, along with the existing status and queue position.

## v0.7.0

- Added automatic import-directory cleanup on the first scheduler pass after startup and hourly thereafter, even with automatic searching disabled. Top-level folders and loose files in `SOULSEEKARR_IMPORT_DIR` (default `/downloads`) are removed after 48 hours without modification; a folder is kept if any descendant was modified more recently. Active job paths, symlinks, and mounted subdirectories are preserved. Cleanup failures are logged and retried on a later pass.

## v0.6.1

- Fixed connection tests ignoring unsaved URL and API key fields for Radarr, Sonarr, and slskd. Tests now use the current form values without saving settings, and reuse the saved key when the field is masked.

## v0.6.0

- Added `-SLSKD` before the extension when determining an import filename without an available release group. Existing episode and movie release groups are preserved.

## v0.5.1

- Fixed pagination and page controls in Missing and Upgrade by allowing the shared cooldown timer script through the existing Content Security Policy.


## v0.5.0

- Added a **Cooldown** column to Missing and Upgrade showing a live countdown of each episode or movie’s remaining automatic search cooldown, including adaptive cooldowns. Cells stay empty when no cooldown is active.


## v0.4.0

- Added Radarr alongside Sonarr. Configure either service, or both, with slskd in Settings. Radarr uses the [Radarr v3 API](https://radarr.video/docs/api/).
- The Library page controls both series and movies. Missing and Upgrade include enabled movies, and movie jobs appear in the shared downloads, dashboard, and history.
- Movies use the same persistent rotation, Missing/cutoff/exploratory priorities, newly enabled priority turn, adaptive cooldowns, search pacing, peer limits, and download recovery as Sonarr. Each movie occupies one position in the library rotation. TV-only season weakness comparisons and folder expansion do not apply to movies.
- Movie queries use title and year. Radarr must recognize the exact movie and return a Custom Format score before queueing. Results rank by Custom Format score, resolution and source, then peer score. Upgrades cannot reduce any of those first three dimensions and must improve at least one. Completed files are evaluated again through Radarr before native ManualImport and verified against the resulting movie file.
- Movie release filenames are retained so custom release tokens survive scoring and import. Missing movies become searchable after their digital release (or physical release when digital is unavailable), plus the shared release delay. Unknown release dates remain ineligible.
- Add Radarr's URL and API key in Settings → Connections, then enable movies in Library. API keys are encrypted at rest. In Storage, set the staging path as Radarr sees it; optional Radarr paths on folder mappings permit direct imports. Without a Radarr mapping, movies use fallback staging. The staging directory must be mounted in both Soulseekarr and Radarr.
- To show Soulseekarr activity in Radarr, add its read-only qBittorrent compatibility client with category **`soulseekarr-radarr`**. Sonarr keeps **`soulseekarr`**; category filtering separates their queues.
- Movie IDs occupy a separate persistent namespace; existing Sonarr records and settings retain their meaning. Both services share the configured automatic-search and download limits.

### Versioning

Versions are **major.feature.bugfix**: the first number is a major release, the second adds new features, and the third fixes bugs. Feature releases reset the bugfix number; the current release is **0.8.4**.


## v0.3.158

- Dashboard **Active downloads** now replaces the empty 0% progress bar for queued transfers with queue state information. Known remote positions render as **Queued #N**; queued rows with strike state also include the current strike count, and peer-limit cooldowns show **Peer limit wait**.
- Rows waiting for their first background slskd snapshot show **Waiting for snapshot…** instead of a misleading empty progress bar. Active transfers continue to use the normal percentage progress bar.


## v0.3.157

- Dashboard **Recent searches** now includes the actual queue count in the status badge when a search queues downloads, for example **`4 queued`** instead of the generic **Queued** label.


## v0.3.156

- Dashboard **Active downloads** now includes **File size** and **Time left** columns. File size uses the existing persisted transfer/job size, while Time left uses the same `remainingSeconds` calculation as the full Downloads page without adding any new slskd polling.
- Preparing/evaluating/importing rows show the stage label (`Preparing`, `Scoring`, or `Importing`) instead of a misleading network ETA; queued or speedless transfers show `—` until an ETA can be calculated.


## v0.3.155

- Reworked scheduled automatic-search pacing around **one logical episode search per search window**. The scheduler now selects one episode, runs that episode's query bundle, finishes, and only then starts the configured **Episode search interval** countdown. A cycle can no longer remain active for minutes/hours simply because searches found nothing and therefore did not reach the download-per-cycle limit.
- Renamed the Search settings to make the two time scales explicit: **Episode search interval** controls spacing between different episode targets (new-install default **2 minutes**), while **Additional-query gap** controls spacing between alternate query forms for the *same* episode (new-install default **15 seconds**). Existing saved values are preserved when upgrading.
- Primary episode pacing is **completion-to-next-start**. Time spent waiting for slskd results or running a second query form never consumes the configured inter-episode interval. Scheduler timing is checked every 5 seconds so the Dashboard countdown does not sit at zero for up to 30 seconds.
- Series configured for **Both** now run both enabled query forms in the same bundle, in preferred order, even when the first form already found a usable result. Results are deduplicated/merged and the best eligible candidate is selected across both searches. A transient slskd/Soulseek error still stops the remaining bundle to avoid a retry burst.
- Season 00 bundles use `Series + episode title` first and `Series + S00E##` second, with the Additional-query gap between them. Ordinary Both mode uses `Series SxxEyy` first and `Series 1x02` legacy form second. Manual single-variant searches remain explicit and unchanged.
- The existing shared slskd search-creation gate and HTTP 429 backoff remain in place as a final burst/rate-limit safety layer. **Base repeat-search cooldown = 0** still disables per-episode adaptive backoff, so repeated rotations can revisit episodes frequently even though the new global pacing is enforced.


## v0.3.154

- Explicitly toggling a TV show from **disabled -> enabled** now queues a persistent one-time **newly enabled priority** turn. That show is searched before the normal global Missing/Upgrade scheduler on the next automatic cycle.
- The priority turn prefers an eligible **Missing** episode from that show. If none is currently eligible, Soulseekarr searches the show's best installed upgrade tier instead (cutoff-unmet, weak CF/source/resolution, then exploratory).
- Old adaptive backoff for every episode in the re-enabled show is reset before the priority turn, while historical last-search timestamps and query-variant state are preserved so the show does not unfairly monopolize normal scheduling afterward.
- After one real Soulseek search attempt, the one-time priority flag is consumed and normal global scheduling resumes. Sonarr/API failures or pre-search external-queue blocks do not consume it. Disabling the show again cancels the pending turn.
- Priority state survives container restarts and is FIFO when several shows are enabled. A show enabled while a cycle is already running is snapshot-isolated from that cycle and leads a later cycle instead. Existing SQLite databases create the priority table automatically.


## v0.3.153

- Upgrade comparison is now source-aware in addition to Sonarr Custom Format score and resolution. The conservative source order is **HDTV < WEBRip < WEBDL < Bluray < Remux**; unknown sources remain unranked.
- Same-resolution/same-CF source improvements such as `HDTV-1080p -> WEBDL-1080p` are now valid upgrades and can be queued/imported. A candidate still cannot reduce Custom Format score, resolution, **or** source quality, so a higher-resolution release cannot hide a source downgrade (for example `WEBDL-1080p -> HDTV-2160p` is rejected).
- Automatic upgrade scheduling now treats episodes with a weaker source than other installed episodes in the same season as **quality-weak** targets, using the same adaptive cadence as weak-resolution upgrades (about 3 days after the first unsuccessful probe, then about weekly with the default 24-hour base).
- Source-aware comparison is applied consistently to pre-download candidate filtering, manual/automatic queue validation, active-transfer frontier checks, final Sonarr import evaluation, and superseded-transfer cleanup. No database migration is required.


## v0.3.152

- Dashboard Recent searches header now shows the exact next automatic-search time and a live one-second countdown.
- The header switches to the scheduler's running, disabled, paused, or unconfigured status when no future run time applies.

## v0.3.151

- Dashboard **Overview** card now appears first, above Active downloads and Recent searches.

## v0.3.150

- Added a new **Dashboard** section and made it the default `/` landing page. The existing Missing section now lives at `/missing`; the Soulseekarr brand and Dashboard navigation both return to the new start page.
- Dashboard card 1 shows the first 10 rows from the same active-download ordering used by Downloads, including status, series/episode, quality, peer, speed, and progress. It refreshes every 5 seconds from Soulseekarr's persisted slskd snapshot without browser-triggered slskd polling.
- Dashboard card 2 shows the last 10 real episode-search audit entries (generic no-work cycle notices are excluded), with manual/automatic source, episode/query, raw/usable/queued counts, and concise outcomes such as **Queued**, **Nothing found**, **No eligible match**, **Failed**, **Skipped**, or **Success**.
- Dashboard card 3 shows **Missing episodes**, **TV shows enabled / total**, **Currently downloading**, and **Queued downloads**. Missing count uses the same enabled-series/specials rules as Missing and is cached for 30 seconds so the 5-second Dashboard refresh does not hammer Sonarr. Downloading uses live persisted transfer state; queued excludes preparing/importing work.

## v0.3.149

- Replaced the automatic nested missing-vs-upgrade round-robin with a global priority scheduler. Due **Missing** episodes are considered before the proactive Upgrade catalog is loaded. When Missing work is exhausted for the cycle, installed episodes are searched in this order: Sonarr **cutoff unmet**, weaker **Custom Format score**, weaker **resolution**, then low-frequency exploratory upgrades.
- Round-robin state is retained as a fairness tie-breaker rather than the primary scheduler. Within a priority tier, never-searched/oldest-searched work wins first, while the persistent TV show -> season -> episode cursors prevent equally old work from concentrating on one show. A season is probed at most once per cycle so one Soulseek query can be leveraged by the existing season-folder expansion instead of repeatedly searching the same season.
- Automatic searches now try all enabled episode query variants in the **same search turn** when the current spelling completes successfully but yields no usable candidates. A transient slskd/Soulseek error stops the turn and keeps the current spelling for retry instead of burning through alternate variants.
- Added persistent adaptive automatic-search backoff. With the default 24-hour base, a missing episode with no results retries after about 4h, then 8h, then 24h, 48h, and eventually weekly; transport/API failures retry in about 30 minutes. Repeated unusable results and low-value upgrade probes back off more aggressively. Setting the base cooldown to `0` still disables these delays.
- Upgrade scheduling now consumes Sonarr's `/api/v3/wanted/cutoff` list when available as a high-priority signal. If that endpoint is unavailable, Soulseekarr falls back to its existing installed-file CF-score/resolution analysis without failing the cycle.
- Existing SQLite databases are migrated automatically with per-episode adaptive scheduling metadata (`next_search_at`, last outcome/target type, and outcome streak). No manual migration is required.

## v0.3.148

- Missing and Upgrade now keep the manual-search and targeted automatic-search buttons available when an episode is already downloading through an external Sonarr download client. Explicit user-triggered searches and selected manual results may run/queue in parallel with Sonarr; the unattended background automatic cycle remains conservative and still skips episodes already owned by Sonarr's external queue.
- Added a compact **Downloading** column to both Missing and Upgrade. A cloud-download icon marks an external Sonarr queue item and a local download icon marks an active Soulseekarr job. If both are active for the same episode, both icons are shown together.
- Sonarr queue indicators now exclude Soulseekarr's own read-only qBittorrent compatibility entries by download ID, so a local Soulseekarr job is not incorrectly presented as an external Sonarr download. No database migration is required.

## v0.3.147

- Notifications are now application-global rather than tied to the currently open section. The shared top-right notification stack is available on Missing, Upgrade, TV shows, Downloads, History, and Settings, and transient action feedback now uses this notification system instead of page-level alert boxes.
- Per-episode automatic searches launched from Missing or Upgrade now run as server-side background tasks. You can start a search, navigate to Downloads (or any other Soulseekarr section), and still receive the success/warning/error notification when that search finishes. Manual **Run auto cycle** uses the same navigation-safe background completion flow.
- Global notifications are persisted through a small SQLite delivery feed plus a per-tab browser cursor. Notifications completed while the document is navigating are delivered on the next section, while old notifications from previous browser sessions are not replayed.
- A notification that is already visible also survives normal section navigation for the remainder of its 5-second lifetime. Notifications still stack in the top-right and can be dismissed immediately with X.
- Settings saves, connection tests, TV-show preference saves/refresh errors, download removal/refresh errors, history actions, search warnings, and manual queue outcomes now use the same notification style. Existing SQLite databases create the new `ui_notifications` table automatically.

## v0.3.146

- Standalone `WEB` source tokens are now normalized to Sonarr-compatible `WEBDL`. For example, `1080p.NF.WEB.DDP.AV1-BTT` is planned as `[NF][WEBDL-1080p][EAC3][AV1]-BTT` rather than `[NF][WEB-1080p]`.
- Existing active jobs created by v0.3.145 with `[WEB-<resolution>]` planned names are repaired automatically on startup. Older resolution-only planned names are also repaired directly to `WEBDL` when the source path contains standalone `WEB`. Explicit `WEBRip` remains `WEBRip`, and explicit `WEB-DL` remains `WEBDL`.

## v0.3.145

- Planned/import filenames now preserve a standalone `WEB` source token. Releases such as `1080p.NF.WEB.DDP.AV1-BTT` are named with `[WEB-1080p]` instead of falling back to `[1080p]`, while explicit `WEB-DL` and `WEBRip` releases keep their existing more-specific labels.
- Queued/downloading jobs created by older versions are repaired on startup when the original Soulseek path proves the source but the persisted planned filename contains only a resolution tag. For example `[NF][1080p][EAC3][AV1]-BTT` becomes `[NF][WEB-1080p][EAC3][AV1]-BTT` before preparation/import.
- No database migration is required.

## v0.3.144

- When slskd/Soulseek rejects a transfer with `Transfer rejected: File not shared`, Soulseekarr now places that exact Soulseek **user + remote file path + size** on a 30-day rejection ban. Automatic and manual searches skip that dead result instead of selecting it again every time.
- The ban is deliberately file-specific: other files from the same Soulseek user remain eligible, and the same release from another user remains eligible. Bans persist across container restarts and expire automatically after 30 days.
- The selected-download endpoint and season-folder expansion also re-check the ban before creating jobs, preventing stale browser results or folder expansion from re-queueing the rejected file. The job failure message records the ban expiry timestamp.
- Existing SQLite databases are upgraded automatically with the new `file_rejection_bans` table; no manual migration is required.

## v0.3.143

- Per-episode magnifying-glass searches on Missing and Upgrade now report their outcomes as stacked Ubuntu-style notifications in the top-right corner instead of reusing the page-level automatic-cycle alert.
- Notifications stack independently, dismiss automatically after 5 seconds, and can be closed immediately with the X button. Successful queues use a success state, normal no-result/not-eligible outcomes use a warning state, and service/search failures use an error state.
- The full **Run auto cycle** action keeps its existing inline status message. No database migration is required.

## v0.3.142

- Upgrade keeps its manual-search and automatic-search buttons available when the episode already has an active Soulseekarr job in Downloads. A Soulseekarr-owned queue/download/preparing/importing item is no longer presented as an external Sonarr queue block.
- Search endpoints now ignore Sonarr Activity entries that belong to any active Soulseekarr job exposed through the read-only qBittorrent compatibility API, while a genuinely external Sonarr download-client queue item still blocks a duplicate search/download.
- Existing repeat-file protection and strict installed-file upgrade checks are unchanged: the exact same active/downloaded file is still filtered, and existing Sonarr files still require a real quality/Custom Format upgrade before queueing.
- No database migration is required.

## v0.3.141

- Upgrade now shows a **Quality** tag for episodes that already have a Sonarr file. The label is read directly from Sonarr's `episodeFile.quality.quality.name` metadata (for example `WEBDL-1080p` or `Bluray-1080p Remux`) rather than inferred from the filename.
- Missing episodes show an em dash in the Quality column until Sonarr has an installed episode file. The same exact Sonarr quality metadata is available on both initial page load and asynchronous Upgrade pagination/refresh.
- No database migration is required.

## v0.3.140

- Added a new **Upgrade** section beside Missing. It lists every episode from TV shows enabled for Soulseekarr, including episodes that already have files, with the same per-episode manual-search (user icon) and targeted automatic-search (magnifying-glass) actions.
- Upgrade-page searches can target episodes that already have Sonarr files. Existing files still use the normal strict Sonarr-backed upgrade rules: a candidate cannot reduce resolution, cannot reduce Custom Format score, and must improve at least one of those dimensions before it can be queued.
- Missing episodes shown in Upgrade retain the configured missing-episode air-date delay, while installed episodes are immediately eligible for proactive upgrade searches just like the background automatic upgrade cycle.
- Upgrade pagination uses a short-lived server-side episode catalog cache so moving between pages does not re-query every enabled TV show from Sonarr; the Refresh button explicitly rebuilds the catalog.
- No database migration is required.

## v0.3.139

- Added persistent per-Soulseek-user enqueue-failure strikes for the repeated slskd `HTTP 500 Internal Server Error` case. The default threshold is 3 strikes; each failed enqueue status now shows the current strike count instead of retrying the same peer/file indefinitely with only the raw HTTP exception.
- When the threshold is reached, the peer enters a dedicated enqueue-failure cooldown. The default is 7 days, and both **Enqueue failure strikes** and **Peer failure cooldown** are configurable under Settings -> Downloads & Import -> Peer failure protection.
- Enqueue-failure cooldowns are intentionally different from normal remote uploader queue-limit cooldowns: queued files that slskd has not accepted are stopped, future automatic/manual queue attempts skip that peer, and already accepted/downloading slskd transfers are left alone.
- A successful enqueue (or rediscovery of an already accepted transfer) clears that peer's accumulated enqueue-failure strikes. After a failure cooldown is created, the strike counter is reset so the peer gets a fresh threshold when the cooldown eventually expires.
- Generic slskd connection errors and established Soulseek remote queue-limit responses do not receive these HTTP-500 strikes; the existing reconnect and queue-limit retry policies remain unchanged. Existing SQLite databases are upgraded automatically with the new peer strike state/cooldown type columns; no manual migration is required.

## v0.3.138

- slskd restart recovery is now scoped to the specific Soulseekarr jobs that could already have existed in slskd when the restart/outage occurred. New downloads added after a Soulseekarr update no longer inherit the global 5-minute persisted-transfer restore wait.
- Startup recovery snapshots the active job IDs that existed before recovery begins. Those pre-existing jobs retain the existing duplicate-download protection and bounded restore timeout, while jobs created afterward can enqueue immediately.
- Later slskd API outages and sudden transfer-list collapses protect only jobs that already show evidence of an accepted/persisted slskd transfer; a brand-new queued job is not unnecessarily held after reconnect.
- No database migration is required.

## v0.3.137

- Season 00 searches now have two result-driven query variants when Sonarr provides a special title: `Series + episode title` first, then `Series + S00E##`. This keeps title-only webisodes discoverable while also finding releases that are named only with the standard special episode code.
- Season 00 candidate matching continues to accept either the full special-title phrase or the exact `S00E##`/`0x##` marker, and regression coverage now includes deeply nested paths such as `Community (2009)/Season 01/Featurettes/Mini Episodes/...` where the immediate parent folders are not the TV-show name.
- Search-response parsing now preserves separate remote-directory metadata when an slskd compatibility payload returns only a basename. This allows series matching to inspect all available ancestor folders instead of losing the show-name context before ranking.
- Season 00 videos use a 1 MB safety floor instead of the normal configured full-episode minimum size. Short webisodes and mini-episodes in the 5-10 MB range are therefore eligible when their series and special identity match, while ordinary seasons continue to obey the configured minimum file size.
- No database migration is required.

## v0.3.136

- Season-folder expansion now retries a transient slskd peer-directory 404 four times over seven seconds. slskd uses 404 here when the remote Soulseek user is temporarily unavailable (or blacklisted), so a peer that disconnects immediately after returning a search result no longer causes expansion to fail on the first request.
- If the peer is still unavailable after the retries, the selected episode remains queued and Soulseekarr now reports a concise `Season-folder expansion skipped` message instead of exposing the raw HTTP client exception/URL.


## v0.3.135

- Missing Episodes now has two explicit per-episode actions. The user icon opens the existing manual Soulseek result picker, while the magnifying-glass icon performs a targeted automatic search and immediately queues the best eligible candidate.
- Targeted automatic search uses the same candidate filtering, Sonarr upgrade/quality checks, active-transfer rules, peer cooldown handling, and search-variant logic as the normal automatic cycle, then performs the existing same-peer season-folder expansion.
- The targeted action is user-triggered: it does not move the background TV-show -> season -> episode round-robin cursors and does not depend on the Automatic search/Automatic downloads scheduler toggles. It still respects missing-episode eligibility and external Sonarr queue ownership.
- Missing-page asynchronous pagination renders both action buttons consistently, and the action column was widened to keep the compact layout aligned. No database migration is required.

## v0.3.134

- Fixed automatic searches for genuinely missing episodes that already have one or more Soulseekarr transfers queued/downloading. Active transfers are no longer treated like an installed Sonarr episode file when deciding whether a newly found release is eligible.
- The normal automatic round-robin may now queue a different eligible release for a still-missing episode even when the existing queued/downloading release has equal or higher Custom Format score/resolution. Exact duplicate files remain blocked by the existing active/download-history filename+size protection.
- Proactive upgrade searches are unchanged: when Sonarr already has an installed episode file, active upgrade transfers still suppress alternatives that are not a strict improvement over the upgrade already in flight.
- This fixes cases where a search could report many raw Soulseek results but end with `none passed quality/score/repeat-download eligibility` solely because the episode had a queued Soulseekarr transfer, even though Sonarr still reported no installed file. No database migration is required.

## v0.3.133

- Fixed the dynamic **Maximum actually downloading** search guard over-counting restored partial transfers. slskd can keep historical `startedAt`/`bytesTransferred` values when a partially-downloaded file returns to `Queued, Remotely`; current queue state now takes precedence, so those transfers no longer count as active network downloads.
- Completed/terminal slskd rows are no longer counted as actively downloading merely because they contain old byte progress or omit `endedAt`. The active-download count also deduplicates by live slskd transfer id so duplicate Soulseekarr rows cannot inflate the guard.
- The same current-state rule is used by queue-health maintenance and the runner timeout logic: a resumed partial transfer that is currently queued remains queue-managed and does not burn its active-download timeout while waiting for the peer.
- No database migration is required.

## v0.3.132

- Fixed slskd restart recovery so the persisted-transfer restore wait has a hard 5-minute timeout instead of a deadline that can be repeatedly extended by restart-like signals. The Downloads status now shows a live countdown while Soulseekarr is waiting for slskd to finish restoring transfers.
- slskd API outages are tracked separately from transfer restoration. A reconnect starts one fresh bounded restore window, but repeated polling during the same restore attempt cannot keep extending it forever.
- When the restore timeout expires and a persisted transfer still has not reappeared, Soulseekarr now marks that old job cancelled/removed instead of silently re-enqueuing the old candidate. The episode can therefore return to Missing Episodes and be handled by the normal TV-show -> season -> episode automatic round-robin.
- If the completed file exists even though the transfer row did not return, Soulseekarr still recognizes it as downloaded rather than cancelling it. No database migration is required.

## v0.3.131

- Hardened slskd restart recovery so persisted transfers are correlated by their transfer id first, exact peer/path/size second, and a unique rootless path fallback last. This tolerates slskd restoring an equivalent Windows/UNC/share path in a different representation without merging genuinely ambiguous transfers.
- Added a shared 15-minute slskd restart/restore grace window on Soulseekarr startup, after slskd API outages, or after a sudden transfer-list collapse. During that window queued jobs are not duplicated and active jobs are not falsely terminalized while slskd is still restoring its persisted queue.
- The Downloads snapshot now uses the same transfer-id and tolerant-path recovery rules, so restored active/queued transfers remain visible instead of disappearing from Soulseekarr after an slskd restart.
- Recent failed/cancelled/rejected Soulseekarr episode jobs are now bounded recovery hints for Sonarr. If Sonarr Wanted/Missing temporarily omits an episode after its download-client item disappears, Soulseekarr verifies that exact episode through Sonarr's direct episode resource and restores it to Missing Episodes/automatic round-robin when `hasFile` is still false.
- Manual Search/Download can likewise resolve a directly verified missing episode even while Sonarr Wanted/Missing is lagging.
- No database migration is required.

## v0.3.130

- Added release-group detection for the common parenthesized naming style where technical metadata and the release group share the final parentheses. For example, `Persons Unknown (2010) - S01E07 - Smoke and Steel (1080p NF WEB-DL x265 Panda).mkv` now identifies `Panda` as the release group.
- Parenthesized group detection is deliberately conservative: the block must contain recognizable release metadata such as resolution/source/provider/codec, and metadata-only endings like `(1080p NF WEB-DL x265)` do not invent a group. Ordinary title suffixes such as `(Part 2)` are not treated as release groups.
- Compact codec/group endings such as `x265-Panda` inside a metadata block are also understood.
- The detected group is preserved in the normalized planned/import filename, so Sonarr parses and scores the candidate with the release group present. No database migration is required.

## v0.3.129

- Fixed the remaining Season 00 missing-episode discovery gap. Soulseekarr no longer relies only on Sonarr's `/wanted/missing` response for specials; it supplements that list from the direct Season 00 episode resources for Soulseekarr-enabled TV shows, so no-file specials with no `airDateUtc` actually appear on the Missing Episodes page and enter the normal automatic round-robin.
- The supplemental scan includes both dated and undated no-file specials and deduplicates by Sonarr episode id. Existing per-series Download choices plus Sonarr season/episode monitoring are still respected, and only series that actually expose Season 00 are queried.
- Manual Search and Download can now resolve a Season 00 episode directly by Sonarr episode id when Wanted/Missing omitted it, so an undated special shown by Soulseekarr remains actionable instead of returning `404 Episode is no longer missing`.
- Fixed redundant special queries when Sonarr already includes the series name in the episode title. `Scrubs` + `Scrubs Funniest Moments` now searches `Scrubs Funniest Moments` instead of `Scrubs Scrubs Funniest Moments`.
- Special result matching now also accepts the episode-specific remainder when Sonarr's title redundantly begins with the series title. For example, `TV/Scrubs/Specials/Funniest Moments.mkv` can match Sonarr's `Scrubs Funniest Moments` while the normal independent series-title check still prevents cross-show false matches.
- Language preflight now asks Sonarr to parse the exact normalized filename Soulseekarr will use for import, including the injected `S00E##` marker, rather than the raw title-only Soulseek filename. This prevents valid title-only specials from being discarded merely because Sonarr cannot parse their raw share name before Soulseekarr normalizes it.
- No database migration is required.

## v0.3.128

- Improved Soulseek peer queue scoring. Queue length remains a ranking signal only: peers with non-zero queues are still valid candidates and are never rejected solely for being queued.
- Replaced the old linear queue penalty, which reached its maximum at roughly 86 queued files and therefore treated queues of 100 and 4,000 identically, with a logarithmic penalty that continues to distinguish longer queues.
- Small queues receive a modest penalty, while queues in the hundreds/thousands progressively rank lower; the penalty is capped so an otherwise excellent release can still win when appropriate. Custom Format and upgrade validation remain unchanged.

## v0.3.127

- Removed the separate stale queued-download alternative-search scheduler. Queue maintenance now only measures remote queue progress and removes a transfer after the existing seven-day/non-improving strike policy; it never starts a Soulseek search.
- Removed immediate alternate-candidate searches after `TimedOut`, `Errored`, and `Aborted` slskd transfers. Those failed attempts are cleaned up and left for the normal automatic round-robin to revisit after the ordinary repeat-search cooldown.
- The automatic **TV show -> season -> episode** cycle is now the only background code path that launches Soulseek searches. Manual user-triggered searches are unchanged.
- Episodes that already have Soulseekarr downloads remain eligible when their normal automatic turn arrives; Soulseekarr ignores its own Sonarr/qBittorrent compatibility queue entries while still yielding to external Sonarr queue work.
- Removed the obsolete **Stale queued-download retry** setting. Existing `settings.json` files containing the old key remain compatible; the unknown key is simply ignored.

## v0.3.126

- Season 00 specials with no Sonarr `airDateUtc` are now immediately eligible for missing-episode Soulseek search/download instead of being permanently blocked by the missing-episode delay.
- Specials that do have a real air date still obey the configured Missing-episode delay, and malformed non-empty dates remain blocked rather than being guessed.
- The rule is centralized in the normal eligibility check, so it applies consistently to automatic search, manual Search/Download, stale queued-download recovery, and season-folder expansion.

## v0.3.125

- Missing Episodes pagination now swaps pages in place without navigating or reloading the page.
- The visible Missing Episodes URL stays at `/missing`; legacy Missing-page query pagination is canonicalized in place.
- Adjacent Missing Episodes pages are prefetched for faster Previous/Next navigation.

## v0.3.124

- TV-show pagination now stays entirely in-place: Previous/Next swap the table contents asynchronously and never navigate the browser to `/series?page=N`.
- The TV-show page now requests paged snapshot data through `/api/series/page/N`, so pagination no longer uses a `?page=N` query even for its background fetches.
- Old `/series?page=N` bookmarks remain usable. When one is opened, Soulseekarr removes the legacy `page` query from the visible address with `history.replaceState()` without reloading the page or losing the currently rendered page.
- The old `/api/series?page=N` endpoint remains as a compatibility alias for browser tabs left open across an upgrade. No database migration is required.

## v0.3.123

- Fixed the remaining false-positive CSRF rejection on Settings saves when a browser does not send `Sec-Fetch-Site: same-origin` and the backend-visible Host/port differs from the browser-visible Origin.
- Soulseekarr now uses a per-process synchronizer token for its own browser UI: normal HTML forms submit it as a hidden field and same-origin state-changing `fetch()` calls add it as `X-Soulseekarr-CSRF`. This lets legitimate UI requests survive Docker/reverse-proxy Host rewriting without weakening cross-site protection.
- Explicit `Sec-Fetch-Site: cross-site` requests are still rejected even if they somehow carry the token. Older/stale UI pages retain the strict Origin/Referer fallback, and Sonarr/qBittorrent server-to-server compatibility requests remain token-free. No database migration is required.

## v0.3.122

- Fixed legitimate Settings saves being rejected as cross-origin when the browser-visible host/port differs from the Host seen by the backend (for example with Docker port publishing or reverse proxies). The CSRF guard now trusts the browser-controlled `Sec-Fetch-Site: same-origin` signal, still rejects `cross-site`, and retains strict Origin/Referer validation as the fallback.

## v0.3.121

- Fixed another source of apparent season pinning in proactive upgrade searches: the installed-episode catalog was still requested from Sonarr with monitored-only filtering. An older season such as S06 could therefore disappear from the round-robin completely when Sonarr marked that season or its episodes unmonitored, leaving a monitored S12 as the only visible upgrade season.
- Once a TV show is enabled for downloads in Soulseekarr, proactive upgrades now build their season ring from **all installed Sonarr episodes**, including installed episodes in unmonitored seasons. Sonarr monitoring still controls Wanted/Missing episode searches; this change applies to already-installed upgrade targets.
- Stale queued-download alternative searches no longer run an independent visible season rotation. Every real automatic Soulseek search now advances the same persisted **TV show -> season -> episode** position, so queue recovery cannot search TBBT S12 and leave normal discovery thinking the previous TBBT season was something else.
- Queue recovery is anchored to the full current series season catalog. If S12 was the last TBBT search and the only stale queue item is also in S12, recovery yields that TBBT turn instead of immediately creating another S12 search; another eligible season must receive a real TBBT search first.
- Stale queued upgrade jobs can still be resolved after their old Sonarr season/episode becomes unmonitored. Legacy dedicated queued-retry cursor tables remain for database compatibility, but no longer drive target selection. No manual database migration is required.
- Added regressions for both production-shaped cases: S12 was last while an unmonitored S06 contains weaker-CF installed episodes, and stale S12 queue recovery attempting to bypass the global season cursor.

## v0.3.120

- Made Settings more compact by moving explanatory text from beneath controls into clickable question-mark help popovers beside the relevant setting title.
- Converted switch descriptions and redundant static settings notes into the same contextual help pattern while keeping live status information such as next automatic search and storage free-space readouts visible.
- Help popovers are keyboard-focusable and dismiss when focus moves away.


## v0.3.119

- Season 00 / specials now search Soulseek using **TV show title + Sonarr episode title** instead of S00E##/0x##. Per-series current/legacy episode-code preferences are intentionally ignored for specials.
- Special search-result matching now accepts the full Sonarr special title even when the shared filename/path has no S00E## marker, while explicit S00E## and 0x## releases remain valid. Series-title validation, language checks, size limits, peer availability, upgrade checks, and all other normal candidate filters still apply.
- Matching is punctuation-insensitive at token boundaries, so names such as `Example.Show.-.Christmas.Special.mkv` match a Sonarr title of `Christmas Special`.
- If Sonarr has no usable title for a Season 00 episode, Soulseekarr falls back to the normal S00E## query rather than issuing an empty/over-broad title search.
- No database migration is required.

## v0.3.118

- Added **Missing-episode delay** to Settings -> Search. Default remains 48 hours; `0` removes the extra post-air delay while still requiring a valid/non-future Sonarr `airDateUtc`. The setting applies consistently to automatic searches, manual missing-episode Search/Download actions, stale-queue recovery of missing episodes, and season-folder expansion. Upgrade searches remain exempt.
- Added **Stale queued-download retry** to Settings -> Search. Default remains 1 hour. This controls both how long a remotely queued transfer must sit before Soulseekarr searches for an alternative and the minimum interval between alternative searches for that episode. `0` disables stale-queue alternative searches without cancelling the original transfer.
- The Missing page now displays the configured delay rather than hard-coded `48h` text. Hourly queue-position health checks / seven-day non-improvement removal remain separate from the configurable stale-retry interval.
- No database migration is required; existing settings automatically receive the previous 48-hour / 1-hour defaults.

## v0.3.117

- Fixed the remaining round-robin wraparound bug. The season cursor previously returned the just-searched season at the end of its candidate list; when every other season was temporarily unavailable because of repeat-search cooldown, weak-score filtering, or queue blocking, selection could walk the list and fall back to the same season again.
- A multi-season show can no longer immediately reuse its last searched season. If no different season currently has an eligible target, that show yields the turn instead of wrapping back to the previous season. Single-season shows continue normally.
- The same no-immediate-wrap rule now applies to the episode ring inside a season. When the active ring contains multiple episodes, the episode searched on the previous turn is not offered again until another episode in that ring has received a turn.
- Stale queued-download recovery now has separate persistent TV-show/season/episode cursors. Queue-maintenance searches can no longer overwrite the normal automatic-discovery rotation position.
- Queued recovery rotates the jobs that are actually stale rather than reapplying proactive weak-Custom-Format-score target selection to jobs that were already chosen earlier. Candidate upgrade validation remains unchanged.
- Added regression coverage for the exact v0.3.116 failure mode where S01/S02 were cooling down and selection wrapped back to S12, plus persistence/isolation tests for the recovery cursors. New SQLite tables are created automatically; no manual migration is required.

## v0.3.116

- Fixed automatic season rotation for shows that have a mix of missing episodes and proactive upgrade targets. Season selection now happens before choosing whether the target is missing or an upgrade, so one target type can no longer pin a show to a single season.
- The persistent season cursor is anchored to the show's full known searchable season catalog, including temporarily cooled-down rows, instead of only the currently due target list.
- Missing episodes still win over upgrades inside the season selected by the round-robin. Upgrade episode selection inside that season keeps the v0.3.112 weaker-Custom-Format-score ring.
- Added a regression test reproducing the failure where several missing targets in S12 caused repeated S12 searches while S01/S02 upgrade targets were waiting. No database migration is required.

## v0.3.115

- Treats slskd terminal `Completed, TimedOut`, `Completed, Errored`, and `Completed, Aborted` transfers as transient transfer failures instead of stopping permanently after the first peer/release.
- Soulseekarr removes the dead terminal slskd row and immediately runs the normal episode search/filter/ranking pipeline for a different candidate.
- The failed transfer remains in History with its original slskd/Soulseek reason, plus the replacement Soulseekarr job id when an alternate is queued.
- Automatic retry chains exclude every already-attempted peer/file in that chain, preventing A -> B -> A loops, and stop after three alternate candidates to avoid runaway retry traffic.
- Recovery retries queue only the replacement episode file; they do not expand the peer's season folder. Ordinary automatic/manual searches keep normal season-folder expansion.
- `Completed, Rejected` remains under the existing rejection/peer-limit policy, and user-cancelled transfers are never auto-retried.
- Retry lineage/count fields are added to SQLite automatically; no manual database migration is required.

## v0.3.114

- Downloads table no longer renders an `Unknown` quality badge. If a download has no detected quality or its quality is `Unknown`, the Quality cell is left blank.
- The download details modal still exposes the raw quality value for troubleshooting.

## v0.3.113 - security hardening

- Hardened all local-path and folder-mapping boundaries used for completed-file discovery, staging, rename, resume, and cleanup. Lexical traversal, filesystem-root mappings, symlinked files, persisted path escapes, invalid planned filenames, and unexpected staging destinations are refused.
- Hardened `/config`: the directory and SQLite/secret/config files reject direct symlinks, private permissions are applied, `secret.key` is created without following links, settings reads are size-bounded regular-file reads, and settings writes are atomic. API keys remain Fernet-encrypted at rest.
- Treats persisted settings as untrusted input. Service URLs, API-key header values, container roots, folder mappings, booleans, and numeric limits are validated/bounded before use. Invalid saved service URLs fail closed.
- Sonarr/slskd HTTP clients no longer follow redirects and ignore environment proxy variables, preventing API-key forwarding to a redirect target or an injected proxy.
- Added browser request hardening: same-origin checks for state-changing requests, a 2 MiB request-body ceiling (including bodies without `Content-Length`), per-request CSP nonces, anti-framing/content-sniffing/referrer/permissions/cross-origin headers, and SRI on CDN assets. Inline event handlers were removed.
- Tightened the qBittorrent compatibility surface: the login cookie is `HttpOnly`/`SameSite=Strict`, category creation is bounded/validated, and add/delete remain explicitly read-only.
- Updated security-sensitive direct dependencies to current fixed lines and removed Uvicorn's unnecessary `standard` extras to reduce transitive attack surface.
- Hardened the example container with a read-only root filesystem, `no-new-privileges`, and a small `nosuid,nodev,noexec` `/tmp`.
- Added security regression coverage for traversal/symlink handling, config/secret permissions, hostile persisted settings, request-size/origin guards, CSP/SRI, and dependency pins. No database migration is required.

## v0.3.112

- Kept the persistent automatic **TV show -> season -> episode** round-robin, but made proactive upgrade selection score-aware *inside the selected season*.
- For an upgrade turn, Soulseekarr finds the highest known installed Custom Format score in that season. If any installed episodes are below it, only those lower-score episodes form the episode ring and are rotated in episode-number order.
- When every known installed episode in the season has the same Custom Format score, the full season becomes the ring and rotates by episode number. This avoids repeatedly spending searches on already-best episodes while weaker episodes remain.
- Repeat-search cooldown still applies. If weaker episodes exist but are cooling down, Soulseekarr does not fall through to a top-score episode merely because it is due.
- Missing-episode searches keep the configured **Sequential / Random** episode ring. Upgrade-search episode choice is intentionally score-aware instead; equal-score upgrade seasons fall back to episode-number order.
- Stale queued upgrade retries use the same lower-score preference when more than one queued upgrade target exists in a season.
- No database migration is required.


## v0.3.111

- Reworked automatic target selection into one persistent nested round-robin for **TV show -> season -> episode**.
- Proactive upgrade searches no longer hunt for the installed episode with the lowest Custom Format score or resolution. Missing episodes still have priority for a show, but when an upgrade turn is needed, the next season/episode is selected only by the round-robin cursors.
- Each time a TV show receives another real automatic search turn, Soulseekarr prefers the next eligible season. When that season comes around again, it continues with the next episode in that season instead of restarting at the first/weakest episode.
- Added a persistent per-show/per-season episode cursor in SQLite. The cursor survives container restarts and keeps its position even while the previously searched episode is temporarily excluded by repeat-search cooldown.
- **Sequential** uses episode-number order inside each season. **Random** now uses a stable randomized episode ring per show/season, so it stays random-looking without re-shuffling into repeats before the ring advances.
- Stale queued-download retries use the same show/season/episode cursor hierarchy.
- Sonarr Custom Format score and resolution are still used to decide whether a found release is a real upgrade; they simply no longer decide which installed episode gets searched next.
- Existing databases are upgraded automatically; no manual migration is required.


## v0.3.110

- Fixed stale queued-download retry searches so they use the same persistent per-show season round-robin cursor as normal automatic missing-episode searches.
- Returning to a TV show during queued-retry searching now advances to the next eligible season instead of continuing through consecutive episodes from the same season.
- Queued-retry searches now honor the **Sequential / Random** search-order setting; previously that maintenance path always followed sorted Sonarr episode IDs, which made Random appear ineffective.
- The TV-show and season fairness cursors remain authoritative in both modes; Random chooses the episode within the selected eligible season rather than randomizing away round-robin fairness.


## v0.3.109

- Active queued/downloading candidates that Sonarr now considers equal or worse are stopped from progressing to import immediately and removed automatically.
- Automatic supersede cleanup now prefers the persisted slskd transfer ID, avoiding a slow/full transfer-list lookup when cancelling a known transfer.
- If slskd cancellation temporarily fails, the runner is stopped, the job clearly shows automatic removal is pending, and later reconciliation retries it.


## v0.3.108

- Repairs persisted active planned filenames from older versions so Blu-ray Remux jobs keep `Remux` before preparation/import.
- Clears cached candidate scoring when such a filename is repaired so Sonarr can evaluate the corrected final name.


## v0.3.107

- Preserve the `Remux` modifier in planned/final Sonarr-style quality tags for Blu-ray remuxes.
- `Bluray-1080p Remux` and `Bluray-2160p Remux` now remain distinct from ordinary `Bluray-1080p` / `Bluray-2160p` encodes during pre-enqueue scoring and final filename generation.
- AVC/HEVC codec normalization remains unchanged (for example `AVC` -> `h264`).


## v0.3.106

- Added total and free filesystem space to both fallback-storage path settings.
- **Import path as Sonarr sees it** reports the capacity of Soulseekarr's actual `SOULSEEKARR_IMPORT_DIR` backing mount, since the Sonarr-visible path may use a different container path.
- **slskd downloads path in Soulseekarr** reports the capacity of that configured local mount.
- Missing/unmounted paths show a non-blocking `Space unavailable` message instead of delaying or breaking the Settings page.


## v0.3.105

- Added a persistent per-TV-show season round-robin cursor for automatic missing-episode searches.
- After a real automatic search in Season 1, that show's next turn prefers the next eligible season before returning to another Season 1 episode.
- TV-show round-robin and season round-robin are nested, e.g. `Alpha S01E01 -> Beta S01E01 -> Alpha S02E01 -> Beta S02E01`.
- Season progress survives Soulseekarr/container restarts and wraps back to earlier seasons only after later eligible seasons have had a turn.
- Skipped/blocked targets do not advance the season cursor; only a real automatic Soulseek search does.
- Proactive upgrade searches keep their existing weakest-file-first ordering.


## v0.3.104

- Increased the padding inside Active Downloads quality badges for slightly better readability while keeping the compact Sonarr-style appearance.


## v0.3.103

- Removed **Episode title** from the Active Downloads table to make the list more compact; the title remains available in the download details dialog.
- Right-aligned the **Size** column header and values for easier numeric scanning.
- Restyled **Quality** as a compact Sonarr-style gray rectangular badge with smaller text.


## v0.3.102

- Fixed automatic TV-show round-robin so the persistent cursor advances only when a real Soulseek search is launched, not when Soulseekarr merely inspects/skips an ineligible show.
- Queued-retry searches now use the same TV-show round-robin cursor instead of sorting by episode ID, preventing batches of retry searches from concentrating on one series.
- When multiple shows have searchable work, consecutive automatic Soulseek searches choose a different show before returning to the previous one. If only one show is actually eligible, Soulseekarr may continue searching that show.

## v0.3.101

- Added a per-TV-show episode search-format setting in TV Shows: `S01E01`, `1x01`, or `Both`.
- Every show defaults to the current `S01E01` format, including shows that existed before this release.
- `Both` keeps result-driven fallback from `S01E01` to `1x01`; single-format modes never search the disabled spelling.
- Automatic searches and the manual episode-search dialog both honor the per-show choice.

## v0.3.100

- Added an optional dynamic automatic-search limit based on transfers that are actually downloading.
- When dynamic mode is enabled, queued/preparing/importing jobs do not consume the limit.
- With a dynamic maximum of 5, automatic and queued-retry searches continue while 0-5 network transfers are active, even if the total active-job queue exceeds the static limit; they pause when the sixth transfer is active.
- The existing total active-job limit remains available as the default static mode.


## v0.3.99

- TV Shows now uses a persistent background Sonarr-series snapshot, matching the local-read architecture already used by Active Downloads.
- A dedicated worker refreshes Sonarr's series list about every 30 seconds and stores the last successful snapshot in SQLite.
- `/series` and `/api/series` now read only SQLite; opening TV Shows, filtering, pagination and adjacent-page prefetching never wait on Sonarr.
- The TV Shows page refreshes its visible data from the local snapshot every 10 seconds without reloading the document.
- The Refresh button requests an early background sync and returns immediately instead of waiting for Sonarr.
- Sonarr errors preserve the last good TV-show snapshot and are shown separately, and the persisted series list survives Soulseekarr/container restarts.


## v0.3.98

- TV Shows pagination is now asynchronous: Previous/Next swaps table data without a full page reload or page-number URL navigation.
- The next TV Shows page is prefetched in the background, with adjacent-page browser caching for near-instant navigation.
- TV Shows filtering and refresh use the same background JSON endpoint.
- Added a short-lived server-side Sonarr series cache so page prefetches do not repeatedly fetch the full series library.


## v0.3.97

- Active Downloads keeps in-progress/preparation/import work first, then sorts ordinary queued transfers by their known remote queue position with the smallest number first.
- Queued transfers whose remote queue position is not known are shown after all queued transfers with a known position.
- Peer-limit cooldown jobs remain at the very bottom of Active Downloads.


## v0.3.96

- Live slskd transfer polling is now owned by a dedicated background worker instead of the Downloads page or Sonarr polling.
- The worker continuously saves the latest successful active-transfer snapshot to SQLite, including progress/speed/state and rate-limited remote queue positions.
- `/downloads`, `/api/downloads`, and the qBittorrent-compatible Sonarr Activity endpoints read only the last saved local snapshot and never contact slskd directly.
- Opening Downloads therefore creates no live slskd request at all. The initial HTML already contains the latest saved snapshot; subsequent 3-second browser refreshes read SQLite only.
- Bulk transfer progress is persisted before optional remote queue-position probes, so a slow peer position response cannot hold back fresh speed/progress data.
- Background refresh failures preserve the last good snapshot and are surfaced separately in the Downloads UI. Snapshot state survives Soulseekarr/container restarts.

## v0.3.95

- Downloads navigation is now non-blocking: `/downloads` renders immediately from Soulseekarr's local SQLite state instead of waiting for slskd/remote queue-position calls.
- Live slskd progress starts refreshing asynchronously as soon as the page is visible.
- Downloads polling no longer overlaps when an upstream refresh takes longer than the 3-second UI interval.

## v0.3.94

- Fixed the **Cycle limits** Settings layout so **Automatic downloads per cycle** and **Active-download search limit** align at the top instead of the shorter first field being pushed down by the neighboring help text.
- Responsive stacking behavior is unchanged on narrow screens.

## v0.3.93

- Added a configurable **Active-download search limit** under Settings → Automation → Cycle limits.
- The default remains **100** active jobs. Automatic missing/upgrade searches and queued-retry searches pause when the active queue grows above the configured value.
- Manual episode searches and manual downloads remain available regardless of this automatic-search guard.

## v0.3.92

- Active Downloads now includes a dedicated **Size** column using human-readable file sizes.
- The info icon on every active-download row is now a button that opens a full details dialog instead of only showing a tooltip.
- Active-download details merge the latest live slskd snapshot with the persisted Soulseekarr job and show status/progress, exact byte counts, speed/ETA, queue position and strikes, peer-limit state, CF score/resolution, preparation state, all known paths/filenames, IDs, and timestamps.

## v0.3.91

- Staging/copy preparation now checks Sonarr's active command queue before starting disk-heavy work. Active `ManualImport`, `DownloadedEpisodesScan`, and legacy `DownloadedEpisodeScan` commands are treated as Sonarr import activity.
- Completed files wait in Soulseekarr with **Preparing: waiting — Sonarr is importing another file** instead of beginning a staging copy while Sonarr is already importing media.
- The single staging-copy lane resumes automatically after Sonarr becomes idle. Multiple waiting jobs share a short activity cache so they do not spam Sonarr's command API.
- A copy that is already in progress rechecks Sonarr periodically and pauses at an 8 MiB chunk boundary if an import starts mid-copy, then resumes from the same file position when Sonarr is idle again.
- Hardlink-preferred fallback staging also waits for an active Sonarr import before entering the staging lane; direct in-place mapped renames are unchanged because they do not copy media data.


## v0.3.90

- Active queued/downloading Soulseekarr transfers are now removed when Sonarr already has an **equal** file by the comparison profile, not only a strictly better one.
- Equality means the installed Sonarr file has the same Custom Format score and the same resolution as the Soulseek candidate. Keeping a duplicate transfer would only waste queue/bandwidth.
- Tradeoffs are still preserved: a candidate with higher resolution but lower CF score, or higher CF score but lower resolution, is not automatically removed.
- Pre-download/manual eligibility already rejects equal-or-worse candidates; this release makes the periodic active-download cleanup use the same rule.


## v0.3.89

- Remote-queued Soulseekarr downloads that have waited at least **1 hour** are now eligible for an automatic alternate-peer search even when the episode is no longer in Sonarr Wanted/Missing (for example an already-downloaded episode being upgraded).
- Queued retries use their own **1-hour retry cadence** instead of the ordinary per-episode repeat-search cooldown, so a newly queued file does not wait 24 hours before Soulseekarr looks for another peer.
- An alternate queued candidate may have the **same Custom Format score and same resolution** as the currently queued release, or be better on either dimension, as long as it is not worse on either. The original queued transfer is never cancelled merely because an alternative is added.
- Active candidate CF score/resolution stored on Soulseekarr jobs is reused for queued-retry comparisons when available, with Sonarr parsing as a fallback.
- A successful queued-alternative selection uses the normal **season-folder expansion**, so sibling missing episodes and valid upgrades from that peer are evaluated and queued using the same rules as ordinary automatic downloads.
- Queued retry searches are recorded under **History -> Jobs** with target type **Queued retry**, including the exact query, result counts, usable candidate count, queued count, and outcome.



## v0.3.87

- Added configurable database history retention (default 48 hours). Finished download/import jobs and search-cycle audit jobs older than the configured window are deleted from SQLite.
- Expired jobs also remove their owned downloaded-file repeat-protection rows; active queued/downloading/preparing/importing jobs are never pruned.
- History cleanup runs at startup, after saving Settings, and periodically while Soulseekarr is running.
- Configure the window in **Settings -> Downloads & Import -> Database history -> History retention** (1-8760 hours).

## v0.3.86

- Fixed proactive upgrade discovery so installed Sonarr files are not filtered by the 48-hour missing-episode air-date delay. If Sonarr already has an episode file, it is eligible to be considered as an upgrade-search target based on monitoring and the configured repeat-search cooldown.
- A lower Custom Format score / lower resolution installed file is therefore enough to make the episode an upgrade-search target; weakest installed episodes are still searched first.
- When an automatic cycle performs no Soulseek search, History -> Jobs now records concrete diagnostics: missing targets due/on cooldown/not-yet-48h, installed upgrade targets due/on cooldown, Sonarr-queue blocks, Sonarr metadata errors, and the configured repeat-search cooldown.

## v0.3.85

- Automatic scheduler cycles and manually triggered **Run auto cycle** runs now leave searchable audit entries under **History → Jobs**.
- Every actual Soulseek search records whether it was a **Missing** search or an **Upgrade** search, the exact query, raw result count, usable candidate count, downloads queued, and the final outcome.
- A cycle that runs but performs no Soulseek search still records why (for example cooldowns, Sonarr queue constraints, disabled configuration, or active-download limits).
- Search-cycle audit rows are kept out of **History → Downloads**, so download/import history remains file-only.
- The Jobs info dialog has a dedicated search-cycle view with source (automatic/manual), target type, exact query, result counts, queued count, status, and timestamps.

## v0.3.84

- Fixed **Run auto cycle** on the Missing page after Recent Jobs was moved into History. The handler no longer calls the removed page-local `refreshJobs()` function.
- Run auto cycle now reports completion, disabled/already-running state, and failures in an inline status banner instead of throwing a JavaScript `refreshJobs is not defined` alert.

## v0.3.83

- Manual/explicitly selected downloads are now compared against Sonarr's currently installed episode file immediately before enqueue. If Sonarr already has an equal-or-better file by Custom Format score/resolution, the Soulseek candidate is not queued.
- Candidate Custom Format score and resolution are persisted on each job when Sonarr can determine them. Active-download supersede cleanup prefers this persisted profile, so a later Sonarr parse failure cannot keep a known inferior queued/downloading candidate alive.
- History file details now show the persisted candidate Custom Format score and resolution when available.
- Existing jobs from older versions remain compatible; the cleanup falls back to Sonarr re-parsing and backfills the persisted profile when successful.

## v0.3.82

- Prevent duplicate active Soulseekarr jobs for one normalized slskd transfer.
- Repair old duplicate active rows at startup without cancelling the shared slskd download.
- Deduplicate Active Downloads API output as a final UI safety net.

## v0.3.81

- Fallback import staging can now prefer filesystem hardlinks instead of copying completed media files byte-for-byte. When the source and staging directory support hardlinks on the same filesystem, staging consumes no additional file data and completes immediately.
- Added **Settings → Downloads & Import → Prefer hardlinks for staging**. It is enabled by default. If hardlink creation is unsupported, crosses filesystems/mounts, or is rejected by permissions, Soulseekarr automatically falls back to the existing resumable copy path.
- Hardlink creation is atomic with respect to an existing partial staging copy: Soulseekarr first creates a temporary link and replaces the staging target only after linking succeeds.
- Symlinks are intentionally not used because Sonarr import/move and cleanup behavior is safer when the staged path is a real directory entry rather than a potentially dangling symbolic reference.

## v0.3.80

- Removed the obsolete **Automatic Sonarr import** setting from Settings and the persisted configuration model.
- Eligible completed downloads continue to be validated and handed to Sonarr automatically; there is no separate import-enable switch anymore.
- Legacy `auto_import` keys in existing `settings.json` files are ignored safely and disappear the next time Settings are saved.


## v0.3.79

- Sonarr import timeout is now a long-running, configurable wait intended for large imports and busy Sonarr instances.
- Default Sonarr import timeout is **120 minutes**.
- Settings → Downloads & Import allows **60–1440 minutes (1–24 hours)** in 15-minute steps.
- Existing installations with an older saved value below 60 minutes are automatically raised to 60 minutes on load.
- Runtime import waits also enforce the 60-minute minimum so a prepared file cannot fail after the old 10-minute default merely because Sonarr is still processing a large import.


## v0.3.78

- Automatic round-robin searches now fall back to proactive upgrades when a TV show has no immediately searchable missing episode. This includes completed shows and shows whose missing episodes are all inside the repeat-search cooldown.
- Proactive upgrade targets are chosen from monitored existing Sonarr files, weakest first: lowest Custom Format score, then lowest resolution, then season/episode order.
- Missing episodes still have priority whenever one is due for that show; upgrade probing only fills otherwise idle round-robin turns.
- Successful season-folder expansion continues through every sibling in the folder and queues all releases that Sonarr considers real upgrades, rather than stopping after one upgrade.
- Installed-file upgrade eligibility now accepts either a Custom Format improvement or a resolution improvement, provided the candidate is no worse on the other dimension and strictly better on at least one.
- Proactive upgrade searches use the same per-episode repeat-search cooldown and the same persisted TV-show round-robin cursor as missing-episode searches.


## v0.3.77

- Automatic Soulseek searches now rotate fairly across enabled TV shows instead of exhausting one show's missing episodes before considering the next show.
- The series rotation cursor is persisted in SQLite, so container/app restarts do not reset automatic search back to the first series.
- Each series keeps its existing internal episode/season ordering. A typical sequence can now be `Show A S01E01 → Show B S01E01 → Show C S01E01 → Show A S02E01`.
- Successful season-folder expansion still suppresses redundant searches for sibling episodes from that same covered season; the next actual search is then selected from the next eligible show whenever one exists.
- Shows whose episodes are still inside the repeat-search cooldown are temporarily skipped without resetting the round-robin position.


## v0.3.76

- Settings → Search now shows the next scheduled automatic search cycle using the live scheduler state, including a local-time countdown.
- The schedule display refreshes every 10 seconds and when the Settings tab regains focus, and reports running/disabled/paused/unconfigured states explicitly.
- Changing the scan interval immediately changes the displayed next scheduled time after saving.

## v0.3.75

- Automatic season searching no longer walks straight from `S01E01` to `S01E02` after a successful season-folder expansion.
- When the selected peer folder queues at least one sibling episode, Soulseekarr treats that season as covered for the current automatic pass and moves the next search to another season.
- Other missing episodes from the expanded season inherit the configured repeat-search cooldown, so the scheduler does not jump back to the same season on the next cycle; unresolved episodes become eligible again after the cooldown.


## v0.3.74

- Sonarr Activity now receives only Soulseekarr transfers that have actually started downloading, plus post-download preparation/import work.
- Soulseek-queued transfers and peer-limit cooldowns remain visible only in Soulseekarr Active Downloads.
- Sonarr qBittorrent compatibility polling no longer triggers remote queue-position lookups for hidden queued transfers.

## v0.3.73

- Remote peer download-limit rejections never remain in downloaded-file repeat protection; affected files stay eligible for their scheduled retry.
- Active downloads waiting for a remote peer-limit cooldown are sorted below ordinary queued/downloading work.


## v0.3.72

- Completed/prepared candidates no longer wait for another queued, downloading, or copying candidate for the same episode. If Sonarr says the completed candidate is eligible, Soulseekarr imports it immediately.
- Parallel same-episode transfers are left running. When a later candidate finishes, it is evaluated against Sonarr's then-current EpisodeFile and can still become a subsequent upgrade.
- Remote peer daily/weekly upload-limit rejections are now retryable instead of terminal. Soulseekarr recognizes `Too many ... today` and `Too many ... this week` variants, including failure-count limits.
- Retry scheduling is persisted per peer and displayed as an explicit **Next retry** timestamp. New files from that peer are blocked until the timestamp passes.
- Ordinary queued limits (`Too many files` / `Too many megabytes`) keep the 12-minute cooldown. Daily limits use a conservative 24-hour cooldown and weekly limits use 7 days because slskd evaluates rolling previous-day/previous-7-day windows and the remote peer does not expose the oldest contributing transfer time.
- Jobs failed by older Soulseekarr versions for recognized daily/weekly peer limits are revived on startup and enter the persisted retry flow automatically.


## v0.3.71

- Treats the exact Soulseek/slskd rejection reasons **Too many megabytes** and **Too many files** as temporary remote-peer queue limits instead of terminal download failures.
- Affected jobs stay active as **Waiting to retry** and are automatically retried after a **12-minute** cooldown, matching Nicotine+'s established requeue interval for these standard Soulseek queue-limit responses.
- The cooldown is persisted per peer in SQLite and is case-insensitive. During the cooldown, Soulseekarr will not enqueue any additional files to that peer; automatic searches can choose another eligible peer instead, and manual enqueue attempts return a retry-later response.
- A new queue-limit rejection extends that peer's cooldown. Restarting Soulseekarr does not clear it.
- Daily/weekly policy rejections such as `Too many files this week` or `Too many failures today` remain terminal and are not automatically retried.
- Jobs that older Soulseekarr versions incorrectly marked failed for the exact temporary queue-limit reasons are revived on startup and enter the same retry/cooldown flow.

## v0.3.70

- History file-details dialog adds **Allow download again** for files currently blocked by downloaded-file repeat protection.
- The action removes only the filename+size duplicate-protection record; the original job/history entry, timestamps, status, paths, and failure/rejection reason remain intact.
- Allowing a file again also clears that episode's repeat-search cooldown so automatic/manual search can consider it immediately.

## v0.3.69

- Automatically reconciles queued/downloading Soulseek candidates against Sonarr's current EpisodeFile about once per minute and at startup.
- If Sonarr already has a file that is no worse in both Custom Format score and resolution and strictly better in at least one, the lower Soulseek transfer is removed.
- Superseded-transfer removal also cleans slskd partial/completed files and Soulseekarr staging artifacts, while History records that Sonarr superseded the transfer rather than attributing it to a user cancellation.
- Equal releases and score-vs-resolution tradeoffs are left running; incomplete Sonarr/release metadata never triggers automatic deletion.

## v0.3.68

- Failure attribution in History is now explicit: the details dialog labels the source as **slskd / Soulseek**, **Sonarr**, or **Soulseekarr**.
- Terminal slskd states such as `Completed, Rejected` are described as download-layer failures that happened **before Sonarr import**, eliminating ambiguity with Sonarr rejection.
- When slskd exposes its persisted transfer `Exception`, Soulseekarr includes that detailed reason in the job message (for example, a remote-peer transfer rejection reason).
- Sonarr candidate rejections are explicitly identified as Sonarr import rejections when the returned reason itself does not already name Sonarr.

## v0.3.67

- History **Jobs** and **Downloads** tables now show an info button on every row.
- Clicking the info button opens a full job/file details dialog backed by the persisted job record, so it includes the latest status rather than only the abbreviated table data.
- The dialog shows episode identity, peer, file size, remote Soulseek path, slskd/local staging/import/Sonarr paths, transfer and job IDs, preparation state, queue-health state, created/updated/downloaded timestamps, and the full status message.
- Failed, rejected, and cancelled jobs highlight the recorded failure/rejection/cancellation reason.

## v0.3.66

- Combined Recent Jobs and completed download/import history under the single **History** navigation section.
- Added **Jobs** and **Downloads** tabs inside History.
- **Jobs** contains the former Recent Jobs table, including its 25-row pagination and 5-second live refresh.
- **Downloads** contains the former History table, including download/import status, pagination, live refresh, and the Clear downloaded history action.
- Removed the standalone Recent Jobs navigation item. The old `/jobs` URL redirects to `/history?tab=jobs` for compatibility.

## v0.3.65

- Moved **Recent jobs** out of the Missing Episodes page into its own `/jobs` navigation section.
- Added independent 25-row pagination for Recent jobs.
- Kept the 5-second live refresh on the dedicated jobs page, scoped to the currently viewed page.

## v0.3.64

- Moved the Sonarr TV shows selector to its own **TV shows** navigation section.
- Added server-side pagination for TV shows (25 per page).
- TV show filtering now searches the full Sonarr series list and preserves the filter across pagination.
- Missing Episodes keeps its own independent pagination.


## Simplified episode search variants

- Episode searches now use exactly two spellings: padded `S01E01` and padded `1x01`.
- Removed the non-padded `S1E1` and `1x1` variants from automatic fallback and from the manual search variant selector.
- Existing persisted variant cursors remain compatible when upgrading: old `S1E1` state maps to `S01E01`, and old `1x1` state maps to `1x01`.
- The existing result-driven fallback remains unchanged: Soulseekarr stays on the current spelling while it produces usable candidates and switches to the other spelling only after a successful search with no usable candidate.

# Soulseekarr v0.3.62

## Parallel better candidates for active downloads

- Automatic searches no longer skip an episode solely because Soulseekarr already has it queued or downloading. After the configured repeat-search cooldown, the episode can be searched again while the existing transfer remains untouched.
- A newly found release is added in parallel only when Sonarr parses it as genuinely better than the active candidates: its Custom Format score and resolution must be no worse than the strongest active values, and at least one of those values must improve. Equal or worse releases are ignored.
- When several Soulseekarr candidates are already active for the same episode, a new release must beat the active score/resolution frontier rather than merely beating the oldest transfer. Existing transfers are never cancelled just because a better candidate appears.
- Sonarr queue checks now ignore Soulseekarr's own qBittorrent-compatibility rows while still yielding to any real external Sonarr download. This allows parallel Soulseekarr alternatives without duplicating work owned by another download client.
- Stale queued-download recovery now uses the same improvement rule. When it finds a better release, it queues that release without cancelling the old queue and runs normal season-folder expansion from the new peer.
- Season-folder expansion can now add a better candidate for a sibling episode that already has a queued/downloading Soulseekarr transfer. Existing-file siblings still pass the normal Sonarr upgrade score/resolution checks first.
- Manual search/download is no longer blocked merely by Soulseekarr's own active compatibility queue row, so you can explicitly add a different candidate while another Soulseekarr transfer is active. Exact duplicate files remain blocked.

# Soulseekarr v0.3.61

## Upgrade priority and repeat-search cooldown

- Season-folder upgrade evaluation now prioritizes the weakest existing episode files first. Missing sibling episodes still come before optional upgrades; among episodes that already have a Sonarr file, lower **Custom Format score** is evaluated first, with lower **resolution** as the tie-breaker.
- Sonarr season lookups now request the embedded EpisodeFile metadata so this priority can be calculated without a separate API request for every episode. Unknown score/quality metadata is kept eligible but sorted after known weak files.
- Added **Repeat-search cooldown** under **Settings → Search → Schedule & pacing**. The default is **24 hours**, it can be adjusted from **0 to 720 hours**, and `0` disables the cooldown.
- The cooldown is persistent per episode in SQLite, so restarting Soulseekarr does not make the same episode immediately eligible again.
- Automatic cycles and stale-queue alternative searches respect the cooldown. A successful manual search bypasses the wait because it is an explicit user action, but it refreshes the timestamp so automation will not immediately repeat it.
- A failed slskd/Soulseek request does not start the long repeat-search cooldown. Normal automatic searches may retry on the next cycle; stale-queue recovery uses its separately configured retry guard.

# Soulseekarr v0.3.60

## Queue-progress strike system

- Queued Soulseekarr downloads now use a persistent hourly progress-strike system instead of being deleted solely because their queue age passed seven days.
- About once per hour, Soulseekarr asks the remote peer for the queued transfer's current place. A **smaller queue number** means the file moved toward the front and immediately resets its strike count to zero.
- The same queue number, a worse/larger number, or **no reported queue place** adds one strike. The latest positive position becomes the next comparison baseline; an unknown result keeps the previous known baseline.
- The threshold is **168 consecutive hourly strikes** (7 × 24, approximately one week without observed upward movement). At the threshold Soulseekarr rechecks that the transfer is still queued, then cancels/removes only that Soulseekarr-owned slskd transfer.
- Strike count, previous known place, and last hourly check are stored in SQLite, so Soulseekarr/container restarts do not reset or accelerate the counter. Once the transfer actually starts, its queue-health state is cleared.
- Queue-position probes keep the existing global five-second spacing. Background maintenance processes a bounded number per pass and waits for that throttle rather than bursting requests at peers.
- The Downloads progress text shows the strike counter once it is non-zero, for example `Queued #205 · 12/168`; its tooltip explains that movement toward `#1` resets the counter.
- The existing stale-queue recovery remains: a queue older than the configured stale-queue retry interval can still trigger an alternate search without cancelling the original queue entry; repeat searches now obey the configured per-episode cooldown.

# Soulseekarr v0.3.59

## Manual search variant selector

- The manual Soulseek results dialog now lists every generated episode search variant instead of only displaying the query that just ran.
- The episode's persisted current variant is selected automatically in the dialog.
- Choose any other variant and click **Search selected** to immediately run another manual search without closing the dialog.
- A manually selected variant becomes the shared current variant for that episode, so later automatic searches use the same spelling. If the selected search succeeds but produces no usable candidates, the normal result-driven fallback advances from that selection.
- The dialog distinguishes the variant that was just searched from the variant that is currently selected for the next search.

# Soulseekarr v0.3.58

## Result-driven episode search variants

- Episode search spellings are now **sticky when they work**. Soulseekarr no longer rotates blindly through episode-code spellings after every search.
- A successful search keeps the same query variant when at least one result remains usable after the normal pre-download eligibility pipeline.
- Soulseekarr advances to the next query spelling only when the current search succeeds but **zero usable candidates remain**. This includes searches where every hit is already downloaded/active, rejected by file/size/episode/series quality checks, rejected by language rules, or rejected by Sonarr upgrade Custom Format score/resolution checks.
- slskd/Soulseek search errors do **not** advance the query variant. A transient network/rate-limit failure therefore cannot burn through all keyword spellings.
- Manual search results now explain whether the current spelling is being retained or whether the next search will try the next variant. Automatic searches and stale-queue retry searches use the same state rule.

# Soulseekarr v0.3.57

## Automatic UI refresh after upgrades

- Every Soulseekarr page now checks the running server version in the background. If the backend version changes while the page is open, the UI automatically refreshes to the new version instead of continuing to run stale HTML/JavaScript.
- Version checks are lightweight and run every 30 seconds, plus immediately when a hidden tab becomes visible or the window regains focus. The version endpoint explicitly disables browser/proxy caching.
- Before reloading after a detected upgrade, Soulseekarr clears the browser Cache Storage API and unregisters any service workers. It then navigates through a one-time cache-busting URL so the HTML document itself cannot be satisfied from a stale HTTP cache.
- Soulseekarr UI HTML responses now include `no-store`/`no-cache` headers. Local static assets remain versioned (`app.css?v=<version>`), so the refreshed page naturally requests the matching asset revision.
- The temporary cache-busting query parameters are removed from the visible address bar after the refreshed page loads. Normal browser storage such as the remembered Settings tab is preserved because it is user preference state, not cache data.

# Soulseekarr v0.3.55

## Windows-path transfer tracking and completed-file lookup

- Fixed slskd transfer correlation for Windows drive and UNC paths when equivalent filenames are returned with different slash styles or casing. For example, `G:\\TV Shows\\The Mick\\...` and `g:/tv shows/the mick/...` now identify the same transfer.
- Soulseek usernames are matched case-insensitively during transfer correlation, while ordinary non-Windows remote paths retain case-sensitive path matching.
- Fixed the completed-file fast path to follow slskd's current default `${SOURCE_DIRECTORY}` destination layout. A remote path such as `g:\\tv shows\\the mick\\season 2\\episode.mp4` is now checked first as `<downloads>/season 2/episode.mp4` rather than assuming slskd preserved the entire remote directory hierarchy.
- Kept deterministic fallbacks for custom/legacy `${SOURCE_PATH}` layouts and downloads-root (`{}`) layouts, followed by the existing literal basename/size recursive lookup for unusual custom configurations.
- Queue-position requests now use the username reported by the matched slskd transfer, avoiding a case-normalization mismatch in the position endpoint.

# Soulseekarr v0.3.54

## Rate-limited remote queue position

- Queued slskd transfers now periodically ask the source peer for their current remote queue position through slskd's dedicated transfer-position endpoint.
- Queue-position requests are deliberately conservative: each transfer is queried at most once per minute and all queued transfers share a global five-second minimum spacing. Large queues therefore refresh round-robin instead of generating a burst, and repeated three-second Downloads-page refreshes reuse cached position data.
- HTTP 429 responses honor slskd's `Retry-After` value when supplied and do not trigger an immediate retry storm. Missing/non-responsive peer position data is treated as optional display metadata and does not break the Downloads page or Sonarr queue polling.
- When the peer reports a positive queue position, the Downloads **Progress** column now shows it inline, for example **Queued #205**. Until a position is known it continues to show **Queued**.

# Soulseekarr v0.3.53

## Windows-drive Soulseek path normalization

- Fixed completed-file handling for Soulseek results whose remote full path begins with a Windows drive root such as `G:\\TV\\Show\\...`.
- Soulseekarr now mirrors slskd's own path-root behavior: drive-letter roots are stripped when translating a remote Soulseek path into a relative local download path. This prevents Soulseekarr from looking for a literal `G:` directory that slskd never created.
- The same normalization is used for completed-file lookup, incomplete-file matching, and Sonarr release parsing. The original unmodified remote path is still retained for slskd transfer lookup/cancel/enqueue identity.
- UNC roots and SoulseekQt `@@share` roots are normalized the same way for consistency with slskd.
- This also gives drive-root downloads a direct-path fast path instead of forcing a recursive scan of the entire slskd downloads mount.
- Added regression coverage for `G:\\...` local lookup, slskd completed-file lookup, Sonarr parse requests, UNC paths, and SoulseekQt roots.

# Soulseekarr v0.3.52

## English / unspecified language candidate policy

- Soulseekarr now asks Sonarr to parse release languages before queueing a Soulseek download. Releases with no declared language (Sonarr `Unknown`) remain eligible.
- Releases that include **English** are eligible even when other languages are also listed. For example, English + French is accepted.
- Releases that explicitly contain only foreign languages are rejected before download. For example, French-only, German-only, Japanese-only, and German + Original without English are ignored.
- Language detection reads both Sonarr's mapped `languages` result and the parser-only `parsedEpisodeInfo.languages` result, so the rule still works when Sonarr can parse a release title but cannot fully map it yet.
- Search results, automatic searches, stale-queue retry searches, manual downloads, and season-folder expansion all use the same language filter.
- Upgrade preflight applies the language rule before Custom Format score / resolution checks. Foreign-only files cannot be downloaded merely because they have a better score.
- Sonarr ManualImport evaluation applies the same rule again after download. If media/import evaluation identifies the candidate as foreign-only, Soulseekarr skips it instead of importing it.
- Sonarr release parsing is cached per client/search and language checks run with bounded concurrency, avoiding duplicate parse calls when the same candidate is also evaluated as a Custom Format upgrade.

# Soulseekarr v0.3.51

## Upgrade preflight: higher Custom Format score without quality downgrade

- Existing Sonarr episode files are now treated as true upgrade targets before a Soulseek download is queued. For season-folder upgrade candidates, Soulseekarr asks Sonarr to parse the remote release name and calculate its Custom Format score before starting the download.
- A release is eligible only when Sonarr maps it to exactly the expected series/episode and its Custom Format score is **strictly higher** than the existing episode file's score. Equal or lower scores are skipped before download.
- Upgrade candidates may not reduce resolution. For example, an existing 1080p episode will reject a 720p candidate even when the 720p release has a higher Custom Format score. A 1080p or 2160p candidate can still qualify when its score improves.
- When several peer-folder releases qualify, Sonarr Custom Format score is the primary upgrade ordering; Soulseekarr's normal peer/file ranking is used as the tie breaker.
- The same score and resolution rules are checked again immediately before ManualImport. This protects against jobs queued by an older Soulseekarr version and against the existing Sonarr file changing while a candidate is downloading.

# Soulseekarr v0.3.50

## Restart-safe preparation checkpoints

- Preparing jobs now persist their exact preparation checkpoint in SQLite, including the normalized target filename and the local source/import paths needed to continue after a container recreation.
- Recovery resumes directly from the last durable preparation step instead of going back through slskd transfer discovery. This prevents false **file not found** failures after slskd has already removed a completed transfer record or Soulseekarr has already renamed the completed file.
- In-place renames use a write-ahead checkpoint: if the container stops after the filesystem rename succeeds but before SQLite records success, startup detects the normalized target and continues from it.
- Staging-copy intent is persisted before a job enters the copy queue. A partially copied staging file is retained across restart and copying resumes from its existing byte count instead of truncating and starting again.
- Recovered staging copies still pass through the single global copy lane, so recreating the container cannot accidentally start several HDD-heavy copies in parallel.
- A completed staging target can be promoted from its checkpoint even if the source disappeared immediately after the last byte was written, provided its exact expected size is present.

# Soulseekarr v0.3.49

## Preparing-stage visibility and serialized staging copies

- The Downloads **Progress** column now shows the live preparation step after a Soulseek transfer finishes: locating the completed file, normalizing the filename, renaming in place, waiting for the copy lane, copying, and finalizing the staged file.
- When a staging copy is required, the Progress column switches to a real copy-progress bar and percentage instead of showing the completed network-transfer percentage.
- Full-file staging copies are now globally serialized: only one Soulseekarr staging copy runs at a time and additional files wait in a **Waiting for copy slot** state. This avoids seek-heavy parallel copies on HDD-backed storage while allowing non-copy preparation work to continue in parallel.
- Copy progress is persisted in bounded increments and the existing cancellable chunk-copy behavior is retained, so the Downloads table refresh can show progress without turning every 8 MiB chunk into a database write.

# Soulseekarr v0.3.48

## Remove active downloads

- Added an **X** action to every removable row in the Downloads table. After confirmation, Soulseekarr cancels and removes the exact transfer from slskd before hiding the job locally.
- User removal cleans disposable files already created by the job: completed source files, Soulseekarr staging copies, and the job-specific staging directory. Empty local parent folders are pruned up to configured storage roots.
- Soulseekarr also cleans slskd's resumable **incomplete** file through the Files API and removes its empty directory chain. If slskd remote file management is disabled or cleanup is otherwise blocked, the download is still cancelled and the UI surfaces a cleanup warning.
- Completed files that were already moved into slskd's downloads directory are removed through the mounted path when possible, with the slskd Files API as a fallback; empty slskd download folders are pruned after API deletion.
- Staging copies are now written in cancellable chunks so removing a job during the copy phase cannot leave a worker thread recreating a deleted temporary file.
- Jobs already in the Sonarr **Importing** phase cannot be removed from the Downloads table because the Sonarr import command may already be moving the final library file.

# Soulseekarr v0.3.47

## Stale queue recovery

- A Soulseekarr-owned slskd download that remains queued past the **Stale queued-download retry** interval (default 1 hour) triggers another automatic Soulseek search for the same episode. The existing queued transfer is deliberately left in place; a different peer/file may be queued alongside it.
- Stale-queue retry searches obey the configured **Repeat-search cooldown**, use the normal search-variant cursor/rate limiting, respect the configured active-download automatic-search guard, Sonarr monitoring, per-series Soulseekarr toggles, the configured missing-episode air-date delay, and external Sonarr queue ownership.
- A queued alternative older than the configured stale-queue retry interval no longer blocks a completed alternative from being scored/imported by Sonarr. If the old queued copy eventually downloads later, it is evaluated normally as a possible Custom Format upgrade.
- The normal Download timeout now begins only once slskd actually starts/initializes a transfer. Remote queue waiting does not expire at the previous six-hour default.
- Soulseekarr only cancels/removes a tracked slskd transfer when it is **still queued for more than 7 days**. It rechecks the live transfer immediately before deletion so a download that just started is never cancelled. Manual/unowned slskd transfers are not touched by this weekly queue cleanup.

# Soulseekarr v0.3.46

## Professional Settings workspace

- Redesigned Settings into five focused sections: **Connections**, **Storage**, **Search**, **Automation**, and **Downloads & Import**.
- Added compact desktop side navigation with responsive horizontal tabs on smaller screens.
- Added a sticky **Save changes** bar, compact service cards with configuration status, clearer path mapping controls, grouped automation switches, and inline operational notes.
- Settings section selection is remembered in the browser so returning after a save keeps you in the same area.

# Soulseekarr v0.3.45

## v0.3.45

- Restyled the History table to match the compact Sonarr-style Downloads/Recent Jobs layout.
- History now uses one-line rows with icon-only status/details, separate Series/Episode/Episode title columns, truncated File/Peer cells with hover details, and compact Downloaded/Updated timestamps.


## Compact main-page tables

The **Missing episodes** and **Recent jobs** tables now use the same dense, one-line Sonarr-style layout as Downloads. Long series names, episode titles, filenames, and messages no longer make rows taller: primary text is truncated with the full value available on hover. Missing-episode actions use compact icons, and Recent Jobs now uses an icon-only status column plus a details icon for messages.

# Soulseekarr v0.3.43

## Default for newly added Sonarr shows

Settings now includes **Download newly added Sonarr shows by default**. Soulseekarr persists a download choice the first time it discovers each series, so the setting applies only to shows added later and changing the default does not rewrite existing TV-show switches. On the first v0.3.43 sync, existing installations preserve the effective behavior from earlier releases: existing Sonarr-monitored shows are seeded enabled, existing unmonitored shows are seeded disabled, and any explicit Soulseekarr choices are kept. New shows discovered after that bootstrap use the configured default regardless of their Sonarr series-level monitored flag; Sonarr season- and episode-level monitoring is still respected.

# Soulseekarr v0.3.42

## Stricter series-name recognition

Series recognition now treats an explicit series name in the episode filename immediately before `SxxExx` / `1x05` as authoritative. A conflicting or near-spelling basename such as `The Mich S01E05 ...` is rejected for Sonarr series `The Mick`, even if a parent folder contains `The Mick`. Short one-distinctive-word titles require the complete normalized title phrase, while existing intentional abbreviations such as `3rd Rock S04E10` for `3rd Rock from the Sun` remain supported.

# Soulseekarr v0.3.41

- Downloads: the **Progress** column now shows **Queued** (with a clock icon) while slskd has not started transferring the file, instead of showing a misleading `0%` progress bar.
- As soon as the transfer starts, the cell automatically switches back to the live progress bar and percentage.

# Soulseekarr v0.3.40

## Respect Sonarr episode monitoring

Soulseekarr now treats Sonarr's **season-level and episode-level monitored flags as authoritative**. A series being enabled in Soulseekarr only allows that series to be considered; an unmonitored season or an individually unmonitored episode is never searched or downloaded. The rule applies to the main Missing list, automatic searching, manual Search/Download actions, and season-folder expansion. An unmonitored Sonarr series can still be explicitly enabled in Soulseekarr, but only episodes whose season and episode are both monitored are eligible.

# Soulseekarr v0.3.39

## Per-series download selection

The main page now lists **every TV series in Sonarr**, including unmonitored series, with a per-series **Download** switch. Sonarr-monitored series default to enabled; unmonitored series default to disabled. Any switch the user changes is stored as an explicit Soulseekarr override in SQLite, so disabling a monitored series or enabling an unmonitored series survives restarts. Newly added series continue to inherit their current Sonarr monitored state until explicitly changed.

The series switch is authoritative for Soulseekarr searches: the Missing list, automatic cycles, manual Search/Download endpoints, and season-folder expansion all obey it. Soulseekarr now reads both Sonarr Wanted/Missing monitor partitions so an unmonitored series that is explicitly enabled can still have its missing episodes found and downloaded. The old global **Only monitored episodes/series** setting is no longer shown because per-series selection replaces it.

# Soulseekarr v0.3.38

## Automatic slskd transfer-history cleanup

Soulseekarr now cleans slskd's **Downloads** list automatically once per minute. It uses slskd's official completed-download cleanup endpoint, which soft-removes terminal transfer records such as **Succeeded, Errored, Cancelled, Timed Out, Rejected, and Aborted** while leaving Requested, Queued, Initializing, and In Progress transfers alone. The downloaded files themselves are not deleted by this cleanup, and Soulseekarr's own Downloads/History records remain stored in its SQLite database. This also runs immediately after startup, so stale terminal rows from before an upgrade are cleared without manual action.

# Soulseekarr v0.3.37

## Stronger series recognition

Candidate matching now separates **series identity** from the episode description. Only the remote path/folder/release text before the requested episode marker can identify the series, so words appearing in an episode title after `SxxExx` cannot create a false series match. Short/generic series names such as `Go On` require the full title phrase instead of accepting a weak 50% word overlap. Multi-word titles use adaptive token coverage, one-character title tokens such as the `X` in `The X-Files` are retained, and an explicit conflicting series year in the best matching release/folder segment rejects the candidate. This specifically prevents `WWE Confidential - S01E05 - Going in-depth on ...` from being accepted as `Go On (2012) S01E05`.

# Soulseekarr v0.3.36

## Downloads quality column

The compact **Downloads** table now includes a dedicated **Quality** column, using the same full-path release metadata parser Soulseekarr uses when normalizing imports. This means a plain episode filename can still show values such as `WEBRip-1080p` when the quality metadata exists in its Soulseek parent folder. Once a file has been normalized/prepared, that normalized filename becomes the quality source. The separate **Queue** column has been removed to keep the table tighter.

# Soulseekarr v0.3.35

## Missing-episode release-age guard

Soulseekarr uses the configurable **Missing-episode delay** (default **48 hours**) after Sonarr's `airDateUtc` before a missing episode can be searched or downloaded through Soulseek. Automatic cycles, manual Search/Download endpoints, stale-queue recovery of missing episodes, and season-folder expansion all enforce the same value. Episodes with a missing or invalid `airDateUtc` are normally treated as ineligible rather than guessed; Season 00 specials with a missing air date are allowed immediately. The Missing page keeps delayed episodes visible and shows the configured waiting period until they become eligible.

# Soulseekarr v0.3.34

## Automatic-search queue guard

Automatic searching pauses whenever Soulseekarr has more active jobs than the configured **Active-download search limit** (default **100**). The guard is checked both before an automatic cycle starts and again before each episode search, so a season-folder expansion that adds many sibling episodes can stop additional automatic searches during the same cycle. Queued-retry searches use the same limit. Manual searches and manual downloads are unaffected. Automatic searching resumes on a later cycle once the active queue drops back to the configured value or below.

# Soulseekarr v0.3.33

## Faster History page

History now uses a dedicated partial SQLite index containing only finished jobs in the exact newest-first order used by the History table. Previously, as the jobs table grew, SQLite had to scan/filter finished jobs and build a temporary sort for every History page load and every 5-second refresh. History count and page retrieval also now share one database connection. Existing databases get the new index automatically on startup; no migration or manual database maintenance is required.

# Soulseekarr v0.3.32

## Compact Sonarr-style Downloads table

The **Downloads** table now uses dense one-line rows inspired by Sonarr's Activity view. Series, episode number, episode title, peer, speed, queue position, time remaining, and progress each have their own compact column. Status is represented by a Font Awesome icon instead of a full badge, progress uses a narrow inline bar, and verbose transfer/file details are moved into hover tooltips so they no longer make rows taller. The full Soulseek source filename remains available by hovering the row/details icon.

# Soulseekarr v0.3.31

## slskd 429 enqueue protection

Season-folder expansion can start several Soulseekarr jobs at the same time. slskd serializes download-enqueue operations internally and can answer one of those overlapping `POST /api/v0/transfers/downloads/{username}` calls with HTTP 429 even though the peer and file are valid. Soulseekarr now uses one shared enqueue lane per slskd server, so sibling episode jobs queue one at a time instead of racing each other. If slskd still returns a transient 429 (for example because another application is enqueueing at the same moment), Soulseekarr honors `Retry-After` when present and otherwise retries with bounded exponential backoff before failing the job.

# Soulseekarr v0.3.30

## Longer configurable Sonarr import wait

Sonarr imports can take longer than the previous fixed limits, especially when a large file is being moved or replaced on slower storage. Soulseekarr now has a **Sonarr import timeout** setting under Settings -> Matching, defaulting to **10 minutes** (configurable from 1 to 60 minutes). The same timeout is used while waiting for the Sonarr `ManualImport` command and while waiting for Sonarr to confirm that the episode's `episodeFileId` has appeared or changed after the command finishes. This prevents Soulseekarr from marking a slow but still-running Sonarr import as failed too early.

# Soulseekarr v0.3.29

## Preserve release metadata in Sonarr Queue

The read-only qBittorrent compatibility API now sends Sonarr the **full active filename** instead of reducing queue items to only `Series - SxxExx - Episode title`. While a Soulseek transfer is active, Sonarr receives the actual peer filename basename, including release/quality tokens such as `WEBDL-1080p`, codecs, providers, audio tags, release groups, and the file extension. Once Soulseekarr has prepared/normalized the file for import, the compatibility API switches to that normalized filename.

This is important because Sonarr parses the download-client item title when calculating queue-side quality and Custom Format information. The simplified fallback remains only for jobs that genuinely have no filename available.

# Soulseekarr v0.3.28

## Sonarr Activity / Queue integration

Soulseekarr now exposes a **read-only qBittorrent Web API compatibility layer** so active Soulseekarr jobs can appear in Sonarr's normal **Activity -> Queue** without modifying Sonarr.

Add a second qBittorrent download client in Sonarr that points to Soulseekarr itself:

- Host: the Docker/network hostname or IP of Soulseekarr (for example `soulseekarr`)
- Port: Soulseekarr's HTTP port (normally `8080` inside the Docker network)
- URL Base: blank
- Category: `soulseekarr`
- Username/password/API key: blank
- Priority: `50` (lowest)
- Add a dedicated Sonarr tag such as `soulseekarr-queue-only` to this download client and **do not assign that tag to any series**. This keeps Sonarr polling the client for Queue status while excluding it from normal release grabs when untagged real download clients are available.

The compatibility API intentionally rejects qBittorrent `add` and `delete` operations. Soulseekarr remains responsible for downloads, Custom Format candidate selection, Sonarr manual import, and cleanup. Active jobs are never exposed as completed/seeding torrents, so Sonarr does not run Completed Download Handling on these synthetic Queue items.

# Soulseekarr


> **Update safety:** release archives contain `docker-compose.example.yml` only. They intentionally do **not** contain `docker-compose.yml`, so extracting a new Soulseekarr version cannot overwrite your working Compose configuration.


Docker-first Sonarr + slskd missing-episode recovery app. Radarr is intentionally left for a later adapter.


## v0.3.27 season-folder expansion

When a Soulseek file is selected for download, Soulseekarr now asks slskd for the contents of that file's **exact remote directory** using `/api/v0/users/{username}/directory`. If the same folder contains files that cleanly match other Sonarr episodes from the same series and season, Soulseekarr queues those files too. This turns a single good season-pack result into a season download without performing a separate Soulseek search for every episode.

Folder expansion includes Sonarr episodes that already have files as well as missing episodes. Existing files are handled by the v0.3.26 Custom Format upgrade logic after download: Sonarr scores the candidate and Soulseekarr only imports it when it is a valid higher-scoring upgrade. Episodes already in Sonarr's queue, episodes already active in Soulseekarr, repeat-protected files, wrong-season files, non-video files, and files that do not pass the normal episode/series/size checks are skipped. The **Monitored only** setting is respected.

The explicitly selected file is always queued first. Remote folder inspection is best-effort; if the peer goes offline or refuses the directory request, the selected download continues normally and the UI shows a warning instead of failing the user's download. Manual and automatic downloads both use this season-folder expansion. The result modal stays open and reports how many episodes were queued from the folder.

## v0.3.26 Sonarr Custom Format upgrade selection

Soulseekarr now uses Sonarr's own **Custom Format score** when multiple Soulseek downloads target the same episode. Completed files are prepared and held until all Soulseekarr downloads for that episode have finished. Soulseekarr then asks Sonarr to parse every candidate, reads each candidate's `customFormatScore`, and selects the highest-scoring Sonarr-approved file.

If Sonarr already has an episode file, Soulseekarr also reads that EpisodeFile's current `customFormatScore`. A downloaded candidate is considered an upgrade only when its score is strictly higher than the existing score, and Sonarr must still return no Manual Import rejections. Equal/lower-scoring candidates are recorded as `skipped` and cleaned up. This keeps Sonarr's quality/profile rules authoritative while using Custom Format score to decide between valid candidates.

Upgrade evaluation is **per episode**, not global: Soulseekarr waits for all downloads of the same episode, but an unrelated slow download cannot block another episode from importing. The selected candidate is imported with Sonarr `ManualImport`; when replacing an existing file, Soulseekarr verifies that Sonarr's `episodeFileId` actually changes before declaring the upgrade successful. Prepared paths are persisted so waiting candidates survive a Soulseekarr restart.

## v0.3.25 literal completed-file lookup

Fixed completed Soulseek downloads whose filenames contain glob metacharacters such as square-bracket release tags (`[TAoE]`). The fallback filesystem locator previously passed the literal filename to `Path.rglob()`, causing characters like `[]`, `?`, and `*` to be interpreted as glob syntax. Soulseekarr now checks the direct remote-relative path first, then traverses with a fixed wildcard and compares filenames literally plus exact byte size. This prevents valid completed files from spending the full locator timeout and then failing as "could not be located" solely because of release-name punctuation.

## v0.3.24 table pagination

The **Missing**, **Downloads**, and **History** sections are now paginated at 25 rows per page. Each table shows its visible range and total count with compact Previous/Next controls. Downloads and History keep their live-refresh behavior on the currently selected page; if the number of rows shrinks and the current page no longer exists, Soulseekarr automatically moves to the last valid page. History pagination is backed by SQLite `LIMIT`/`OFFSET` queries instead of loading the full completed-job history into the page.

## v0.3.23 completed-file path diagnostics

When slskd reports a successful transfer but Soulseekarr cannot locate the completed file, the History error now records the complete path-resolution chain: the original Soulseek remote path, expected byte size, the completed-file path returned by slskd (or that none was returned), the mapped Soulseekarr path, mapped Sonarr path, fallback Soulseekarr search root, the Sonarr staging path that would have been used, and the configured folder mappings. The message explicitly states that no Sonarr request was sent when the file could not be located.

## v0.3.22 recursive empty-folder cleanup

Post-import cleanup now removes empty temporary directory chains, not only the immediate directory that contained the downloaded/staged file. Soulseekarr walks upward with non-recursive `rmdir()` calls until it reaches a configured protected root such as `/downloads`, `/slskd-downloads`, or a folder-mapping root. Any non-empty directory stops the walk, so unrelated files and folders are never removed. This also cleans nested temporary directories left behind after Sonarr moves an imported file.

## v0.3.21 multi-download result modal

The Soulseek result modal now stays open after a file is queued. The selected row changes to **Queued**, while the remaining result buttons stay available so multiple different files can be downloaded for the same episode when needed. Manual downloads no longer reject a second active Soulseekarr job solely because it targets the same episode; duplicate-file history, active-file protection, and Sonarr queue protection still apply. Automatic cycles continue to keep one active Soulseekarr download per episode.

## v0.3.20 Sonarr path diagnostics

Sonarr-stage failures and rejections now include the exact full file path and manual-import folder path that Soulseekarr presented to Sonarr. This includes command failures/timeouts and the case where a `ManualImport` command completes but Sonarr never creates an episode file.

## v0.3.19 verified Sonarr imports

- A completed Sonarr `ManualImport` command is no longer treated as proof that the file imported.
- After the command completes, Soulseekarr polls `/api/v3/episode/{id}` and requires `hasFile=true` plus a non-zero `episodeFileId`.
- If Sonarr finishes the command but the episode still has no file, the job is recorded as `failed` instead of `imported`.
- Download/staging cleanup still follows the configured lifecycle and runs after the attempt, including failures.

## v0.3.18 stale slskd transfer reconciliation

- Active Soulseekarr download jobs now notice when their matching slskd transfer has been manually removed.
- A transfer that had previously been seen is marked `cancelled` after a short missing-transfer grace period instead of lingering until the full download timeout.
- A newly queued transfer gets a longer first-appearance grace period to avoid false cancellation while slskd is creating the transfer row.
- Before cancelling a missing transfer, Soulseekarr checks slskd's completed-download files. If the file already exists, processing continues into Sonarr import.
- Cancelled jobs leave Active Downloads and remain visible in History with the reason.

## v0.3.17 clear downloaded-file history

The **History** page now has a **Clear downloaded history** button. It asks for confirmation, then clears Soulseekarr's persistent duplicate-download memory so a previously downloaded Soulseek file can be selected again. Completed job/import history is stored separately and remains visible after the reset. The reset also survives container restarts; the one-time legacy history migration will not silently repopulate cleared duplicate history.

## v0.3.16 automatic-search master switch

Settings now include **Enable automatic search**, enabled by default. Turning it off prevents both scheduled automatic cycles and the manual **Run auto cycle** action from starting Soulseek searches. Normal per-episode manual search/download remains available. The existing **Automatic downloads** setting remains separate and continues to control whether the automatic cycle is allowed to queue downloads.

## v0.3.15 Sonarr queue duplicate protection

Soulseekarr now checks Sonarr's active queue before searching or downloading a missing episode. Manual searches are blocked with a clear message when the episode is already present in Sonarr's queue, and the Missing page marks those episodes as **In Sonarr queue** instead of offering a Search button. Automatic cycles check the queue immediately before each Soulseek search and then check again after the search, before creating a download job, so a release grabbed by Sonarr while Soulseek is searching is still protected from duplicate download. Direct manual download requests also re-check Sonarr's queue before creating a Soulseekarr job.

## v0.3.14 paced automatic searches

Automatic cycles are now globally serialized and paced. Soulseekarr starts exactly one episode search, waits for that slskd search to fully finish, then enforces the configured **Minimum auto-search gap** before the next automatic episode search may start. The default gap is **5 seconds** and can be changed in Settings from 1 to 300 seconds. The cooldown is completion-to-next-start and carries across cycle boundaries. A scheduled cycle and a manually triggered cycle can no longer interleave their searches; if an automatic cycle is already running, another trigger is ignored instead of creating a second walker through the missing list. Manual episode searches keep their existing behavior.

## v0.3.13 search order, history, and empty-folder cleanup

Automatic missing-episode searches now have a **Sequential** / **Random** order setting. Sequential is the default and deterministically walks series, season, and episode numbers from lowest to highest; Random shuffles the eligible missing episodes for each automatic cycle. Manual searches are unchanged.

A dedicated **History** tab now shows finished Soulseekarr jobs with whether the Soulseek file was actually downloaded, the final import status (`imported`, `rejected`, `failed`, etc.), peer, file, result message, and timestamps. The active Downloads page is now focused only on in-progress work.

After post-import file cleanup, Soulseekarr also attempts to remove the **immediate source folder** when it has become empty. Removal is non-recursive and configured mount roots are protected, so a folder containing any other file/subfolder is left untouched.

## v0.3.12 download history and queue ordering

Soulseekarr now records every successfully completed Soulseek network download in persistent SQLite history before Sonarr import begins. Repeat protection uses the case-insensitive basename plus exact byte size, so the same file is recognized even when different peers expose it under different parent folders. Previously downloaded files and files already active in another Soulseekarr job are hidden from manual search results, skipped by automatic download selection, and rejected if an old UI tries to queue them directly. Existing pre-v0.3.12 jobs that clearly reached import handling are backfilled into the history table at startup. The dedicated History page exposes these records together with each job's final import status.

Live transfer speed on the Downloads page is now shown in decimal **Mbit/s**. Active/non-queued jobs are ordered by progress with the highest percentage first; queued/waiting downloads are placed after them and ordered by file size with the smallest first.

## v0.3.11 import cleanup

After a successful slskd download, Soulseekarr now always attempts the Sonarr manual-import flow and waits for Sonarr's import command to reach a terminal state. When that import processing finishes, Soulseekarr removes the completed Soulseek download and any temporary staging copy whether Sonarr imported it successfully, rejected it, or the import command failed/timed out. Successful `ManualImport` operations still use `move`, so the Sonarr library copy is never deleted by Soulseekarr cleanup.

If Soulseekarr cannot unlink the original completed download directly, it falls back to slskd's Files API. That fallback requires slskd `remote_file_management` to be enabled. For the simplest setup, keep the completed-download mount writable in Soulseekarr. Cleanup errors are retained in the job-history message instead of being hidden.

## Current flow

1. Read all Sonarr series plus both monitored and unmonitored Wanted / Missing partitions, then keep only episodes whose series is enabled on Soulseekarr's main page. Episodes already represented in Sonarr's queue are not searched or downloaded by Soulseekarr. Automatic searches re-check the queue immediately before searching and again before queueing a Soulseek download.
2. Search Soulseek through slskd using exactly **one episode-code variant per search attempt**. Soulseekarr uses only the padded forms `S03E01` and `3x01`; the current form remains sticky while it produces usable candidates and switches to the other form only when a successful search produces none. Search creation is serialized across the app and automatically retried with backoff when slskd returns HTTP 429. Automatic cycles additionally enforce the configured completion-to-next-start gap between episode searches and cannot overlap each other.
   Soulseekarr creates one slskd search and then polls its state until slskd exposes the `endedAt` finalization marker. Only after that does Soulseekarr return the result set. Soulseekarr's setting is shown in **seconds**, but the value sent to slskd is converted to **milliseconds** because slskd currently passes it directly into Soulseek.NET `SearchOptions.SearchTimeout`, whose unit is milliseconds. The timeout is an inactivity timeout measured from the last response. Persisted rows are then collected from `/api/v0/searches/{id}/responses` plus the embedded-response form as a compatibility fallback. If slskd has not finalized within the safety window, Soulseekarr reports a timeout instead of falsely reporting “no results”.
3. Match against the **full remote path**, not only the basename. This allows a short filename such as `S03E03.mkv` to use the series/release information in its parent pack folders.
4. Filter locked results, wrong episode numbers, non-video extensions and implausible sizes.
5. Keep each Soulseekarr-created search visible in slskd for one hour after Soulseekarr finishes collecting it, then remove it automatically. Search IDs and cleanup deadlines are persisted in SQLite so cleanup survives a Soulseekarr restart.
6. Remove files already present in Soulseekarr's persistent download history (and files already active in another Soulseekarr job), then rank the remaining results by series-title match, peer upload speed and a progressive logarithmic queue-length penalty. The duplicate-download memory can be reset from **History → Clear downloaded history** without deleting completed job/import records.
7. Queue the selected result in slskd. Before moving on to another search, ask slskd for the selected file's exact remote directory. If sibling files in that directory map to other Sonarr episodes in the same season, queue one valid file for each matching episode as well. Existing episodes may be included so Sonarr can evaluate them later as Custom Format upgrades; Sonarr-queued/active/repeat-protected episodes are skipped.
8. Poll slskd until the transfer reaches `Completed, Succeeded` (or a terminal failure state). As soon as the network download succeeds, record its basename + exact size in persistent download history before import processing continues.
9. Ask slskd's Files API for the completed file's `fullName` and resolve any configured folder mapping.
10. Build a Sonarr-friendly filename from Sonarr's series/episode data plus release metadata found anywhere in the Soulseek path (including parent folders). For example: `3rd Rock from the Sun (1996) - S02E25 - A Nightmare on Dick Street 1 [AMZN][WEBDL-1080p][EAC3 5.1][h264]-SiGMA.mkv`.
11. If the mapped Soulseekarr download mount is writable, rename the completed file in place. If it is read-only, a name collision exists, or no direct mapping is available, copy the file into Soulseekarr's staging folder using the normalized name.
12. Hold the prepared file until all Soulseekarr downloads for the **same episode** have finished. Other episodes do not block this batch.
13. Ask Sonarr `/api/v3/manualimport` to parse every prepared candidate and use Sonarr's returned `customFormatScore` for comparison. Candidates with Sonarr import rejections are not eligible.
14. If the episode already has a file, read its Sonarr EpisodeFile `customFormatScore`; only candidates with a strictly higher score are upgrade candidates. If the episode has no file, select the highest-scoring Sonarr-approved candidate.
15. Submit only the selected candidate through Sonarr's `ManualImport` command using Sonarr's parsed quality/languages/release metadata. `downloadId` is deliberately omitted because this is an untracked manual import.
16. Wait for Sonarr's import command to finish and verify the actual EpisodeFile. For an upgrade, the `episodeFileId` must change; for a missing episode, Sonarr must create a non-zero `episodeFileId`. Lower/equal-scoring candidates are recorded as `skipped`.
17. Delete all completed Soulseek downloads and fallback staging copies from the evaluated batch **regardless of the Sonarr outcome**. Sonarr's final library file is not one of the cleanup paths. If local deletion is not possible, Soulseekarr tries slskd's Files API as a fallback. Empty temporary folder chains are removed up to protected mount roots.

Jobs are persisted in SQLite under `/config` and active jobs resume after a container restart. The **Downloads** tab shows all active Soulseekarr jobs (queued, downloading, copying, ready/evaluating, and importing) and refreshes live slskd progress every three seconds, including transferred bytes, percentage, average speed in Mbit/s, peer queue position, ETA when available, and the final Sonarr-processing stage. Active transfers are ordered by progress (highest first); queued/waiting items are ordered by size (smallest first). Finished jobs and persistent repeat-download records are shown in the separate **History** tab.

## Folder mappings

Folder mappings solve the common Docker case where the **same host directory** has different paths in slskd, Soulseekarr and Sonarr.

Example:

| Container | Path |
| --- | --- |
| slskd | `/app/downloads` |
| Soulseekarr | `/slskd-downloads` |
| Sonarr | `/data/soulseek` |

Add this in **Settings → Folder mappings**:

```text
slskd:        /app/downloads
Soulseekarr:  /slskd-downloads
Sonarr:       /data/soulseek
```

Then a file reported by slskd as:

```text
/app/downloads/TV/Example.Show.S02E03.mkv
```

is resolved as:

```text
Soulseekarr: /slskd-downloads/TV/Example.Show.S02E03.mkv
Sonarr:      /data/soulseek/TV/Example.Show.S02E03.mkv
```

Multiple mappings are supported. The most specific (longest) matching slskd path wins, so subfolders can override a broader mapping.

For direct mapped imports, the host download directory must also be mounted into Sonarr at the configured Sonarr path. A **read/write** Soulseekarr mount allows in-place filename normalization and direct cleanup after import. If Soulseekarr sees that folder read-only, it automatically uses the fallback staging directory for import and then tries slskd's Files API to delete the original completed download; that API fallback requires slskd `remote_file_management`. Sonarr needs whatever permissions are required by the selected manual-import mode. The current import mode is `move`.

## Run

For a **new install only**, copy the example and then edit your working file:

```bash
cp docker-compose.example.yml docker-compose.yml
```

For an **update**, keep your existing `docker-compose.yml`; do not copy the example over it. The release archive intentionally contains no `docker-compose.yml`.

Configure your working `docker-compose.yml`:

- `/path/on/host/slskd-downloads` = the host directory configured as slskd's completed download directory. Mount it read/write so Soulseekarr can normalize and delete completed files directly. Read-only can still import through staging, but deleting the original then depends on slskd `remote_file_management`.
- Mount that same host directory into Sonarr if you want direct mapped imports.
- `/path/on/host/soulseekarr-import` = a dedicated fallback staging directory.
- Mount the fallback staging directory into Sonarr too. It does **not** need to have the same container path; Settings must contain the path as Sonarr sees it.

Then:

```bash
docker compose up -d --build
```

Open `http://<docker-host>:8787/settings` and enter:

- Sonarr URL and API key
- slskd URL and a read/write API key
- one or more folder mappings when the containers use different paths
- fallback staging paths

Automatic search is enabled by default. Use **Enable automatic search** as the master switch for scheduled/manual auto cycles. Start with **Automatic downloads** disabled. Test a few episodes manually, then enable it. Scheduled auto mode performs **one episode bundle per search window**: due **Missing** episodes come first; when Missing work is exhausted, upgrades are considered in Sonarr cutoff-unmet, weaker Custom Format score/source/resolution, then exploratory order. Persistent TV show -> season -> episode round-robin cursors remain fairness tie-breakers among similarly old targets. **Episode search interval** controls the minimum completion-to-next-start delay between different episode bundles (2 minutes by default on new installs). **Additional-query gap** controls only related query forms inside the same bundle (15 seconds by default). Both-mode ordinary episodes run `SxxEyy` then legacy `NxEE`; specials run `Series + episode title` then `S00E##`, and results are merged before selecting a candidate. The **Base repeat-search cooldown** remains adaptive: recent empty Missing searches retry sooner, repeated misses and exploratory upgrades back off longer, and transport/API errors retry quickly. Keep a non-zero base (24 hours is the default) for normal use; `0` deliberately disables per-episode adaptive/repeat delays. After selecting a manual search result, Soulseekarr opens the **Downloads** tab so the transfer/import can be followed without opening slskd.

## Security

Soulseekarr is a **trusted-network application and does not provide user authentication**. Do not publish port `8787` directly to the public Internet. For remote access, place it behind an authenticated reverse proxy or use a VPN. The qBittorrent-compatible login endpoint exists for Sonarr protocol compatibility; it is not an authentication boundary for Soulseekarr itself. The same-origin request checks and browser security headers reduce browser-origin attacks, but they do not replace access control.

Sonarr and slskd API keys are encrypted at rest with a Fernet key generated in `/config/secret.key`. Soulseekarr applies private permissions to `/config`, the key, settings JSON, and SQLite database where the platform permits it, and refuses direct symlinks for those sensitive files. Back up `secret.key` together with `settings.json`; losing the key makes saved API keys unreadable.

Configured Sonarr/slskd URLs are restricted to ordinary HTTP(S) origins. Redirect following and environment-proxy inheritance are disabled for API-key requests so credentials are not forwarded to redirect destinations or proxy settings. Private/LAN target addresses are intentionally allowed because those services normally run on the local network. Use HTTPS whenever API keys cross an untrusted network.

Downloaded files, staging files, persisted preparation paths, and folder mappings are treated as untrusted path data and are containment/symlink checked before file operations. The example Compose file additionally uses a read-only container root filesystem, `no-new-privileges`, and a restricted `/tmp`. The configured download/staging bind mounts remain writable by design, so a compromise of the Soulseekarr process can still affect data available through those mounts; expose only the directories Soulseekarr actually needs.


## slskd rate limiting

Soulseek/slskd may reject closely spaced search creations with HTTP `429 Too Many Requests`. Soulseekarr therefore has two user-facing pacing layers plus a final internal slskd gate. **Episode search interval** spaces different automatic episode targets; **Additional-query gap** spaces only alternate query forms for the same target. Soulseekarr also shares one low-level search-creation gate per slskd server across manual and automatic searches, honors `Retry-After` when supplied, and otherwise uses bounded exponential backoff. Each TV show can use `S01E01`, `1x01`, or both; `S01E01` is the default. With `Both`, scheduled/targeted automatic discovery runs both forms in one bundle and merges the eligible results. Specials similarly run title form then `S00E##`. A transient search error stops the remaining bundle rather than cascading into more requests. Manual variant selection still launches only the spelling explicitly selected by the user.

## Matching policy

Soulseekarr performs a conservative path/size/type pre-filter, but Sonarr is the final authority. Series and release metadata may come from either the filename or any parent directory in the Soulseek result path. A file is not imported if Sonarr reports any rejection or recognizes a different episode.

## Filename normalization

Before Sonarr validates a completed download, Soulseekarr builds a clean filename from authoritative Sonarr episode data and the release metadata visible in the complete Soulseek path. It currently recognizes common provider tags (for example `AMZN`, `NF`, `DSNP`, `ATVP`), source/resolution (`WEBDL`, `WEBRip`, `Bluray`, `1080p`, `2160p`), audio/channels, video codec, 10-bit/HDR markers, and common release-group suffixes. Metadata is never invented when it is not present in the remote path.


## Search diagnostics

The results modal shows slskd's state counters, what Soulseekarr collected, whether slskd finalization (`endedAt`) was observed, and how many finalization polls were needed. Soulseekarr does not return an empty result set while the slskd search is still in progress. The same diagnostic data is written to the container log after every search.
