# Daily Check-In Archive

Adds Daily Check-In alongside Pulse and Spotlight in Archive, using the same section tabs, filter-button and option-chip classes, spacing and card framing. The banner uses shared artwork references and a static title rather than downloading another video.

All verified members can read completed London community days. Today/future/deleted posts are excluded. All retained history stays available; the default window is the previous 30 completed days. Your Check-Ins, Feeling, Date, Name and Replies filters combine with Newest/Oldest/A–Z/Z–A sorting. Posts keep stored customisation snapshots; legacy posts retain the existing profile-style fallback when no snapshot was recorded. Existing replies expand read-only. Authors can delete their own past posts with confirmation; uniqueness and EXP grant markers remain unchanged.

## Storage and request bounds

Existing SQLite records are queried directly: no daily duplicate archive, media copy, global in-memory history or automatic expiry. Posts/replies page at 20 (server maximum 50); replies are fetched only on expansion. SQL uses additive partial indexes and deterministic, viewer/filter/day-bound keyset cursors. Name searches are bounded and escaped. No archive read grants EXP or creates a post/reply. Today’s participation gate remains on the current feed.

Synthetic planning sample: 36,500 short posts plus 73,000 short replies and reply slots took about 59 MiB including the archive indexes. This is not a measurement of production usage or a guaranteed capacity estimate; it excludes other app data, WAL, backups and longer artwork metadata. Main Render disk was configured at 1 GB when checked. Current free space was not available through the connector. No disk resize, paid service or external backup job was introduced.

## Verification

API tests cover signed/verified access, London rollover, default/custom date boundaries, combined filters, escaped name search, all sorting cursors/ties and viewer/parent/filter binding, bounded pages, lazy replies, author-only deletion, unchanged grants/uniqueness, legacy schema migration and module import order. UI checks cover filtering, query limits, stale responses, forbidden responses, lazy reply pages, own deletion/failure handling, and local demo isolation.

Broader API suite retains its existing failure in `tests/test_daily_checkin.py::DailyCheckInTests::test_feeling_and_message_are_both_required` (fixture reads missing exp_grants table). Offline frontend checks exclude the two older live-catalog curl tests (`reward-claim-heal.test.js`, `stamp-reward-label.test.js`) and retain four existing failures: discussion-host-inbox.test.js, feature-admin-care-ui.test.js, live-room-quiet-mode.test.js, profile-name-spotlight-copy.test.js.

Independent review found keyboard-focus, debounced-name, repeated-delete and direct-import issues; these were addressed before release. No production personal posts, replies, rewards or deletions are used for smoke testing. Local browser rendering was unavailable in the execution environment; actual page boot/interactions are checked with JSDOM and hosted page/asset/access checks after release.

Final validation: focused API archive tests 12 passed; focused UI tests 16 passed before final focus refinement. Full API suite: 287 passed, 1 skipped, 8 subtests and the documented baseline failure. Offline frontend suite: 309 passed with the four baseline failures and two live-catalog tests excluded. Parent reran real JavaScript controller against FastAPI TestClient: combined filters, lazy replies and author deletion passed in a temporary database. Full-page JSDOM boot, combined filters, snapshot renderer, date validation and zero-network browsing passed.
