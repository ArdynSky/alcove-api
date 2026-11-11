# Public wheel guest submission repair

The standalone public wheel is a name-only browser experience. Member authentication on its shared wheel-entry, review, stream-comment and reaction routes caused HTTP 401 rejections. The public page had no Telegram proof to supply.

## Changes

- `api/public_wheel.py`: dedicated public session and four submission routes. Opaque tokens are stored hashed, expire after 12 hours, and authorize only public wheel actions. Server-issued negative IDs cannot alias Telegram member IDs. Supplied member IDs, usernames and display names are ignored in favor of session identity.
- `api/main.py`: register the public router and aggregate/match wheel identities by canonical ID whenever IDs exist. Same-name guests cannot overwrite member usernames or consume another ID's entry allowance. Existing host round limits, pending moderation, muted-name rules and review/reaction handlers remain in use.
- `wheel-public.html` and `public-wheel-session.js`: name-only sessions, per-API/name storage, session refresh and one 401 retry. Entry, chat, review and reaction errors are shown; rejected drafts are retained and buttons recover. Public guests do not synchronize member progression.
- Verified Mini App routes remain protected; public tokens cannot authorize membership, Pulse, Daily Check-In, or the original protected submission routes.

## Validation and deploy review

- Seven API tests and twelve frontend tests pass. Initial tests reproduced missing public routes and identity collisions before fixes.
- Actual public-page HTML and JavaScript were executed against FastAPI TestClient: video submission, chat, review and reaction all succeeded without Telegram using the same server-issued guest identity. No production content was posted.
- Independent review checked scoped credentials, identity spoofing, same-name aggregation, failed drafts, expiry, retries and member-route isolation. The identity-collision finding was fixed and retested; no further critical issue was identified.
- Full API suite: 263 pass, 1 skipped, plus the existing `test_daily_checkin.py::DailyCheckInTests::test_feeling_and_message_are_both_required` fixture failure (missing EXP table after rejected inputs).
- Full frontend has four previously reproduced unrelated failures: `discussion-host-inbox.test.js`, `feature-admin-care-ui.test.js`, `live-room-quiet-mode.test.js`, `profile-name-spotlight-copy.test.js`.
- Blast radius: public wheel page and its submission/guest helper, public router, shared wheel identity matching/room-user aggregation. No activity schedule, OBS playback controls, verification state machine, membership rules or API app-state schema changes. Primary API source changes are synchronized to the second API.
- Deployment order: APIs first, public page/helper next. Check both live session endpoints, handler validation with empty/invalid content (no published test posts), member-route rejection, and published helper/page revision.
