# Verification admission repair — 11 November 2026

## Required behavior

A new person requests to join, opens F.O.X's signed verification button, completes the ethos, CAPTCHA (or FOX backup), feedback and form vote, and requests Telegram admission if needed. Only confirmed admission after those server-recorded checks grants new app membership. A genuine returning member does not repeat onboarding. Explicit admin approval remains an audited admin override.

## Implementation and blast radius

- `api/verification_flow.py`: identity-bound SQLite sessions, ordered atomic checkpoints, two-hour expiry, idempotent retries and trusted final answers. Starting verification does not enqueue approval or grant access.
- `api/main.py`: signed start/step routes, session-required final submit, session generation IDs and compare-and-set worker updates; pending queue excludes obsolete flows. Completed retries preserve admission timestamps. Revocation invalidates sessions and cannot be undone by stale callbacks.
- `api/member_access.py`: primary API is the membership authority; the secondary API delegates decisions to it and fails closed when unavailable. Tester preview status does not grant unverified app access. Historical registry/completed flags alone no longer grant access; current F.O.X-confirmed members, admitted protocol sessions and explicit owner recoveries remain supported. Revocation timestamps cannot move backward. Explicit full bot snapshots support empty membership sets and reject older snapshots.
- Bot `captcha.py`/`bot_sync.py`: signed `/start` and `/verify`, authoritative verified-users table snapshots, per-user admission locks, fresh worker preflight, session updates and confirmed Telegram participation. Failed admission never creates new verified membership. Direct unverified joins are removed. Obsolete verification callbacks cannot grant access. Revocation is propagated before local deletion; uncertain admission rollback is persisted and retried conditionally for that exact session.
- Bot `admission_policy.py`/`main.py`: inspect privacy using the Bot API; secure or create and persist a managed approval-required invite, preserve old URLs, and report policy privately to the API. Bots cannot enumerate or change every other administrator's invite. Public content visibility and owner-only invite review are reported rather than described as secured.
- Frontend `verify.html`, `verification-session.js`, `index.html`: acknowledge each server checkpoint before advancing; retain failed inputs; use fresh Telegram data and trusted final session answers. Include the secured Request to Join action before final admission. Do not close via `sendData` or offer the private welcome topic before approval. Refresh verification entry/helper versions.
- Consumers affected: onboarding, membership gates, protected member API requests and welcome-pack eligibility. Existing profile progress/reward claims are preserved. Host activities, schedules, OBS overlays and app-state schema are unchanged.

## Validation

- Tests were observed failing before fixes, including unearned completion/tester access, stale generation updates, backward revocation timestamps and refreshed terminal admission evidence.
- API focused verification/access suite: 35 pass. Protocol tests cover signed identity binding, skipped steps, wrong answers, expiry, persistence, duplicate actions and revocation.
- Full API suite: one existing failure in `test_daily_checkin.py::DailyCheckInTests::test_feeling_and_message_are_both_required` (test counts a table that is not created after rejected inputs). Reproduced against unchanged production API files in a separate baseline directory. One existing skipped test remains.
- Full bot suite: 88 pass, plus 6 subtests. Includes actual per-user lock interleaving, stale preflight rejection, generation CAS, rollback persistence, managed invitation behavior and database-error fail-closed snapshots.
- Full frontend suite: 277 pass, four existing unrelated failures (`discussion-host-inbox.test.js`, `feature-admin-care-ui.test.js`, `live-room-quiet-mode.test.js`, `profile-name-spotlight-copy.test.js`). Earlier baseline runs reproduced these failures with unchanged production files.
- Actual verification HTML and helper executed in a DOM harness connected to FastAPI TestClient using locally generated Telegram HMAC proof: all required actions persisted; access denied before and during pending approval; access allowed after the authenticated bot admission acknowledgment. No real account or production post was used.
- Independent review found and verified fixes for stale polling after revocation, reused queue IDs, backward revocation tombstones, mutable completed timestamps and missing join-request recovery. No remaining critical issue was identified in the final focused review.

## Deployment and practical limits

Deploy API routes first, then F.O.X and the synchronized secondary API, then the frontend. Confirm all Render revisions are live, the public helper/HTML match, unsigned and obsolete submissions fail, and the admission-policy report describes actual Telegram settings. Do not describe mocked Telegram membership as a real new-user end-to-end test. Telegram bots cannot inspect every invitation created by another administrator; an owner must keep those links approval-only. The invitation title/Request to Join screen remains visible before admission.
