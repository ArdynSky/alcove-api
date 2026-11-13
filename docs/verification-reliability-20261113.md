# Verification delivery and welcome-gift repair

The owner approved tightening the existing join-request flow, rather than replacing it, and auditing the four most recent verified members' welcome packs.

## Design and scope

Keep signed Telegram verification and server-enforced membership. Preserve the public wheel's separate guest access. Improve delivery retries, durable deduplication, failure alerts, and auditability within the current Telethon architecture. Do not add a competing Bot API update consumer or manufacture temporary user_chat_id values.

Restore a previously claimed verification gift after progression reset from authoritative retained membership and its selected catalog pack. Add cosmetics and missing artwork metadata only; keep EXP, levels, statistics, reset checkpoints, unrelated rewards and claim receipts unchanged. Never mark a claimed gift eligible for another award. Restore current published gift contents; historical catalog snapshots are unavailable.

Alternatives considered: replacing invitations with verification-first links would not explain existing failures and is deferred; reopening claims after resets would repeat rewards and is rejected. Silent entitlement restoration preserves once-only reward semantics.

## Implementation and checks

- Bot: preserve cached DM peers; retry explicit 429/5xx rejections within a bounded wait budget; respect retry_after; record ambiguous delivery without blindly resending; persist accepted/unknown request receipts; retry admin failure alerts; retain audit history for 30 days/20,000 events and prune completed alerts.
- API: repair trusted claimed-gift ownership on profile reads/saves, including missing profiles and rejected stale reset uploads. Persist repair without minting receipts, EXP, unlock notifications or pending gifts.
- Profile: restore trusted already-claimed gift cosmetics silently, retry transient eligibility failures twice, reject stale identity/reset responses and refresh after owner reset.
- Test regressions before changes; run local delivery, profile and restoration checks; review independently; publish bot/API and frontend; verify deployment and anonymous membership denial.

## Live audit limitation

Render logs confirm completed verifications and welcome posts for user IDs 7161592842 (2026-11-11 20:40:57 UTC), 2058605662 (2026-11-13 06:02:19), 7917361976 (06:06:20), and 880974842 (06:19:56). This is evidence of verification/admission, not pack claims. Protected member profiles, selected packs and receipts require authenticated diagnostic access. The browser sign-in did not complete; no live claim/content mutation or manual approval was performed.

## Validation limitations

The existing API suite has a baseline failure in tests/test_daily_checkin.py::DailyCheckInTests::test_feeling_and_message_are_both_required. The frontend offline suite excludes two legacy tests which shell out to fetch the live reward catalog (reward-claim-heal.test.js and stamp-reward-label.test.js). Four existing frontend failures remain: discussion-host-inbox.test.js, feature-admin-care-ui.test.js, live-room-quiet-mode.test.js, profile-name-spotlight-copy.test.js. Targeted repair tests are verified separately.

Telegram acceptance of a message is not proof it was seen. Exactly-once remote delivery cannot be guaranteed across process crashes or lost responses. Temporary join-request DM addressing is deferred until a single supported Bot API update owner can be established.

## Final checks

Bot full suite: 106 passed, 6 subtests. Frontend offline run: 293 passed, four existing failures; six focused repair tests pass. API focused restoration/eligibility/access checks pass; full suite retains the documented daily-checkin fixture failure. Independent review found a mixed pending-choice entitlement issue, reproduced it, and the fix pins already-claimed eligibility to authoritative retained fox_form.
