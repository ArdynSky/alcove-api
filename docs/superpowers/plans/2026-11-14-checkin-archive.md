# Daily Check-In Archive Implementation Plan

> For agentic workers: follow test-driven development and independent review.

**Goal:** Browse and filter historical daily posts safely and efficiently in the existing Archive.
**Architecture:** Existing SQLite rows, indexed SQL and bounded keyset pagination; a new browser module integrated with archive.html and its styles.
**Tech Stack:** FastAPI, SQLite, plain JavaScript/HTML/CSS.
**Spec:** ../specs/2026-11-14-checkin-archive-design.md

## Constraints and review focus
Verified-only; exclude today; preserve snapshot art/replies; no new EXP/history copies; own deletion only; filter-safe cursors; no stale asynchronous response mutation; mobile parity and keyboard controls.

## Task 1 — API
- [ ] Add failing tests in tests/test_daily_checkin_archive.py for signed/verified access, past-day rollover, combined filters, keyset page ties/sorts, custom/invalid dates, stale or malformed cursors, historical replies/own deletion and absent EXP changes.
- [ ] Implement GET /api/daily-checkin/archive (mine, feeling, period=30d|7d|year|all|custom, date_from/date_to, name, replies=all|with|without|mine, sort=newest|oldest|az|za, cursor, limit=20/max50). Return status, posts, next_cursor, community_date. Reuse public check-in payloads; can_reply=false.
- [ ] Implement GET /api/daily-checkin/archive/{id}/replies with cursor/limit (return replies/next_cursor), DELETE /api/daily-checkin/archive/{id} for authenticated author, excluding today's/deleted posts. Preserve current routes.
- [ ] Run focused and full API tests and report existing failures by name.

## Task 2 — Archive UI
- [ ] Add failing offline UI tests for combined filter queries, stale responses, load-more/replies, deletion failures/drafts and access denial.
- [ ] Add assets/js/archive-checkin.js and integrate third section in archive.html, using existing styles/AlcoveLiveFeed renderer, controls and bounded APIs above.
- [ ] Include explicit local-only demo mode with sample posts/replies for visual tests; never send writes in demo mode.
- [ ] Run offline JS tests, then inspect desktop/mobile screenshots and interactions.

## Task 3 — Review, release
- [ ] Independently review API/UI and correct findings.
- [ ] Verify no anonymous archive access, no regression of today’s participation rules, retained history and no write calls during browsing.
- [ ] Publish API to primary and secondary repositories, then website. Verify deployments and assets; no production personal-content test writes.
