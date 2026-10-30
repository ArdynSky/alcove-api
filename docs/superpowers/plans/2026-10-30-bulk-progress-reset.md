# Bulk Progress Reset Implementation Plan

> Execute inline using superpowers:executing-plans. User approved implementation and deployment of the agreed reset design.

**Goal:** Admin bulk reset with saved Telegram ID exclusions; only Pulse and Spotlight approvals after reset contribute to fresh progress.

**Architecture:** Existing FastAPI/SQLite profile store holds atomic reset records and one-use previews. Keep submitted content and moderation queues intact. Snapshot previously approved IDs to prevent historical rewards returning even after re-review. Frontend uses approval timestamps and server eligibility, and adopts remote resets before uploading stale profiles.

**Constraints:** Never execute a production reset during deployment. Excluded accounts retain all progress. Reset to Level 1/0 EXP, starter items, no earned rewards or achievements; preserve membership date. Pending submissions remain eligible on future approval. No infrastructure changes.

## Task 1: Backend
- [ ] Add failing tests for admin auth, preview without mutations, case-insensitive exclusions, missing usernames, atomic execution/retry, local-only accounts, stale replace, approval boundary and old approved submissions.
- [ ] Add api/progress_reset.py with preview/config/execute endpoints and approval metadata; integrate member_progress and main payloads.
- [ ] Run new tests and existing member/review tests.

## Task 2: Frontend
- [ ] Add failing behavioural tests for Pulse achievement increments, Spotlight approval-only recovery, old approved exclusions and reset adoption.
- [ ] Add bulk admin UI with saved exclusions, member list preview and typed confirmation; update progression and profile sync.
- [ ] Verify JS tests and admin flow with mocked API; inspect production source configuration.

## Task 3: Review and deploy
- [ ] Review changes, run backend suite and frontend checks, commit with expected-head protection.
- [ ] Merge to linked production repositories, observe automatic deployments and verify public health/source.

## Review Focus
Unknown exclusions must fail closed; stale clients must not revive progress; late publication of old approvals must remain excluded; duplicate approvals must grant once; retrying execute must not reset twice.
