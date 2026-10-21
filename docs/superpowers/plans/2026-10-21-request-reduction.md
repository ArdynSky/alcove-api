# Live Room and bot request reduction implementation plan

> For agentic workers: use executing-plans or subagent-driven-development for these independent tasks.

**Goal:** Stop closed-room activity polling, reduce unchanged downloads, and prevent repeated bot work.

**Architecture:** Public room lifecycle invalidations use a small WebSocket tied to active agendas. Private state remains in existing HTTP endpoints, cached per full URL and identity; local timers keep repainting. Care followups ride the existing admin-job bundle with durable per-resolution receipts; daily reports use durable SQLite claims and stable Telegram retry IDs.

**Tech stack:** Existing FastAPI, SQLite, browser WebSocket/fetch, Telethon; no new production dependencies.

**Spec:** User instruction on 2026-10-21: substantially reduce data and requests, no Live Room activity requests when closed, consolidate obsolete duplicates while retaining correct current behavior.

## Global constraints

- Live Room openness means an active agenda, independently of Wheel room_open.
- Do not include answers, usernames or admin secrets in push frames.
- Preserve reveal timing, cosmetics, manual controls and standalone Wheel sources.
- Healthy push requires no idle activity HTTP; disconnected sources may perform gate-only recovery checks.
- No Telegram test sends; tests use isolated databases and fake clients.

## Review focus

- Closed-to-open and open-to-closed transitions, including pending reads.
- Two member identities must never share personalised cached state.
- Timers/results must progress locally and refresh at deadline.
- Lost acknowledgements, changed resolutions and permanent delete failures.
- Restart/concurrent daily schedulers and the send/receipt crash gap.

## Tasks

- [x] Add public lifecycle bus and initial/transition/disconnect/privacy tests. Integrate installer in api/main.py.
- [x] Add scoped frontend transport, initial closed gating, full-identity caches, deadline expiry and invalidation. Retire duplicate boot read and unsafe old cache.
- [x] Bundle pending care followups, acknowledge durable results/backoff, remove three historical-list GETs and duplicate sync loop.
- [x] Persist daily report claim/receipt and stable Telegram retry ID; preserve manual report command.
- [ ] Run full repository suites, actual browser request-count/entry/exit tests, and independent review.
- [ ] Merge backend first, then bot/frontend; verify deployed versions, closed-room request counters and bot log behavior.
