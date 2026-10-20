# Live question games implementation plan

> For agentic workers: use superpowers:executing-plans. Implement the accepted chat design without altering unrelated games.

Goal: reusable 1–20-round Match the Crowd and Quiz, individual/pairs/triples, host reveals, compact chat and final prizes.
Architecture: separate question game engine and HTTP adapter backed by SQLite beside the existing team game data; small frontend modules reuse activity-stage and Live Feed rendering. No new Render service.
Tech stack: existing FastAPI/Python SQLite and plain browser JavaScript/CSS.
Spec: accepted conversation, consolidated below.

## Constraints
- Default eight questions; 1–20 configurable. Quiz aliases use semicolons.
- Match scoring 1 ordinary match, 2 most popular, tied most popular together, unique 0.
- Quiz correct 1, incorrect 0. Host reviews before reveal.
- Team captain submits once; fixed teams, authenticated private chat, replacement captain.
- Existing video backgrounds, typography, collapse controls, 16:9 customised message cards.
- Two columns with centred odd final card. Current group only, neutral then green winners.
- Timer in OBS only. Persistent OBS leaderboard.
- Prizes at final results only, protected from retries. Preserve current collection-complete behavior unless host explicitly chooses repeat participation.
- No deployment or main-branch changes during implementation.

## Review focus
- Input remains stable during background polling and reconnection.
- Wrong-round or repeat actions cannot score or submit twice.
- Neither accepted answers nor unrevealed player answers reach public clients.
- Team chat requires verified identity rather than a claimed user ID.
- Reward retries across databases cannot duplicate grants.

## Tasks
- [x] Isolate branches and establish baseline; use GitHub Actions because local execution is unavailable.
- [x] Commit failing behavioral checks before implementation.
- [ ] Engine: create_game(config), begin_round(state), submit_answer(state,uid,text), review_answers(state,corrections,merges), reveal_next(state), public_state(state,uid). Verify pytest scoring/privacy/deadlines/persistence.
- [ ] HTTP adapter: host auth, participant auth, state revisions, templates, team chat, atomic final grants. Verify route authentication, chat isolation and retry tests.
- [ ] Shared frontend renderer and single state poller. Verify browser inputs, cards, role-specific controls and text escaping.
- [ ] Host question editor and reveal panel. Preserve other host handlers and background media.
- [ ] Member activity, polished chat dialog, OBS answer/leaderboard and existing timer extension.
- [ ] Run complete backend/frontend suites; record pre-existing failures separately; inspect in browser at mobile and OBS dimensions.
- [ ] Review both pull requests and provide validation evidence before release.

## Ledger
- 2026-10-20: backend baseline 147 passed, 1 skipped; frontend baseline five test files fail before changes.
- Ruling: use remote isolated branches and CI instead of unavailable local runtime.
- Ruling: unresolved collection fallback remains existing behavior; optional host repeat toggle makes additional awards explicit.
- Ruling: odd pair count allows one team of three rather than leaving a member alone; preview teams before starting.
