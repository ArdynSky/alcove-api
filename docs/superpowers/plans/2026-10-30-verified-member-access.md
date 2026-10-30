# Verified member access

User requested tighter access checks and confirmed Telegram ID 919308569 previously verified and claimed the Dancer pack.

1. Add regression tests: stale cached tester cannot authorize; missing/invalid signed identity cannot authorize; completed verification survives a missing bot-sync row; identity IDs cannot be substituted; previous pack claims do not generate a second welcome pack.
2. Add durable verified-member records derived only from authenticated bot sync, completed verification, or private owner-confirmed recovery configuration. Preserve original verification dates when known; never trust a client pack selection or progress as proof of verification. Restore the confirmed account through private runtime configuration, not a public code exception.
3. Add signed membership access endpoint and enforce verified membership on member data/actions. Retain authenticated bot/admin and intentional owner/tester access. Preserve verification/signup, assets, public settings and OBS/host routes.
4. Update shared client access to ask the server, attach signed Telegram data only to the configured Alcove APIs, recheck on resume/periodically, and hide protected pages until authorized. Persist no unsigned authorization.
5. Run backend/frontend suites, review, publish frontend signed-request transport before strict backend rollout, then publish the signed gate; enforcement is never disabled, verify live account membership and pack claim state. Never reset progress or reissue a claimed pack.

Limits: this change hardens member access; it does not redesign host/OBS authorization. Fail closed on authentication or membership-check failures.
