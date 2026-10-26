# Resource cleanup implementation plan

Goal: reduce avoidable memory retention and repeat public image transfers without changing member flows, data retention, upload limits, polling timings, media quality, hosting plans or database schema.

Approach: targeted resource management on an isolated branch. Defer architectural changes and global edge caching because they require broader testing. The user authorised conservative optimisation of the recommended actions.

1. Reproduce delayed SQLite cleanup with live connections kept reachable. Explicitly close the scoped connections in main.py and home_alerts.py after their existing transaction contexts exit. Verify saved state and alert creation/read/consume still work.
2. Bound the existing reward, banner and homepage upload reads to the existing size limit plus one byte. Preserve the same accepted file sizes and rejection messages. Test each oversized route and existing successful uploads.
3. Give public reward image responses a one-hour cache lifetime. Their upload filenames are unique; leave authenticated routes and dynamic data untouched. Verify response bytes, content type, ETag, range serving and missing-file behaviour.
4. Run the full existing test suite, review the complete diff, and preserve a reviewable GitHub pull request. Do not claim lower live memory until the change is deployed and measured. Do not run a load test against production.

Baseline: 180 passed, 1 skipped; existing deprecation warnings. Reproduction tests added before product edits.
