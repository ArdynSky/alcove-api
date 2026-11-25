# Rehearsal fake audience

Closed-room rehearsals have no real members, so Host Control / OBS stay empty unless a fake crowd joins.

## Tonight / ops

```bash
export BOT_SYNC_SECRET='…'   # same value as Feature Admin
python3 tools/rehearsal_audience.py --register-members
```

That script:

1. Optionally registers IDs `91001–91006` on the membership roster (needed for Herd Mentality / Quiz until the API bypass ships).
2. Watches the rehearsal agenda and auto-joins / answers:
   - Debate volunteers + audience thoughts
   - Herd Mentality / Quiz answers when a question is open
   - Drawing pool joins
   - Discussion questions / comments
   - Poll votes

Leave it running in a terminal while you rehearse. Use `--once` for a single tick.

## After deploy

`api/rehearsal_dummies.py` lets bot-secret callers drive those IDs **only while an agenda is `rehearsal`**, and pack grants skip them. `--register-members` then becomes optional.
