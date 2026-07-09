import datetime
import json
import os
import re
import uuid

FOX_MESSAGES_PATH = os.getenv(
    "FOX_MESSAGES_PATH",
    os.path.join(os.getcwd(), "fox_messages.json"),
)

UPLOADED_BANNER_PREFIX = "fox-banners/"
MAX_BANNER_BYTES = 5 * 1024 * 1024
ALLOWED_BANNER_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}

DEFAULT_LINK_WARNING_MESSAGES = [
    "Hey there {name}. Sorry (not sorry) but links aren't allowed here, so I had to remove it.",
    "Looks like your message included a hyperlink. I've removed it to keep The Alcove tidy.",
    "I noticed a link in your message {name}, but this chat keeps things link-free.",
    "Looks like your message included a URL {name}, so I removed it to follow The Alcove's rules.",
    "I saw that link you posted, but the rules here don't allow external links. But I'm sure you know that, right {name}?",
    "Nice try {name}, but I can't let that link stay in the chat.",
    "That URL looked ready to load, but the rules told me to stop it.",
    "I caught a hyperlink sneaking through your message {name}, so I quietly removed it.",
    "That link almost made it into the conversation, but I had to step in.",
    "{name}, your link tried to join the chat, but nothing gets past F.O.X.",
]

DEFAULT_ADMIN_WARN_TEMPLATES = {
    "1": "A gentle nudge from F.O.X: please keep things light and within the spirit of The Alcove.",
    "2": "Quick reminder: open conversation is welcome here, but please avoid turning the chat into sexual or drug-seeking talk.",
    "3": "Please pause and reset the tone a little. The Alcove works best when everyone keeps it safe, steady, and respectful.",
    "4": "F.O.X has noticed the chat drifting. Please keep things supportive rather than explicit or triggering.",
    "5": "This is a friendly reminder to keep the group comfortable for everyone. If you need support, ask for it directly and safely.",
    "6": "Please avoid using the group to arrange sex, chems, smoking, slamming, or similar activity. Keep it conversational and safe.",
}

DEFAULT_VERIFIED_WELCOME_MESSAGES = [
    "{name}\n\nWelcome to The Alcove. Settle in, say hi, and share the sticker you received from me.",
    "{name}\n\nThe door is open. Come in gently, say hello, and drop your welcome sticker when you are ready.",
    "{name}\n\nWelcome in. The room is better with you here. Say hi and show us the sticker you picked.",
    "{name}\n\nYou made it through. Welcome to The Alcove. Say hello, get comfortable, and share your sticker.",
    "{name}\n\nF.O.X has opened the door. Welcome to The Alcove. Come say hi and let the room meet you.",
    "{name}\n\nA new resident has arrived. Welcome to The Alcove. Say hi and share your sticker with us.",
]

FOX_VERIFICATION_DM_REFERENCE = [
    {
        "id": "verify_intro",
        "title": "Verification intro (private DM)",
        "step": "1 · Start",
        "summary": "First DM after someone opens verification. Includes the “Let’s do it” button.",
        "sample": (
            "VERIFICATION REQUIRED - F.O.X_Security_v1.012\n\n"
            "Hello, I am F.O.X - resident bot and your guide here at The Alcove. "
            "Before I open its doors fully, I just need to run you through a quick check."
        ),
    },
    {
        "id": "verify_ethos",
        "title": "Led by the heart (private DM)",
        "step": "2 · After intro button",
        "summary": "Ethos message plus hi sticker. Includes “Yes, I understand!” button.",
        "sample": "The Alcove is not just about horny chatter… If that sounds like your kind of space, we’d love to get to know you.",
    },
    {
        "id": "verify_captcha",
        "title": "Captcha check (private DM)",
        "step": "3 · After ethos confirm",
        "summary": "CHECK 1 intro before the visual captcha buttons.",
        "sample": "Perfect. We’re excited to get to know you… CHECK 1: Can you identify the real F.O.X among these imposters?",
    },
    {
        "id": "verify_reward",
        "title": "Welcome gift link (private DM)",
        "step": "Late · After captcha",
        "summary": "Reward sticker plus welcome gift / sticker-pack link. Includes “Got it” button.",
        "sample": "Here you go — your little welcome gift:\n\n[welcome topic / sticker pack link]",
    },
    {
        "id": "verify_resident",
        "title": "Resident confirmation (private DM)",
        "step": "Final · After gift",
        "summary": "Tells them they are verified and to say hello in General Chat.",
        "sample": "You are now officially a resident of The Alcove… Head back to the group and drop a message in General Chat…",
    },
]


def default_template_settings() -> dict:
    return {
        "link_warning": {
            "enabled": True,
            "banner": "assets/fox_caution.png",
            "messages": list(DEFAULT_LINK_WARNING_MESSAGES),
        },
        "verified_welcome": {
            "enabled": True,
            "banner": "assets/fox_welcome.png",
            "headline": "Welcome our newest resident. 👋",
            "body": "{name}\n\nSay hi and please share your sticker you received from me.",
            "messages": list(DEFAULT_VERIFIED_WELCOME_MESSAGES),
        },
        "join_verification": {
            "enabled": True,
            "banner": "assets/fox_verify.png",
            "request_text": (
                "<b>{display_name}</b>, thank you for requesting to join The Alcove.\n\n"
                "I have sent you a private message with the Mini App verification. "
                "Open it there, then I can open the doors for you."
            ),
            "direct_join_text": (
                "<b>{display_name}</b>, welcome to The Alcove.\n\n"
                "Before you can post freely, I need to get you verified. "
                "I have sent you a private message with the Mini App verification."
            ),
            "dm_failed_text": (
                "<b>{display_name}</b>, I saw your request to join The Alcove, "
                "but I could not send you a private Mini App verification message.\n\n"
                "Please open the bot and start verification here: {verify_link}"
            ),
        },
        "admin_public_warn": {
            "enabled": True,
            "banner": "assets/fox_reminder.png",
            "templates": dict(DEFAULT_ADMIN_WARN_TEMPLATES),
        },
        "group_app_launcher": {
            "enabled": True,
            "banner": "assets/pulse_test.png",
            "text": "Tap Open Alcove to launch the Mini App.",
            "link_url": "",
            "link_label": "Open Alcove Mini App",
        },
    }


FOX_TEMPLATE_EDITORS = [
    {
        "id": "link_warning",
        "title": "Link violation warning",
        "description": "Public banner when F.O.X removes a link. Use {name} for the member.",
        "fields": [
            {"key": "enabled", "type": "bool", "label": "Enabled"},
            {"key": "banner", "type": "banner", "label": "Banner image"},
            {"key": "messages", "type": "lines", "label": "Warning messages (one per line)"},
        ],
    },
    {
        "id": "verified_welcome",
        "title": "Verified arrival welcome",
        "description": "Welcome banner in the welcome topic after verification. Use {name} for @username or profile link.",
        "fields": [
            {"key": "enabled", "type": "bool", "label": "Enabled"},
            {"key": "banner", "type": "banner", "label": "Banner image"},
            {"key": "headline", "type": "text", "label": "Headline (used when variants below are empty)"},
            {"key": "body", "type": "textarea", "label": "Single body fallback (after headline)"},
            {"key": "messages", "type": "lines", "label": "Welcome variants (one per line — F.O.X picks one at random; use {name})"},
        ],
    },
    {
        "id": "join_verification",
        "title": "Join verification banner",
        "description": "Group banner when someone joins or requests access. Use {display_name} and {verify_link} where noted.",
        "fields": [
            {"key": "enabled", "type": "bool", "label": "Enabled"},
            {"key": "banner", "type": "banner", "label": "Banner image"},
            {"key": "request_text", "type": "textarea", "label": "Join request text"},
            {"key": "direct_join_text", "type": "textarea", "label": "Direct join text"},
            {"key": "dm_failed_text", "type": "textarea", "label": "DM failed fallback ({verify_link})"},
        ],
    },
    {
        "id": "admin_public_warn",
        "title": "Admin public warning (/warn public)",
        "description": "Templates 1–6 used by admins. Template ID is chosen in the warn command.",
        "fields": [
            {"key": "enabled", "type": "bool", "label": "Enabled"},
            {"key": "banner", "type": "banner", "label": "Banner image"},
            {"key": "templates", "type": "numbered", "label": "Warning templates", "count": 6},
        ],
    },
    {
        "id": "group_app_launcher",
        "title": "Mini App launcher banner",
        "description": "Singleton banner posted when F.O.X starts. Leave link URL blank to use the bot default Mini App link.",
        "fields": [
            {"key": "enabled", "type": "bool", "label": "Post on startup"},
            {"key": "banner", "type": "banner", "label": "Banner image"},
            {"key": "text", "type": "textarea", "label": "Caption text"},
            {"key": "link_url", "type": "text", "label": "Button link (optional override)"},
            {"key": "link_label", "type": "text", "label": "Button label"},
        ],
    },
]

AUDIT_LINE_RE = re.compile(
    r"\[(?P<logged_at>[^\]]+)\] action=(?P<action>\S+) chat_id=(?P<chat_id>-?\d+)"
    r"(?: message_id=(?P<message_id>\d+))?(?: reply_to=(?P<reply_to>\d+))?(?: note=(?P<note>.*))?$"
)

MEDIA_KIND_HINTS = {
    "fox_reminder.png": "self_care",
    "fox_caution.png": "link_warning",
    "fox_welcome.png": "verified_welcome",
    "fox_verify.png": "join_verification",
    "pulse_test.png": "group_app_launcher",
    "fox_banner.png": "group_app_launcher",
}

DEFAULT_SELF_CARE_MESSAGES = [
    "Water. Now.",
    "Stand up. Stretch.",
    "Your eyes need a break.",
    "Go outside for five minutes.",
    "Quick reset: look away from your screen for 30 seconds.",
]

KNOWN_BANNERS = [
    "assets/fox_banner.png",
    "assets/fox_reminder.png",
    "assets/fox_caution.png",
    "assets/fox_welcome.png",
    "assets/fox_verify.png",
    "assets/fox_denied.png",
    "assets/fox_access.png",
    "assets/fox_rules.png",
    "assets/pulse_test.png",
    "assets/spotlight_gold.png",
    "assets/spotlight_pink.png",
    "assets/spotlight_purple.png",
    "assets/spotlight_blue.png",
    "assets/spotlight_green.png",
]

FOX_MESSAGE_CATALOG = [
    {
        "id": "self_care",
        "title": "Self-care reminder",
        "category": "scheduled",
        "controllable": True,
        "target": "main_group",
        "topic_label": "Health topic",
        "schedule_label": "Random every 8–12 hours (when enabled)",
        "default_banner": "assets/fox_reminder.png",
        "content_source": "Rotating message pool (editable below)",
    },
    {
        "id": "daily_safety_digest",
        "title": "Daily safety digest",
        "category": "scheduled",
        "controllable": True,
        "target": "admin_group",
        "topic_label": "Safety reports topic",
        "schedule_label": "Once daily at chosen UTC hour",
        "default_banner": "",
        "content_source": "Auto-generated from F.O.X logs (see Safety Rules tab for hour toggle)",
    },
    {
        "id": "spotlight_auto_publish",
        "title": "Spotlight award publish",
        "category": "scheduled",
        "controllable": False,
        "target": "main_group",
        "topic_label": "Spotlight topic",
        "schedule_label": "When an approved spotlight is marked publish-pending",
        "default_banner": "assets/spotlight_*.png",
        "content_source": "Spotlight review workflow + API",
    },
    {
        "id": "group_app_launcher",
        "title": "Mini App launcher banner",
        "category": "startup",
        "controllable": True,
        "target": "main_group",
        "topic_label": "General",
        "schedule_label": "Once when F.O.X starts",
        "default_banner": "assets/pulse_test.png",
        "content_source": "Editable below (Event templates)",
    },
    {
        "id": "join_verification",
        "title": "Join verification banner",
        "category": "event",
        "controllable": True,
        "target": "main_group",
        "topic_label": "Welcome topic",
        "schedule_label": "When someone joins unverified",
        "default_banner": "assets/fox_verify.png",
        "content_source": "Editable below (Event templates)",
    },
    {
        "id": "verified_welcome",
        "title": "Verified arrival welcome",
        "category": "event",
        "controllable": True,
        "target": "main_group",
        "topic_label": "Welcome topic",
        "schedule_label": "After successful verification",
        "default_banner": "assets/fox_welcome.png",
        "content_source": "Editable below (Event templates)",
    },
    {
        "id": "link_warning",
        "title": "Link violation warning",
        "category": "event",
        "controllable": True,
        "target": "main_group",
        "topic_label": "Same thread as violation",
        "schedule_label": "When a link is removed",
        "default_banner": "assets/fox_caution.png",
        "content_source": "Editable below (Event templates)",
    },
    {
        "id": "admin_public_warn",
        "title": "Admin public warning",
        "category": "manual",
        "controllable": True,
        "target": "main_group",
        "topic_label": "Health topic",
        "schedule_label": "/warn @user public",
        "default_banner": "assets/fox_reminder.png",
        "content_source": "Editable below (Event templates)",
    },
]

def now_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


def normalize_templates(raw: dict | None = None) -> dict:
    defaults = default_template_settings()
    if not isinstance(raw, dict):
        return defaults
    merged = {}
    for template_id, default in defaults.items():
        incoming = raw.get(template_id) if isinstance(raw.get(template_id), dict) else {}
        entry = {**default, **incoming}
        entry["enabled"] = bool(entry.get("enabled", True))
        if template_id == "link_warning":
            messages = entry.get("messages")
            if not isinstance(messages, list) or not messages:
                entry["messages"] = list(default["messages"])
            else:
                entry["messages"] = [str(line).strip() for line in messages if str(line).strip()][:40]
            entry["banner"] = str(entry.get("banner") or default["banner"]).strip()
        elif template_id == "verified_welcome":
            messages = entry.get("messages")
            if not isinstance(messages, list) or not messages:
                entry["messages"] = list(default.get("messages") or DEFAULT_VERIFIED_WELCOME_MESSAGES)
            else:
                entry["messages"] = [str(line).strip() for line in messages if str(line).strip()][:40]
            entry["headline"] = str(entry.get("headline") or default["headline"]).strip()
            entry["body"] = str(entry.get("body") or default["body"]).strip()
            entry["banner"] = str(entry.get("banner") or default["banner"]).strip()
        elif template_id == "join_verification":
            for key in ("request_text", "direct_join_text", "dm_failed_text"):
                entry[key] = str(entry.get(key) or default[key]).strip()
            entry["banner"] = str(entry.get("banner") or default["banner"]).strip()
        elif template_id == "admin_public_warn":
            entry["banner"] = str(entry.get("banner") or default["banner"]).strip()
            templates = entry.get("templates") if isinstance(entry.get("templates"), dict) else {}
            entry["templates"] = {
                str(index): str(templates.get(str(index)) or default["templates"][str(index)]).strip()
                for index in range(1, 7)
            }
        elif template_id == "group_app_launcher":
            entry["text"] = str(entry.get("text") or default["text"]).strip()
            entry["link_url"] = str(entry.get("link_url") or "").strip()
            entry["link_label"] = str(entry.get("link_label") or default["link_label"]).strip()
            entry["banner"] = str(entry.get("banner") or default["banner"]).strip()
        merged[template_id] = entry
    return merged


def merge_template_update(current: dict, template_id: str, patch: dict) -> dict:
    templates = normalize_templates(current)
    if template_id not in templates or not isinstance(patch, dict):
        return templates
    merged_patch = {**templates[template_id], **patch}
    if template_id == "admin_public_warn" and isinstance(patch.get("templates"), dict):
        merged_patch["templates"] = {
            **templates[template_id].get("templates", {}),
            **{str(k): str(v).strip() for k, v in patch["templates"].items() if str(v).strip()},
        }
    templates[template_id] = normalize_templates({template_id: merged_patch})[template_id]
    return templates


def infer_message_kind(note: str | None) -> str | None:
    if not note:
        return None
    for fragment, kind in MEDIA_KIND_HINTS.items():
        if fragment in note:
            return kind
    return None


def parse_audit_line(line: str) -> dict | None:
    match = AUDIT_LINE_RE.match((line or "").strip())
    if not match:
        return None
    groups = match.groupdict()
    note = (groups.get("note") or "").strip() or None
    entry = {
        "logged_at": groups.get("logged_at"),
        "action": groups.get("action"),
        "chat_id": int(groups["chat_id"]) if groups.get("chat_id") else None,
        "message_id": int(groups["message_id"]) if groups.get("message_id") else None,
        "reply_to": int(groups["reply_to"]) if groups.get("reply_to") else None,
        "note": note,
        "kind": "audit",
        "message_kind": infer_message_kind(note),
    }
    return entry


def audit_dedupe_key(entry: dict) -> str:
    return "|".join(
        [
            str(entry.get("logged_at") or ""),
            str(entry.get("action") or ""),
            str(entry.get("chat_id") or ""),
            str(entry.get("message_id") or ""),
        ]
    )


def append_audit_log(state: dict, entries: list[dict]) -> int:
    if not entries:
        return 0
    log = state.setdefault("audit_log", [])
    seen = {audit_dedupe_key(row) for row in log}
    added = 0
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        normalized = {**entry, "kind": entry.get("kind") or "audit"}
        key = audit_dedupe_key(normalized)
        if key in seen:
            continue
        seen.add(key)
        log.append(normalized)
        added += 1
    state["audit_log"] = log[-500:]
    return added


def merged_recent_deliveries(state: dict, limit: int = 80) -> list[dict]:
    rows = []
    for entry in state.get("delivery_log") or []:
        rows.append({**entry, "source": "scheduled"})
    for entry in state.get("audit_log") or []:
        rows.append({**entry, "source": "fox_audit"})
    rows.sort(key=lambda row: str(row.get("logged_at") or ""), reverse=True)
    return rows[:limit]


def default_fox_messages_state() -> dict:
    return {
        "builtin_settings": {
            "self_care": {
                "enabled": True,
                "interval_min_hours": 8,
                "interval_max_hours": 12,
                "banner": "assets/fox_reminder.png",
                "messages": list(DEFAULT_SELF_CARE_MESSAGES),
            },
            "templates": default_template_settings(),
        },
        "scheduled_posts": [],
        "delivery_log": [],
        "audit_log": [],
        "custom_banners": [],
    }


def _coerce_bounded_int(value, default: int, minimum: int, maximum: int) -> int:
    try:
        if value is None or value == "":
            raise ValueError
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def _field_int(raw: dict, base: dict, key: str, default: int, minimum: int, maximum: int) -> int:
    value = raw.get(key)
    if value is None or value == "":
        value = base.get(key, default)
    return _coerce_bounded_int(value, default, minimum, maximum)


def normalize_scheduled_post(raw: dict, existing: dict | None = None) -> dict:
    base = existing or {}
    post_id = str(raw.get("id") or base.get("id") or uuid.uuid4().hex[:12])
    schedule_type = str(raw.get("schedule_type") or base.get("schedule_type") or "daily").strip().lower()
    if schedule_type not in {"daily", "interval_hours", "once", "weekly"}:
        schedule_type = "daily"
    target = str(raw.get("target") or base.get("target") or "main_group").strip().lower()
    if target not in {"main_group", "admin_group"}:
        target = "main_group"
    return {
        "id": post_id,
        "title": str(raw.get("title") or base.get("title") or "Untitled F.O.X post").strip()[:120],
        "enabled": bool(raw.get("enabled", base.get("enabled", True))),
        "schedule_type": schedule_type,
        "hour_utc": _field_int(raw, base, "hour_utc", 12, 0, 23),
        "minute_utc": _field_int(raw, base, "minute_utc", 0, 0, 59),
        "interval_hours": _field_int(raw, base, "interval_hours", 12, 1, 168),
        "weekday_utc": _field_int(raw, base, "weekday_utc", 0, 0, 6),
        "run_at": str(raw.get("run_at") or base.get("run_at") or "").strip(),
        "target": target,
        "topic_id": raw.get("topic_id", base.get("topic_id")),
        "text": str(raw.get("text") or base.get("text") or "").strip(),
        "banner": str(raw.get("banner") or base.get("banner") or "").strip(),
        "link_url": str(raw.get("link_url") or base.get("link_url") or "").strip(),
        "link_label": str(raw.get("link_label") or base.get("link_label") or "Open link").strip()[:60],
        "replace_singleton": str(raw.get("replace_singleton") or base.get("replace_singleton") or "").strip()[:40],
        "last_posted_at": base.get("last_posted_at"),
        "next_run_at": base.get("next_run_at"),
        "created_at": base.get("created_at") or now_iso(),
        "updated_at": now_iso(),
        "send_now": bool(raw.get("send_now", False)),
    }


def load_fox_messages_state() -> dict:
    if not os.path.exists(FOX_MESSAGES_PATH):
        return default_fox_messages_state()
    try:
        with open(FOX_MESSAGES_PATH, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return default_fox_messages_state()
    if not isinstance(raw, dict):
        return default_fox_messages_state()
    state = default_fox_messages_state()
    if isinstance(raw.get("builtin_settings"), dict):
        state["builtin_settings"].update(raw["builtin_settings"])
        sc = state["builtin_settings"].setdefault("self_care", {})
        if not isinstance(sc.get("messages"), list) or not sc["messages"]:
            sc["messages"] = list(DEFAULT_SELF_CARE_MESSAGES)
        state["builtin_settings"]["templates"] = normalize_templates(
            state["builtin_settings"].get("templates")
        )
    if isinstance(raw.get("scheduled_posts"), list):
        state["scheduled_posts"] = [
            normalize_scheduled_post(entry) for entry in raw["scheduled_posts"] if isinstance(entry, dict)
        ]
    if isinstance(raw.get("delivery_log"), list):
        state["delivery_log"] = raw["delivery_log"][-200:]
    if isinstance(raw.get("audit_log"), list):
        state["audit_log"] = raw["audit_log"][-500:]
    if isinstance(raw.get("custom_banners"), list):
        state["custom_banners"] = [
            entry for entry in raw["custom_banners"] if isinstance(entry, dict) and entry.get("path")
        ]
    return state


def save_fox_messages_state(state: dict) -> dict:
    directory = os.path.dirname(FOX_MESSAGES_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(FOX_MESSAGES_PATH, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
    return state


def append_delivery_log(state: dict, entry: dict) -> None:
    log = state.setdefault("delivery_log", [])
    log.append({**entry, "logged_at": now_iso()})
    state["delivery_log"] = log[-200:]


def compute_next_run(post: dict, after: datetime.datetime | None = None) -> str | None:
    after = after or datetime.datetime.utcnow()
    schedule_type = post.get("schedule_type")
    if schedule_type == "once":
        run_at = post.get("run_at")
        if not run_at:
            return None
        try:
            run_time = datetime.datetime.fromisoformat(run_at.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None
        if run_time <= after and post.get("last_posted_at"):
            return None
        return run_time.isoformat()
    if schedule_type == "interval_hours":
        hours = int(post.get("interval_hours") or 12)
        if post.get("last_posted_at"):
            try:
                last = datetime.datetime.fromisoformat(str(post["last_posted_at"]).replace("Z", "+00:00")).replace(tzinfo=None)
                return (last + datetime.timedelta(hours=hours)).isoformat()
            except ValueError:
                pass
        return after.isoformat()
    if schedule_type == "weekly":
        hour = int(post.get("hour_utc") or 12)
        minute = int(post.get("minute_utc") or 0)
        weekday = int(post.get("weekday_utc") or 0)
        days_ahead = (weekday - after.weekday()) % 7
        candidate = after.replace(hour=hour, minute=minute, second=0, microsecond=0) + datetime.timedelta(days=days_ahead)
        if candidate <= after:
            candidate += datetime.timedelta(days=7)
        return candidate.isoformat()
    hour = int(post.get("hour_utc") or 12)
    minute = int(post.get("minute_utc") or 0)
    candidate = after.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= after:
        candidate += datetime.timedelta(days=1)
    return candidate.isoformat()


def posts_due_now(state: dict) -> list[dict]:
    now = datetime.datetime.utcnow()
    due = []
    for post in state.get("scheduled_posts") or []:
        if not post.get("enabled"):
            continue
        if post.get("send_now"):
            due.append({**post, "send_now": True})
            continue
        next_run = post.get("next_run_at")
        if not next_run:
            next_run = compute_next_run(post, now)
            post["next_run_at"] = next_run
        if not next_run:
            continue
        try:
            run_time = datetime.datetime.fromisoformat(str(next_run).replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            continue
        if run_time <= now:
            due.append(post)
    return due


def mark_post_sent(state: dict, post_id: str, *, message_id=None, chat_id=None, error: str | None = None) -> None:
    for post in state.get("scheduled_posts") or []:
        if post.get("id") == post_id:
            post.pop("send_now", None)
            break
    if error:
        append_delivery_log(
            state,
            {
                "post_id": post_id,
                "message_id": message_id,
                "chat_id": chat_id,
                "error": error,
                "kind": "scheduled_post",
            },
        )
        return
    now = now_iso()
    for post in state.get("scheduled_posts") or []:
        if post.get("id") != post_id:
            continue
        post["last_posted_at"] = now
        post["next_run_at"] = compute_next_run(post)
        if post.get("schedule_type") == "once":
            post["enabled"] = False
        break
    append_delivery_log(
        state,
        {
            "post_id": post_id,
            "message_id": message_id,
            "chat_id": chat_id,
            "error": error,
            "kind": "scheduled_post",
        },
    )


def fox_banners_dir() -> str:
    default_dir = os.path.join(os.path.dirname(os.path.abspath(FOX_MESSAGES_PATH)), "fox-banners")
    directory = os.getenv("FOX_BANNERS_DIR", default_dir)
    os.makedirs(directory, exist_ok=True)
    return directory


def sanitize_banner_stem(name: str) -> str:
    stem = os.path.splitext(os.path.basename(name or "banner"))[0].lower()
    stem = re.sub(r"[^a-z0-9_-]+", "-", stem).strip("-_")
    return stem[:48] or "banner"


def normalize_banner_filename(filename: str) -> str:
    base = os.path.basename(str(filename or "").strip())
    if not base or base in {".", ".."} or "/" in base or "\\" in base:
        raise ValueError("Invalid banner filename")
    ext = os.path.splitext(base)[1].lower()
    if ext not in ALLOWED_BANNER_EXTENSIONS:
        raise ValueError("Banner must be PNG, JPEG, WebP, or GIF")
    return base


def list_banner_paths(state: dict) -> list[str]:
    custom = []
    for entry in state.get("custom_banners") or []:
        path = str(entry.get("path") or "").strip()
        if path:
            custom.append(path)
    return list(KNOWN_BANNERS) + custom


def banner_manifest(state: dict) -> list[dict]:
    manifest = []
    for entry in state.get("custom_banners") or []:
        filename = str(entry.get("filename") or "").strip()
        if not filename:
            continue
        file_path = os.path.join(fox_banners_dir(), filename)
        if not os.path.isfile(file_path):
            continue
        manifest.append(
            {
                "id": entry.get("id"),
                "path": entry.get("path"),
                "filename": filename,
                "label": entry.get("label") or filename,
                "size_bytes": os.path.getsize(file_path),
            }
        )
    return manifest


def resolve_uploaded_banner_file(filename: str) -> str:
    safe_name = normalize_banner_filename(filename)
    file_path = os.path.join(fox_banners_dir(), safe_name)
    if not os.path.isfile(file_path):
        raise FileNotFoundError(safe_name)
    return file_path


def save_banner_upload(content: bytes, original_name: str, label: str, state: dict) -> dict:
    if not content:
        raise ValueError("Empty file")
    if len(content) > MAX_BANNER_BYTES:
        raise ValueError("Banner file too large (max 5 MB)")
    ext = os.path.splitext(original_name or "")[1].lower()
    if ext not in ALLOWED_BANNER_EXTENSIONS:
        raise ValueError("Banner must be PNG, JPEG, WebP, or GIF")
    stem = sanitize_banner_stem(original_name)
    filename = f"{stem}-{uuid.uuid4().hex[:8]}{ext}"
    file_path = os.path.join(fox_banners_dir(), filename)
    with open(file_path, "wb") as handle:
        handle.write(content)
    entry = {
        "id": uuid.uuid4().hex[:12],
        "path": f"{UPLOADED_BANNER_PREFIX}{filename}",
        "filename": filename,
        "label": str(label or stem).strip()[:80] or filename,
        "uploaded_at": now_iso(),
        "size_bytes": len(content),
    }
    banners = state.setdefault("custom_banners", [])
    banners.append(entry)
    state["custom_banners"] = banners[-100:]
    return entry


def delete_custom_banner(state: dict, banner_id: str) -> bool:
    banners = state.get("custom_banners") or []
    kept = []
    deleted = False
    for entry in banners:
        if entry.get("id") == banner_id:
            deleted = True
            filename = str(entry.get("filename") or "").strip()
            if filename:
                try:
                    os.remove(os.path.join(fox_banners_dir(), normalize_banner_filename(filename)))
                except OSError:
                    pass
            continue
        kept.append(entry)
    if deleted:
        state["custom_banners"] = kept
    return deleted


def admin_payload(state: dict) -> dict:
    posts = []
    for post in state.get("scheduled_posts") or []:
        enriched = dict(post)
        enriched["next_run_at"] = enriched.get("next_run_at") or compute_next_run(enriched)
        posts.append(enriched)
    builtin = state.get("builtin_settings") or {}
    builtin = {
        **builtin,
        "templates": normalize_templates(builtin.get("templates")),
    }
    return {
        "catalog": FOX_MESSAGE_CATALOG,
        "verification_dm_reference": FOX_VERIFICATION_DM_REFERENCE,
        "banners": list_banner_paths(state),
        "custom_banners": state.get("custom_banners") or [],
        "template_editors": FOX_TEMPLATE_EDITORS,
        "builtin_settings": builtin,
        "scheduled_posts": posts,
        "delivery_log": list(reversed(state.get("delivery_log") or [])),
        "audit_log": list(reversed(state.get("audit_log") or [])),
        "recent_deliveries": merged_recent_deliveries(state),
    }


def upsert_scheduled_post(state: dict, raw: dict, post_id: str | None = None) -> dict:
    posts = state.setdefault("scheduled_posts", [])
    if post_id:
        for index, existing in enumerate(posts):
            if existing.get("id") == post_id:
                posts[index] = normalize_scheduled_post(raw, existing)
                posts[index]["next_run_at"] = compute_next_run(posts[index])
                return posts[index]
        raise KeyError(post_id)
    post = normalize_scheduled_post(raw)
    post["next_run_at"] = compute_next_run(post)
    posts.append(post)
    return post


def delete_scheduled_post(state: dict, post_id: str) -> bool:
    posts = state.get("scheduled_posts") or []
    filtered = [post for post in posts if post.get("id") != post_id]
    if len(filtered) == len(posts):
        return False
    state["scheduled_posts"] = filtered
    return True


def queue_test_send(state: dict, post_id: str) -> dict | None:
    for post in state.get("scheduled_posts") or []:
        if post.get("id") == post_id:
            post["send_now"] = True
            return post
    return None
