from __future__ import annotations

import datetime as dt
import hashlib
import os
import re
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Literal, Optional

from fastapi import APIRouter, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, model_validator

router = APIRouter(prefix="/api", tags=["help"])
_LOCK = threading.RLock()
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
VIDEO_EXTENSIONS = {".mp4"}
IMAGE_MAX_BYTES = 5 * 1024 * 1024
VIDEO_MAX_BYTES = 100 * 1024 * 1024

LEGACY_ITEMS = (
    ("legacy-rules", "Group Rules", "GROUP RULES", "🔵 ┃ Group Rules", "rules", "assets/help_rules_banner.png"),
    ("legacy-ardyn", "Message from Ardyn", "MESSAGE FROM ARDYN", "🟣 ┃ Message from Ardyn", "ardyn", "assets/help_ardyn_banner.png"),
    ("legacy-guide", "Mini App Guide", "MINI APP GUIDE", "🟢 ┃ Mini App Guide", "guide", "assets/help_guide_banner.png"),
    ("legacy-faq", "F.A.Q.'s", "F.A.Q.'S", "🟠 ┃ F.A.Q.'s", "faq", "assets/help_faq_banner.png"),
)
APP_ROOTS = ("LIVE ROOM", "PROFILE", "CONNECT", "ARCHIVE")

APP_HELP_CONTENT_MIGRATION = "app-help-content-v1"
APP_HELP_CONTENT = (
    {
        "id": "help-live-room-lobby", "parent_id": "app-live-room", "title": "LOBBY",
        "subtitle": "Enter the Live Room, check the schedule and join the conversation.", "theme": "#38a9e8",
        "tutorials": (
            ("help-lobby-overview", "WHAT IS THE LIVE ROOM?", "What is the Live Room?", "The Live Room is The Alcove's space for scheduled video sessions, interactive activities and live community conversation. You can see what is happening now, check what is coming next and choose how you would like to take part."),
            ("help-lobby-schedule", "VIEW THE SCHEDULE", "View the Schedule", "Open **View Schedule** to see upcoming sessions and the current activity. Completed activities may be marked as finished so you can quickly find what is happening next."),
            ("help-lobby-join", "JOIN A LIVE SESSION", "Join a Live Session", "Open the Live Room from the homepage and follow the on-screen join controls. Camera and microphone permissions are controlled by your device, and participating on camera is always your choice."),
            ("help-lobby-feed", "USE THE LIVE FEED", "Use the Live Feed", "Use the Live Feed to follow the room and contribute alongside the current activity. Keep messages relevant, give others space to join in and follow the host's guidance."),
            ("help-lobby-rules", "LIVE ROOM RULES", "Live Room Rules", "Respect everyone in the room. Do not record or take screenshots without permission. Avoid disruptive noise or repeatedly speaking over others. Debate ideas rather than attacking people, and follow the host's instructions. Nudity, threatening behaviour and inappropriate material are not permitted."),
        ),
    },
    {
        "id": "help-live-room-debate", "parent_id": "app-live-room", "title": "DEBATE",
        "subtitle": "Join structured debates, share your thoughts and cast your vote.", "theme": "#c76c5c",
        "tutorials": (
            ("help-debate-overview", "WHAT IS A DEBATE?", "What is a Debate?", "Debates are hosted Live Room activities where members explore different sides of a topic. The aim is thoughtful conversation, not personal conflict: challenge the idea, never the person."),
            ("help-debate-join", "JOIN A DEBATE", "Join a Debate", "When a debate is open, use the participation control to register your interest. The host selects speakers and manages the speaking order so everyone gets a fair opportunity."),
            ("help-debate-thought", "SUBMIT A THOUGHT", "Submit a Thought", "Use the Thoughts box to share a point during the debate. Once sent, the control confirms that your thought was submitted. If you edit the text, you can submit the updated version again."),
            ("help-debate-vote", "VOTE IN A DEBATE", "Vote in a Debate", "When voting opens, choose the option that best reflects your view. Submit one considered vote and wait for the host to reveal the final result."),
            ("help-debate-results", "VIEW THE RESULT", "View the Result", "The result is revealed after voting closes. The display shows the final community vote while the host brings the activity to a close."),
        ),
    },
    {
        "id": "help-live-room-drawing", "parent_id": "app-live-room", "title": "DRAWING CHALLENGE",
        "subtitle": "Create, submit and vote in a community drawing challenge.", "theme": "#d68a3c",
        "tutorials": (
            ("help-drawing-overview", "WHAT IS A DRAWING CHALLENGE?", "What is a Drawing Challenge?", "Drawing Challenge is a light-hearted Live Room activity. Members respond to a shared prompt, submit their creation and enjoy the results together."),
            ("help-drawing-join", "JOIN THE CHALLENGE", "Join the Challenge", "Join while registrations are open. The host will introduce the prompt, explain the round and start the activity when everyone is ready."),
            ("help-drawing-submit", "SUBMIT YOUR DRAWING", "Submit Your Drawing", "Create your response to the current prompt and use the on-screen submission control before the round closes. Follow any format or time guidance shown by the host."),
            ("help-drawing-vote", "VOTE FOR A DRAWING", "Vote for a Drawing", "When voting opens, review the eligible drawings and choose your favourite. Vote fairly and celebrate the effort behind every entry."),
            ("help-drawing-results", "VIEW THE RESULTS", "View the Results", "After voting closes, the host reveals the results in the Live Room. Winning and participation activity may also contribute to your Alcove progress."),
        ),
    },
    {
        "id": "help-live-room-discussions", "parent_id": "app-live-room", "title": "DISCUSSIONS",
        "subtitle": "Take part in guided conversations with other Alcove members.", "theme": "#6f86d8",
        "tutorials": (
            ("help-discussions-overview", "WHAT IS A DISCUSSION?", "What is a Discussion?", "Discussions are hosted conversations built around a subject or question. They are less competitive than debates and focus on sharing experiences, ideas and different perspectives."),
            ("help-discussions-join", "JOIN A DISCUSSION", "Join a Discussion", "Enter the Live Room during a scheduled discussion and follow the host's guidance. Listen to others, avoid dominating the conversation and contribute when you feel comfortable."),
            ("help-discussions-thought", "SHARE A THOUGHT", "Share a Thought", "Use the Thoughts box when it is available to contribute alongside the spoken discussion. Keep your message relevant, respectful and easy for the room to follow."),
        ),
    },
    {
        "id": "help-profile-exp", "parent_id": "app-profile", "title": "EXPERIENCE POINTS",
        "subtitle": "Understand EXP, levels and how your participation is recognised.", "theme": "#4d9bd6",
        "tutorials": (
            ("help-exp-overview", "WHAT ARE EXPERIENCE POINTS?", "What are Experience Points?", "Experience Points, or **EXP**, recognise positive participation across The Alcove. Your EXP contributes to your Alcove level and unlocks rewards as you continue taking part."),
            ("help-exp-earn", "EARN EXP", "Earn EXP", "Earn EXP by taking part in eligible Alcove activities, such as Live Room sessions, Pulse, Spotlight and other community features. The amount awarded depends on the activity."),
            ("help-exp-summary", "DAILY EXP SUMMARY", "Daily EXP Summary", "Open **Daily EXP Summary** on your Profile to review the EXP recorded from your recent activity and see how it contributed to your progress."),
            ("help-exp-levels", "LEVELS AND REWARDS", "Levels and Rewards", "Your Alcove level increases as your EXP grows. When a new level includes a reward, a present marked **NEW** appears on your Profile until you open it."),
        ),
    },
    {
        "id": "help-profile-reward-packs", "parent_id": "app-profile", "title": "REWARD PACKS",
        "subtitle": "Open earned packs and discover new profile rewards.", "theme": "#d39a37",
        "tutorials": (
            ("help-rewards-overview", "WHAT ARE REWARD PACKS?", "What are Reward Packs?", "Reward Packs contain customisation items earned through your Alcove progress. Available rewards may include visual items that can be used across your Profile and experience."),
            ("help-rewards-open", "OPEN A REWARD PACK", "Open a Reward Pack", "Open the reward area from your Profile and select an available pack. Follow the reveal to see what you have received; the item is then added to your available rewards."),
            ("help-rewards-level-up", "CLAIM A LEVEL-UP REWARD", "Claim a Level-Up Reward", "When the **NEW** present appears on your Profile, open it to view the rewards waiting from your latest level. The indicator remains until the reward has been viewed."),
            ("help-rewards-use", "USE YOUR REWARDS", "Use Your Rewards", "Open the relevant Profile customisation area, select an available reward and apply it. You can return later to change or remove equipped items."),
        ),
    },
    {
        "id": "help-profile-achievements", "parent_id": "app-profile", "title": "ACHIEVEMENTS",
        "subtitle": "Track milestones and display completed achievements on your Profile.", "theme": "#8d71c9",
        "tutorials": (
            ("help-achievements-overview", "WHAT ARE ACHIEVEMENTS?", "What are Achievements?", "Achievements mark milestones reached through your activity in The Alcove. Some achievement families progress through one-star, two-star and three-star tiers."),
            ("help-achievements-unlock", "UNLOCK ACHIEVEMENTS", "Unlock Achievements", "Complete the requirement shown on an achievement to unlock it. Progress updates as eligible activity is recorded, and a completion notice appears when a new achievement is earned."),
            ("help-achievements-equip", "EQUIP AN ACHIEVEMENT", "Equip an Achievement", "Open **Achievements**, choose a completed achievement and select **EQUIP**. It will appear in one of the available achievement positions on your Profile. Select **UNEQUIP** to remove it."),
            ("help-achievements-filter", "FILTER ACHIEVEMENTS", "Filter Achievements", "Use **All**, **In Progress** and **Completed** to quickly find the achievements you want to view or equip."),
        ),
    },
    {
        "id": "help-profile-customisation", "parent_id": "app-profile", "title": "CUSTOMISATION",
        "subtitle": "Personalise your Profile and use the rewards you have unlocked.", "theme": "#55a994",
        "tutorials": (
            ("help-customisation-profile", "CUSTOMISE YOUR PROFILE", "Customise Your Profile", "Open your Profile to manage the personal and visual options currently available to you. Your changes are saved to your Alcove account."),
            ("help-customisation-skins", "APPLY MESSAGE SKINS", "Apply Message Skins", "Choose an unlocked message skin from the relevant customisation control and apply it. The selected texture or design is then used across supported messages."),
            ("help-customisation-equipped", "MANAGE EQUIPPED ITEMS", "Manage Equipped Items", "Review the items currently displayed on your Profile. Equip an available item to use it, or unequip it when you want to make space for something different."),
        ),
    },
    {
        "id": "help-connect-pulse", "parent_id": "app-connect", "title": "PULSE",
        "subtitle": "Ask anonymous questions and respond using daily Pulse orbs.", "theme": "#39b977",
        "tutorials": (
            ("help-pulse-overview", "WHAT IS PULSE?", "What is Pulse?", "Pulse is The Alcove's anonymous question and reflection space. Members can submit thoughtful questions and use daily orbs to respond to questions from the community."),
            ("help-pulse-submit", "SUBMIT A PULSE QUESTION", "Submit a Pulse Question", "Choose Mental, Physical or General, then write and submit your question. You can submit up to two questions per day. Questions are anonymous and require approval before they appear to members."),
            ("help-pulse-answer", "ANSWER A QUESTION", "Answer a Question", "Choose an available community question and submit your response with an orb. You cannot answer your own question. Questions remain open for responses for their configured availability period."),
            ("help-pulse-green", "GREEN ORBS", "Green Orbs", "Green orbs are used for your everyday Pulse answers. You begin with one available orb, with another becoming available every four hours until you have up to six. A maximum of six green orbs can be used each day."),
            ("help-pulse-red", "RED PULSE", "Red Pulse", "Red Pulse is unlocked by the community reaching the daily green-answer target. To take part, you must also have answered at least one green Pulse question that day. When eligible, you can submit one Red Pulse response for that unlocked cycle."),
        ),
    },
    {
        "id": "help-connect-spotlight", "parent_id": "app-connect", "title": "SPOTLIGHT",
        "subtitle": "Recognise the care, character and contributions of fellow members.", "theme": "#b986da",
        "tutorials": (
            ("help-spotlight-overview", "WHAT IS SPOTLIGHT?", "What is Spotlight?", "Spotlight is The Alcove's member-recognition feature. It gives you a thoughtful way to acknowledge people who have made a positive difference to the community."),
            ("help-spotlight-nominate", "NOMINATE A MEMBER", "Nominate a Member", "Open Spotlight, choose an eligible member and select the award that best reflects what you want to recognise. Add any supporting message requested and confirm your nomination."),
            ("help-spotlight-awards", "THE FOUR SPOTLIGHT AWARDS", "The Four Spotlight Awards", "Spotlight includes four forms of recognition: **Recognition**, **Appreciation**, **Respect** and **Support**. Choose the one that most closely matches the contribution you want to celebrate."),
            ("help-spotlight-send", "SEND A SPOTLIGHT AWARD", "Send a Spotlight Award", "Review the member, award and message you selected, then submit it. F.O.X and the app will use the same Spotlight activity when displaying the result."),
            ("help-spotlight-activity", "VIEW SPOTLIGHT ACTIVITY", "View Spotlight Activity", "Use Spotlight and the Archive to see relevant recognition activity, including awards that have been published or received."),
        ),
    },
    {
        "id": "help-connect-check-in", "parent_id": "app-connect", "title": "DAILY CHECK-IN",
        "subtitle": "A quick space to pause and record how you are feeling.", "theme": "#5b9cb8",
        "tutorials": (
            ("help-check-in-overview", "WHAT IS DAILY CHECK-IN?", "What is Daily Check-In?", "Daily Check-In offers a quick, low-pressure moment to pause and record how you are feeling. It helps you reflect without needing to begin a longer conversation."),
            ("help-check-in-complete", "COMPLETE A CHECK-IN", "Complete a Check-In", "Open **Daily Check-In** from Connect and follow the short on-screen prompts. Submit your response when you are ready, and only share what feels comfortable to you."),
        ),
    },
    {
        "id": "help-archive-pulse", "parent_id": "app-archive", "title": "PULSE HISTORY",
        "subtitle": "Revisit previous Pulse questions, answers and activity.", "theme": "#3eaa7a",
        "tutorials": (
            ("help-pulse-history-overview", "WHAT IS PULSE HISTORY?", "What is Pulse History?", "Pulse History brings together the previous Pulse activity available to your account, making it easier to revisit questions and responses without returning to the active daily flow."),
            ("help-pulse-history-find", "FIND PAST PULSE QUESTIONS", "Find Past Pulse Questions", "Open **Archive**, choose **Pulse History** and browse the available entries. Select an item to view the information retained for that question."),
            ("help-pulse-history-yours", "VIEW YOUR PULSE ACTIVITY", "View Your Pulse Activity", "Use Pulse History to review the activity associated with your account, subject to Pulse's anonymity and privacy rules."),
        ),
    },
    {
        "id": "help-archive-spotlight", "parent_id": "app-archive", "title": "SPOTLIGHT HISTORY",
        "subtitle": "Browse past recognition and awards recorded in The Alcove.", "theme": "#9470c2",
        "tutorials": (
            ("help-spotlight-history-overview", "WHAT IS SPOTLIGHT HISTORY?", "What is Spotlight History?", "Spotlight History provides an archive of eligible published recognition activity from across The Alcove."),
            ("help-spotlight-history-browse", "BROWSE PAST RECOGNITION", "Browse Past Recognition", "Open **Archive**, select **Spotlight History** and use the available categories to browse previous Spotlight activity."),
            ("help-spotlight-history-yours", "VIEW YOUR AWARDS", "View Your Awards", "Open your relevant Spotlight history to review awards associated with your account and the recognition shared by the community."),
        ),
    },
    {
        "id": "help-archive-live-room", "parent_id": "app-archive", "title": "LIVE ROOM HISTORY",
        "subtitle": "Review previous sessions, activities and published results.", "theme": "#4b82bd",
        "tutorials": (
            ("help-live-history-overview", "WHAT IS LIVE ROOM HISTORY?", "What is Live Room History?", "Live Room History collects available records from previous sessions and activities so you can look back at what happened in The Alcove."),
            ("help-live-history-sessions", "VIEW PAST SESSIONS", "View Past Sessions", "Open **Archive**, choose **Live Room History** and browse the available session records. Select a session to view its saved details."),
            ("help-live-history-results", "REVIEW ACTIVITY RESULTS", "Review Activity Results", "Where results have been retained, select a past activity to review its outcome, such as a debate vote or Drawing Challenge result."),
        ),
    },
)

LEGACY_BODY = {
    "rules": "🌿 Welcome to The Alcove\n\nA calmer space for wellbeing, respect, and real conversation — seeing each other as whole people, not just profiles.\n\nBy joining, you agree to:\n\n**Group Rules**\nRespect all members. Harassment, hate speech, bullying, or intimidation may lead to removal.\n• No spam, adverts, promo posts, group links, or text-bombing. F.O.X will remove these.\n• No illegal, violent, or pornographic content. You are responsible for what you post.\n• Do not dox, expose, or share anyone's personal details.\n• Ardyn and F.O.X may remove content or members to keep the space safe.\n\n**Video Chat Rules**\n• Cameras and mics should be on where possible.\n• Let everyone speak. Don't dominate the conversation.\n• Take part; calls are interactive and inclusive.\n• Respect the host. Rudeness or disruption may result in being muted.\n\n💚 Have fun, be kind, and look after each other.",
    "ardyn": "Hey everyone, Ardyn Sky here.\n\nI created The Alcove because I wanted to build something different from the usual Telegram spaces — somewhere warmer, safer, more thoughtful, and more human.\n\nIt's a place where people can still laugh, flirt, be cheeky, and have fun, but where connection, care, and respect sit at the heart of everything.\n\nThe Alcove is for people who want to be seen as more than a profile picture, a body, or a quick message in a busy chat. It's about creating space for real conversation, honest questions, mutual support, and deeper connections.\n\nF.O.X is our little guardian, here to welcome new residents, protect the vibe, guide the space, and add a bit of whimsy along the way.\n\nThe Alcove is still growing, and it will continue to be shaped by the people who join it. My hope is that it becomes a place where people can show up as they are, feel they belong, and connect without pressure or judgement.",
    "guide": "Tap **LAUNCH APP** below or type `/app` to open The Alcove inside Telegram.\n\n**What's included**\n• **Pulse** — daily questions and anonymous reflections\n• **Daily Check-In** — quick wellbeing check-ins\n• **Spotlight** — celebrate and recognise members\n• **Connect** — your home hub for community tools\n• **VC Companion** — video calls made interactive\n• **Profile** — customise your experience, and earn achievements\n• **Archive** — your Alcove journey, chronicled for you\n\n_More features are added as The Alcove grows._",
    "faq": "**The Alcove Group**\nQ: What is this group?\nA: A calmer space for wellbeing, respect, and real conversation.\n\nQ: Why were my links removed?\nA: The group is link-free. F.O.X removes links, adverts, promos, and group invites.\n\nQ: Where are the full rules?\nA: Tap **Group Rules** in this menu.\n\n**F.O.X**\nQ: What does F.O.X do?\nA: Welcomes members, verifies newcomers, removes spam/links, and keeps the space safe.\n\nQ: What can I type?\nA: `/help` — this menu · `/app` — Mini App\nDuring a live call, message F.O.X privately: `/draw`, `/debate`, or `/discuss`.\n\nQ: Why did F.O.X DM me?\nA: New members verify privately before posting freely in the group.\n\n**Mini App**\nQ: How do I open it?\nA: Tap **LAUNCH APP** below or type `/app`.\n\nQ: What's included?\nA: Pulse, Check-In, Spotlight, Connect, VC Companion, Profile, and Archive.\n\nQ: What is Pulse?\nA: Daily anonymous questions and community reflections.\n\nContact @Ardyn_Sky for help.",
}


def _data_dir() -> Path:
    raw = os.getenv("ALCOVE_STATE_DB_PATH") or os.getenv("FOX_MESSAGES_PATH")
    return Path(raw).expanduser().resolve().parent if raw else Path.cwd()


def _db_path() -> Path:
    return Path(os.getenv("HELP_CMS_DB_PATH", str(_data_dir() / "help_cms.sqlite3"))).expanduser().resolve()


def _media_dir() -> Path:
    path = Path(os.getenv("HELP_MEDIA_DIR", str(_data_dir() / "help-media"))).expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _seed_app_help_content(con: sqlite3.Connection, now: str) -> None:
    con.execute(
        "CREATE TABLE IF NOT EXISTS help_content_migrations "
        "(name TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
    )
    if con.execute(
        "SELECT 1 FROM help_content_migrations WHERE name=?",
        (APP_HELP_CONTENT_MIGRATION,),
    ).fetchone():
        return

    def existing_id(
        item_id: str,
        parent_id: Optional[str],
        internal_name: str,
        item_type: str,
        sort_order: int,
    ) -> Optional[str]:
        row = con.execute("SELECT id,parent_id,type FROM help_items WHERE id=?", (item_id,)).fetchone()
        if row:
            if row["parent_id"] != parent_id or row["type"] != item_type:
                con.execute(
                    "UPDATE help_items SET parent_id=?,type=?,sort_order=?,updated_at=? WHERE id=?",
                    (parent_id, item_type, sort_order, now, row["id"]),
                )
            return row["id"]
        row = con.execute(
            "SELECT id,parent_id,type FROM help_items WHERE parent_id IS ? AND internal_name=? LIMIT 1",
            (parent_id, internal_name),
        ).fetchone()
        if row and row["type"] != item_type:
            con.execute(
                "UPDATE help_items SET type=?,sort_order=?,updated_at=? WHERE id=?",
                (item_type, sort_order, now, row["id"]),
            )
        return row["id"] if row else None

    for root_order, title in enumerate(APP_ROOTS):
        root_id = "app-" + title.lower().replace(" ", "-")
        root = con.execute("SELECT id,parent_id,type FROM help_items WHERE id=?", (root_id,)).fetchone()
        if not root:
            con.execute(
                "INSERT INTO help_items("
                "id,parent_id,type,internal_name,title,button_text,sort_order,show_in_app,"
                "show_in_telegram,status,created_at,updated_at"
                ") VALUES(?,NULL,'container',?,?,?,?,1,0,'draft',?,?)",
                (root_id, title, title, title, root_order, now, now),
            )
        elif root["parent_id"] is not None or root["type"] != "container":
            con.execute(
                "UPDATE help_items SET parent_id=NULL,type='container',updated_at=? WHERE id=?",
                (now, root_id),
            )

    section_orders = {}
    for section in APP_HELP_CONTENT:
        section_order = section_orders.get(section["parent_id"], 0)
        section_orders[section["parent_id"]] = section_order + 1
        internal_name = section["title"].title()
        section_id = existing_id(
            section["id"], section["parent_id"], internal_name, "container", section_order
        )
        if not section_id:
            section_id = section["id"]
            con.execute(
                "INSERT INTO help_items("
                "id,parent_id,type,internal_name,title,button_text,subtitle,theme,sort_order,"
                "show_in_app,show_in_telegram,status,created_at,updated_at"
                ") VALUES(?,?, 'container',?,?,?,?,?,?,1,1,'draft',?,?)",
                (
                    section_id, section["parent_id"], internal_name, section["title"],
                    section["title"].title(), section["subtitle"], section["theme"],
                    section_order, now, now,
                ),
            )

        for tutorial_order, (item_id, title, button_text, body) in enumerate(section["tutorials"]):
            tutorial_name = f"{section['title'].title()} — {button_text}"
            if existing_id(item_id, section_id, tutorial_name, "content", tutorial_order):
                continue
            con.execute(
                "INSERT INTO help_items("
                "id,parent_id,type,internal_name,title,button_text,body,sort_order,"
                "show_in_app,show_in_telegram,status,created_at,updated_at"
                ") VALUES(?,?, 'content',?,?,?,?,?,1,1,'draft',?,?)",
                (
                    item_id, section_id, tutorial_name, title, button_text, body,
                    tutorial_order, now, now,
                ),
            )

    con.execute(
        "INSERT INTO help_content_migrations(name,applied_at) VALUES(?,?)",
        (APP_HELP_CONTENT_MIGRATION, now),
    )


def _admin(secret: Optional[str]):
    expected = os.getenv("BOT_SYNC_SECRET", "")
    if not expected:
        raise HTTPException(503, "Admin secret is not configured")
    if secret != expected:
        raise HTTPException(403, "Invalid admin secret")


def _conn():
    path = _db_path(); path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("CREATE TABLE IF NOT EXISTS help_settings (id INTEGER PRIMARY KEY CHECK(id=1), terminal_title TEXT NOT NULL, introduction TEXT NOT NULL, default_button_style TEXT NOT NULL, default_back_wording TEXT NOT NULL, default_home_wording TEXT NOT NULL, published INTEGER NOT NULL, root_media_url TEXT NOT NULL, launcher_video_url TEXT NOT NULL, launcher_fallback_image_url TEXT NOT NULL, launcher_video_opacity INTEGER NOT NULL DEFAULT 50, updated_at TEXT NOT NULL)")
    settings_columns = {row[1] for row in con.execute("PRAGMA table_info(help_settings)")}
    if "launcher_video_opacity" not in settings_columns:
        con.execute("ALTER TABLE help_settings ADD COLUMN launcher_video_opacity INTEGER NOT NULL DEFAULT 50")
    con.execute("CREATE TABLE IF NOT EXISTS help_items (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES help_items(id) ON DELETE RESTRICT, type TEXT NOT NULL CHECK(type IN ('container','content')), internal_name TEXT NOT NULL, title TEXT NOT NULL, button_text TEXT NOT NULL, subtitle TEXT NOT NULL DEFAULT '', body TEXT NOT NULL DEFAULT '', media_type TEXT NOT NULL DEFAULT 'none', media_url TEXT NOT NULL DEFAULT '', skin_media_url TEXT NOT NULL DEFAULT '', image_position TEXT NOT NULL DEFAULT '50% 0%', overlay_strength INTEGER NOT NULL DEFAULT 45, title_color TEXT NOT NULL DEFAULT '#ffffff', theme TEXT NOT NULL DEFAULT '', sort_order INTEGER NOT NULL DEFAULT 0, show_in_app INTEGER NOT NULL DEFAULT 1, show_in_telegram INTEGER NOT NULL DEFAULT 1, show_back INTEGER NOT NULL DEFAULT 1, show_home INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'draft' CHECK(status IN ('draft','published','hidden')), legacy_key TEXT UNIQUE, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
    con.execute("CREATE TABLE IF NOT EXISTS help_telegram_media_cache (media_url TEXT NOT NULL, bot_identity TEXT NOT NULL, source_sha256 TEXT NOT NULL, telegram_file_id TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(media_url,bot_identity))")
    now = _now()
    con.execute("INSERT OR IGNORE INTO help_settings(id,terminal_title,introduction,default_button_style,default_back_wording,default_home_wording,published,root_media_url,launcher_video_url,launcher_fallback_image_url,launcher_video_opacity,updated_at) VALUES(1,?,?,?,?,?,?,?,?,?,?,?)", ("F.O.X HELP TERMINAL", "_Thank you for accessing the terminal._\n**How can I help today?**", "primary", "↩ ┃ Back to menu", "HELP HOME", 1, "assets/help_terminal_banner.png", "", "", 50, now))
    count = con.execute("SELECT COUNT(*) FROM help_items").fetchone()[0]
    if count == 0:
        for order, (item_id, name, title, button, key, media) in enumerate(LEGACY_ITEMS):
            con.execute("INSERT INTO help_items(id,parent_id,type,internal_name,title,button_text,body,media_type,media_url,sort_order,show_in_app,show_in_telegram,status,legacy_key,created_at,updated_at) VALUES(?,NULL,'content',?,?,?,?, 'image',?,?,0,1,'published',?,?,?)", (item_id,name,title,button,LEGACY_BODY[key],media,order,key,now,now))
        for order, title in enumerate(APP_ROOTS):
            item_id = "app-" + title.lower().replace(" ", "-")
            con.execute("INSERT INTO help_items(id,parent_id,type,internal_name,title,button_text,sort_order,show_in_app,show_in_telegram,status,created_at,updated_at) VALUES(?,NULL,'container',?,?,?,?,1,0,'published',?,?)", (item_id,title,title,title,order,now,now))
    _seed_app_help_content(con, now)
    con.commit()
    return con


def _row(row):
    value = dict(row)
    for key in ("show_in_app","show_in_telegram","show_back","show_home"):
        value[key] = bool(value[key])
    return value


def _tree(rows):
    items = [_row(row) for row in rows]
    by_id = {item["id"]: item for item in items}
    roots = []
    for item in items:
        item["children"] = []
    for item in items:
        parent = by_id.get(item["parent_id"])
        (parent["children"] if parent else roots).append(item)
    return roots


def _visible_tree(items, destination):
    visibility_key = "show_in_app" if destination == "app" else "show_in_telegram"

    def keep(item):
        if item["status"] != "published" or not item[visibility_key]:
            return None
        visible = dict(item)
        visible["children"] = [child for child in (keep(child) for child in item["children"]) if child]
        return visible

    return [item for item in (keep(item) for item in items) if item]


class SettingsPayload(BaseModel):
    admin_secret: str
    terminal_title: str
    introduction: str = ""
    default_button_style: str = "primary"
    default_back_wording: str = "BACK"
    default_home_wording: str = "HELP HOME"
    published: bool = True
    root_media_url: str = ""
    launcher_video_url: str = ""
    launcher_fallback_image_url: str = ""
    launcher_video_opacity: Literal[25, 50, 75] = 50

    @model_validator(mode="after")
    def validate_telegram_limits(self):
        telegram_units = lambda value: len(str(value).encode("utf-16-le")) // 2
        if telegram_units(self.introduction) > 1000:
            raise ValueError("Telegram Help introduction must be 1000 characters or fewer")
        if telegram_units(self.default_back_wording) > 64 or telegram_units(self.default_home_wording) > 64:
            raise ValueError("Telegram Help navigation labels must be 64 characters or fewer")
        return self


class ItemPayload(BaseModel):
    admin_secret: str
    parent_id: Optional[str] = None
    type: Literal["container", "content"]
    internal_name: str
    title: str
    button_text: str
    subtitle: str = ""
    body: str = ""
    media_type: Literal["none", "image", "video"] = "none"
    media_url: str = ""
    skin_media_url: str = ""
    image_position: str = "50% 0%"
    overlay_strength: int = 45
    title_color: str = "#ffffff"
    theme: str = ""
    sort_order: Optional[int] = None
    show_in_app: bool = True
    show_in_telegram: bool = True
    show_back: bool = True
    show_home: bool = True
    status: Literal["draft", "published", "hidden"] = "draft"

    @model_validator(mode="after")
    def validate_telegram_limits(self):
        if not self.show_in_telegram:
            return self
        telegram_units = lambda value: len(str(value).encode("utf-16-le")) // 2
        if telegram_units(self.button_text) > 64:
            raise ValueError("Telegram button text must be 64 characters or fewer")
        caption_length = telegram_units(self.body)
        if not self.internal_name.startswith("legacy-"):
            caption_length += telegram_units(self.title) + telegram_units(self.subtitle) + 4
        if caption_length > 1000:
            raise ValueError("Telegram Help captions must be 1000 characters or fewer")
        return self


class PositionPayload(BaseModel):
    admin_secret: str
    parent_id: Optional[str] = None
    sort_order: int = 0


class AdminPayload(BaseModel):
    admin_secret: str


def _validate_parent(con, item_id, parent_id):
    if not parent_id:
        return
    parent = con.execute("SELECT id,parent_id,type FROM help_items WHERE id=?", (parent_id,)).fetchone()
    if not parent or parent["type"] != "container":
        raise HTTPException(400, "Parent must be an existing container")
    cursor = parent
    while cursor:
        if cursor["id"] == item_id:
            raise HTTPException(400, "An item cannot be moved beneath itself or its descendant")
        cursor = con.execute("SELECT id,parent_id,type FROM help_items WHERE id=?", (cursor["parent_id"],)).fetchone() if cursor["parent_id"] else None


def _renumber_parent(con, parent_id, excluded_id=None):
    if excluded_id:
        rows = con.execute(
            "SELECT id FROM help_items WHERE parent_id IS ? AND id<>? ORDER BY sort_order,created_at",
            (parent_id, excluded_id),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT id FROM help_items WHERE parent_id IS ? ORDER BY sort_order,created_at",
            (parent_id,),
        ).fetchall()
    for order, row in enumerate(rows):
        con.execute("UPDATE help_items SET sort_order=? WHERE id=?", (order, row["id"]))
    return [row["id"] for row in rows]


def _place_item(con, item_id, parent_id, target_order):
    current = con.execute("SELECT parent_id FROM help_items WHERE id=?", (item_id,)).fetchone()
    old_parent_id = current["parent_id"]
    if old_parent_id != parent_id:
        _renumber_parent(con, old_parent_id, item_id)
    ordered_ids = _renumber_parent(con, parent_id, item_id)
    target_order = min(max(0, int(target_order)), len(ordered_ids))
    ordered_ids.insert(target_order, item_id)
    now = _now()
    for order, sibling_id in enumerate(ordered_ids):
        con.execute(
            "UPDATE help_items SET parent_id=?,sort_order=?,updated_at=? WHERE id=?",
            (parent_id, order, now, sibling_id),
        )


@router.get("/help")
def public_help(destination: Literal["app", "telegram"] = "app"):
    with _LOCK:
        con = _conn()
        settings = dict(con.execute("SELECT * FROM help_settings WHERE id=1").fetchone())
        rows = con.execute("SELECT * FROM help_items ORDER BY parent_id,sort_order,created_at").fetchall()
        con.close()
    settings["published"] = bool(settings["published"])
    return {"settings": settings, "items": _visible_tree(_tree(rows), destination) if settings["published"] else []}


@router.get("/admin/help")
def admin_help(admin_secret: Optional[str] = None, x_admin_secret: Optional[str] = Header(None)):
    _admin(x_admin_secret or admin_secret)
    with _LOCK:
        con = _conn(); settings = dict(con.execute("SELECT * FROM help_settings WHERE id=1").fetchone())
        rows = con.execute("SELECT * FROM help_items ORDER BY parent_id,sort_order,created_at").fetchall(); con.close()
    settings["published"] = bool(settings["published"])
    return {"settings": settings, "items": _tree(rows)}


@router.put("/admin/help/settings")
def save_settings(payload: SettingsPayload):
    _admin(payload.admin_secret); data = payload.model_dump(exclude={"admin_secret"}); data["published"] = int(data["published"]); data["updated_at"] = _now()
    with _LOCK:
        con = _conn(); con.execute("UPDATE help_settings SET " + ",".join(f"{key}=?" for key in data) + " WHERE id=1", tuple(data.values())); con.commit()
        row = dict(con.execute("SELECT * FROM help_settings WHERE id=1").fetchone()); con.close()
    row["published"] = bool(row["published"]); return {"settings": row}


@router.post("/admin/help/items")
def create_item(payload: ItemPayload):
    _admin(payload.admin_secret); item_id = uuid.uuid4().hex[:16]; now = _now()
    with _LOCK:
        con = _conn(); _validate_parent(con,item_id,payload.parent_id)
        order = payload.sort_order if payload.sort_order is not None else con.execute("SELECT COALESCE(MAX(sort_order)+1,0) FROM help_items WHERE parent_id IS ?",(payload.parent_id,)).fetchone()[0]
        data = payload.model_dump(exclude={"admin_secret","sort_order"}); data.update(id=item_id,sort_order=order,created_at=now,updated_at=now)
        keys=list(data); con.execute(f"INSERT INTO help_items({','.join(keys)}) VALUES({','.join('?' for _ in keys)})",tuple(data[k] for k in keys)); con.commit()
        row=con.execute("SELECT * FROM help_items WHERE id=?",(item_id,)).fetchone(); con.close()
    return {"item":_row(row)}


@router.patch("/admin/help/items/{item_id}/position")
def move_item(item_id: str, payload: PositionPayload):
    _admin(payload.admin_secret)
    with _LOCK:
        con=_conn(); current=con.execute("SELECT id,parent_id FROM help_items WHERE id=?",(item_id,)).fetchone()
        if not current: con.close(); raise HTTPException(404,"Help item not found")
        _validate_parent(con,item_id,payload.parent_id)
        _place_item(con, item_id, payload.parent_id, payload.sort_order)
        con.commit()
        row=con.execute("SELECT * FROM help_items WHERE id=?",(item_id,)).fetchone(); con.close()
    return {"item":_row(row)}


@router.put("/admin/help/items/{item_id}")
def update_item(item_id: str, payload: ItemPayload):
    _admin(payload.admin_secret)
    with _LOCK:
        con = _conn()
        current = con.execute("SELECT id,parent_id,sort_order FROM help_items WHERE id=?", (item_id,)).fetchone()
        if not current:
            con.close(); raise HTTPException(404, "Help item not found")
        _validate_parent(con, item_id, payload.parent_id)
        data = payload.model_dump(exclude={"admin_secret"})
        requested_order = data.pop("sort_order")
        requested_parent = data.pop("parent_id")
        data["updated_at"] = _now()
        con.execute("UPDATE help_items SET " + ",".join(f"{key}=?" for key in data) + " WHERE id=?", tuple(data.values()) + (item_id,))
        if requested_parent != current["parent_id"]:
            destination_count = con.execute("SELECT COUNT(*) FROM help_items WHERE parent_id IS ? AND id<>?", (requested_parent, item_id)).fetchone()[0]
            _place_item(con, item_id, requested_parent, destination_count if requested_order is None else requested_order)
        elif requested_order is not None:
            _place_item(con, item_id, requested_parent, requested_order)
        con.commit(); row = con.execute("SELECT * FROM help_items WHERE id=?", (item_id,)).fetchone(); con.close()
    return {"item": _row(row)}


@router.post("/admin/help/items/{item_id}/duplicate")
def duplicate_item(item_id: str, payload: AdminPayload):
    _admin(payload.admin_secret)
    with _LOCK:
        con = _conn(); source = con.execute("SELECT * FROM help_items WHERE id=?", (item_id,)).fetchone()
        if not source: con.close(); raise HTTPException(404, "Help item not found")
        def copy_node(row, parent_id):
            new_id = uuid.uuid4().hex[:16]; now = _now(); values = dict(row)
            values.update(id=new_id, parent_id=parent_id, internal_name=values["internal_name"] + " — Copy", status="draft", legacy_key=None, created_at=now, updated_at=now)
            keys = list(values); con.execute(f"INSERT INTO help_items({','.join(keys)}) VALUES({','.join('?' for _ in keys)})", tuple(values[k] for k in keys))
            for child in con.execute("SELECT * FROM help_items WHERE parent_id=? ORDER BY sort_order", (row["id"],)).fetchall(): copy_node(child, new_id)
            return new_id
        new_id = copy_node(source, source["parent_id"])
        _place_item(con, new_id, source["parent_id"], source["sort_order"] + 1)
        con.commit(); row = con.execute("SELECT * FROM help_items WHERE id=?", (new_id,)).fetchone(); con.close()
    return {"item": _row(row)}


@router.delete("/admin/help/items/{item_id}")
def delete_item(item_id: str, admin_secret: Optional[str] = None, x_admin_secret: Optional[str] = Header(None)):
    _admin(x_admin_secret or admin_secret)
    with _LOCK:
        con = _conn()
        if con.execute("SELECT 1 FROM help_items WHERE parent_id=? LIMIT 1", (item_id,)).fetchone():
            con.close(); raise HTTPException(409, "Move or delete child items first")
        changed = con.execute("DELETE FROM help_items WHERE id=?", (item_id,)).rowcount; con.commit(); con.close()
    if not changed: raise HTTPException(404, "Help item not found")
    return {"ok": True}


@router.post("/admin/help/media")
async def upload_media(admin_secret: Optional[str] = Form(None), file: UploadFile = File(...), x_admin_secret: Optional[str] = Header(None)):
    _admin(x_admin_secret or admin_secret); original=Path(file.filename or "help-media"); ext=original.suffix.lower()
    if ext not in IMAGE_EXTENSIONS | VIDEO_EXTENSIONS: raise HTTPException(400,"Use PNG, JPEG, WebP, GIF, or MP4 media")
    limit=VIDEO_MAX_BYTES if ext in VIDEO_EXTENSIONS else IMAGE_MAX_BYTES
    stem=re.sub(r"[^a-zA-Z0-9_-]+","-",original.stem).strip("-")[:50] or "help"
    filename=f"{stem}-{uuid.uuid4().hex[:10]}{ext}"; target=_media_dir()/filename; total=0; digest=hashlib.sha256()
    try:
        with target.open("wb") as handle:
            while chunk := await file.read(1024*1024):
                total += len(chunk)
                if total > limit: raise HTTPException(413,"Media file is too large")
                digest.update(chunk); handle.write(chunk)
    except Exception:
        target.unlink(missing_ok=True); raise
    if not total: target.unlink(missing_ok=True); raise HTTPException(400,"Media file is empty")
    return {"asset":{"url":f"/api/help/media/{filename}","filename":filename,"size":total,"sha256":digest.hexdigest(),"media_type":"video" if ext in VIDEO_EXTENSIONS else "image"}}


@router.get("/help/media/{filename}")
def serve_media(filename: str):
    clean=Path(filename).name
    if clean != filename: raise HTTPException(404,"Media not found")
    path=(_media_dir()/clean).resolve()
    if path.parent != _media_dir().resolve() or not path.is_file(): raise HTTPException(404,"Media not found")
    return FileResponse(path)
