"""Name-only public-wheel guests, scoped separately from verified Mini App members."""
from __future__ import annotations

from contextlib import closing, contextmanager
import hashlib
import secrets
import sqlite3
import threading
import time

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(prefix='/api/public-wheel', tags=['public-wheel'])
_LOCK = threading.RLock()
SESSION_TTL_SECONDS = 12 * 60 * 60


@contextmanager
def _connection():
    from .member_progress import _db_path
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(str(path), timeout=20)) as con, con:
        con.row_factory = sqlite3.Row
        con.execute('''CREATE TABLE IF NOT EXISTS public_wheel_sessions (
            token_hash TEXT PRIMARY KEY, guest_id INTEGER NOT NULL,
            display_name TEXT NOT NULL, expires_at REAL NOT NULL
        )''')
        yield con


class SessionRequest(BaseModel):
    display_name: str = Field(min_length=1, max_length=48)


class EntryRequest(BaseModel):
    link: str = Field(min_length=1, max_length=4096)
    note: str | None = Field(default=None, max_length=1200)
    video_title: str | None = Field(default=None, max_length=500)
    video_longer_than_5_minutes: bool | None = None
    clip_start_seconds: int | None = Field(default=None, ge=0)
    clip_start_label: str | None = Field(default=None, max_length=80)


class ReviewRequest(BaseModel):
    rating: int = Field(ge=1, le=5)
    review: str = Field(default='', max_length=600)
    anonymous: bool = False


class CommentRequest(BaseModel):
    text: str = Field(max_length=140)
    feed_style: dict | None = None


class ReactionRequest(BaseModel):
    reaction_key: str = Field(min_length=1, max_length=24)


@router.post('/session')
def create_session(payload: SessionRequest):
    name = ' '.join(payload.display_name.split())
    if not name:
        raise HTTPException(400, 'Please enter a display name first.')
    token = secrets.token_urlsafe(32)
    # Negative IDs cannot alias Telegram member IDs and remain exact in JavaScript.
    guest_id = -(secrets.randbelow(2**51) + 1)
    expires_at = time.time() + SESSION_TTL_SECONDS
    with _LOCK, _connection() as con:
        con.execute('DELETE FROM public_wheel_sessions WHERE expires_at<=?', (time.time(),))
        con.execute('INSERT INTO public_wheel_sessions VALUES (?, ?, ?, ?)',
            (hashlib.sha256(token.encode()).hexdigest(), guest_id, name, expires_at))
    return {'status':'ok', 'session_token':token, 'expires_at':expires_at,
            'identity':{'user_id':guest_id, 'username':None, 'display_name':name, 'anonymous':False}}


def guest_identity(token):
    if not token or len(token) > 200:
        raise HTTPException(401, 'Your public wheel session expired. Please try again.')
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    with _LOCK, _connection() as con:
        row = con.execute('SELECT * FROM public_wheel_sessions WHERE token_hash=?', (token_hash,)).fetchone()
    if not row or row['expires_at'] <= time.time():
        raise HTTPException(401, 'Your public wheel session expired. Please try again.')
    return {'user_id':row['guest_id'], 'username':None, 'display_name':row['display_name']}


@router.post('/entry')
def submit_entry(payload: EntryRequest, x_public_wheel_session: str | None = Header(default=None)):
    from . import main
    guest = guest_identity(x_public_wheel_session)
    return main.submit_wheel(main.WheelEntry(**payload.model_dump(),
        telegram_id=guest['user_id'], username=None, display_name=guest['display_name']))


@router.post('/review')
def submit_review(payload: ReviewRequest, x_public_wheel_session: str | None = Header(default=None)):
    from . import main
    guest = guest_identity(x_public_wheel_session)
    if not main.state.get('review_prompt_open'):
        return {'status':'error', 'message':'Reviews are closed right now.'}
    return main.submit_review(main.VideoReview(**payload.model_dump(), **guest))


@router.post('/comment')
def submit_comment(payload: CommentRequest, x_public_wheel_session: str | None = Header(default=None)):
    from . import main
    guest = guest_identity(x_public_wheel_session)
    return main.submit_stream_comment(main.StreamComment(**payload.model_dump(), **guest), authorization=None)


@router.post('/reaction')
def submit_reaction(payload: ReactionRequest, x_public_wheel_session: str | None = Header(default=None)):
    from . import main
    guest = guest_identity(x_public_wheel_session)
    return main.submit_wheel_reaction(main.WheelReaction(**payload.model_dump(), **guest))
