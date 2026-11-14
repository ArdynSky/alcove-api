"""Bounded browsing of completed London community days."""
from __future__ import annotations
import base64
import datetime
import hashlib
import json
from . import daily_checkin as dc

def _scope(value):
    return hashlib.sha256(json.dumps(value, separators=(',', ':')).encode()).hexdigest()

def _encode(scope, keys):
    return base64.urlsafe_b64encode(json.dumps({'v': 1, 'scope': _scope(scope), 'keys': keys}).encode()).decode().rstrip('=')

def _decode(raw, scope, count):
    if not raw:
        return None
    try:
        if len(raw) > 2048:
            raise ValueError()
        value = json.loads(base64.b64decode(raw + '=' * (-len(raw) % 4), altchars=b'-_', validate=True))
        keys = value['keys']
        if value['v'] != 1 or value['scope'] != _scope(scope):
            raise ValueError()
        if not isinstance(keys, list) or len(keys) != count or any((not isinstance(k, str) or len(k) > 500 for k in keys)):
            raise ValueError()
        return keys
    except (ValueError, KeyError, TypeError, UnicodeDecodeError):
        raise dc._http(400, 'That page cursor is not valid for these filters.') from None

def _date(raw):
    try:
        if not raw or len(raw) != 10:
            raise ValueError()
        parsed = datetime.date.fromisoformat(raw)
        if parsed.isoformat() != raw:
            raise ValueError()
        return parsed
    except ValueError:
        raise dc._http(400, 'Choose valid dates in YYYY-MM-DD format.') from None

def list_archive(user, *, mine=False, feeling='all', period='30d', date_from=None, date_to=None, name='', replies='all', sort='newest', cursor=None, limit=20):
    uid = dc._user_id(user)
    dc._limit(uid, 'archive_read', 120)
    clock = dc.community_clock()
    today = datetime.date.fromisoformat(clock['community_date'])
    if feeling not in ('all', *dc.FEELINGS) or period not in {'30d', '7d', 'year', 'all', 'custom'} or replies not in {'all', 'with', 'without', 'mine'} or (sort not in {'newest', 'oldest', 'az', 'za'}):
        raise dc._http(400, 'Choose valid archive filters.')
    name = str(name or '').strip()
    if len(name) > 80:
        raise dc._http(400, 'Name search can be at most 80 characters.')
    start = end = None
    if period == 'custom':
        start, end = (_date(date_from), _date(date_to))
        if start > end:
            raise dc._http(400, 'The start date must be before the end date.')
    elif period in {'30d', '7d'}:
        start = today - datetime.timedelta(days=30 if period == '30d' else 7)
    elif period == 'year':
        start = today.replace(month=1, day=1)
    scope = ['posts', uid, str(today), bool(mine), feeling, period, str(start or ''), str(end or ''), name, replies, sort]
    columns = ['p.community_date', 'p.created_at', 'p.id']
    descending = sort in {'newest', 'za'}
    if sort in {'az', 'za'}:
        columns = ['p.display_name COLLATE NOCASE', *columns]
    keys = _decode(cursor, scope, len(columns))
    clauses = ['p.community_date < ?', 'p.deleted_at IS NULL']
    params = [str(today)]
    if mine:
        clauses.append('p.user_id = ?')
        params.append(uid)
    if feeling != 'all':
        clauses.append('p.feeling = ?')
        params.append(feeling)
    if start:
        clauses.append('p.community_date >= ?')
        params.append(str(start))
    if end:
        clauses.append('p.community_date <= ?')
        params.append(str(end))
    if name:
        clauses.append("p.display_name LIKE ? ESCAPE '\\'")
        params.append('%' + name.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%')
    if replies != 'all':
        exists = 'EXISTS (SELECT 1 FROM daily_checkin_replies r WHERE r.checkin_id = p.id AND r.deleted_at IS NULL'
        if replies == 'mine':
            exists += ' AND r.user_id = ?'
            params.append(uid)
        clauses.append(('NOT ' if replies == 'without' else '') + exists + ')')
    if keys:
        clauses.append('(' + ','.join(columns) + ') ' + ('<' if descending else '>') + ' (' + ','.join(('?' for _ in keys)) + ')')
        params.extend(keys)
    direction = ' DESC' if descending else ' ASC'
    conn = dc._connect()
    try:
        rows = conn.execute('SELECT p.* FROM daily_checkins p WHERE ' + ' AND '.join(clauses) + ' ORDER BY ' + ','.join((c + direction for c in columns)) + ' LIMIT ?', (*params, limit + 1)).fetchall()
        visible = rows[:limit]
        next_cursor = None
        if len(rows) > limit:
            last = visible[-1]
            last_keys = [last['community_date'], last['created_at'], last['id']]
            if sort in {'az', 'za'}:
                last_keys.insert(0, last['display_name'])
            next_cursor = _encode(scope, last_keys)
        posts = [dc._public_checkin(conn, row, uid) for row in visible]
        for post in posts:
            post['can_reply'] = False
        return {'status': 'ok', 'posts': posts, 'next_cursor': next_cursor, **clock}
    finally:
        conn.close()

def _past_post(conn, id, today):
    row = conn.execute('SELECT * FROM daily_checkins WHERE id = ? AND community_date < ? AND deleted_at IS NULL', (id, today)).fetchone()
    if row is None:
        raise dc._http(404, 'That archived check-in is no longer available.')
    return row

def list_archive_replies(user, id, cursor=None, limit=20):
    uid = dc._user_id(user)
    dc._limit(uid, 'archive_read', 120)
    clock = dc.community_clock()
    scope = ['replies', uid, clock['community_date'], id]
    keys = _decode(cursor, scope, 2)
    conn = dc._connect()
    try:
        _past_post(conn, id, clock['community_date'])
        clause = 'checkin_id = ? AND deleted_at IS NULL'
        params = [id]
        if keys:
            clause += ' AND (created_at, id) > (?, ?)'
            params.extend(keys)
        rows = conn.execute('SELECT * FROM daily_checkin_replies WHERE ' + clause + ' ORDER BY created_at, id LIMIT ?', (*params, limit + 1)).fetchall()
        visible = rows[:limit]
        next_cursor = _encode(scope, [visible[-1]['created_at'], visible[-1]['id']]) if len(rows) > limit else None
        return {'status': 'ok', 'checkin_id': id, 'can_reply': False, 'replies': [{'id': r['id'], 'display_name': r['display_name'], 'message': r['message'], 'created_at': r['created_at'], 'is_self': str(r['user_id']) == uid, 'can_delete': False} for r in visible], 'next_cursor': next_cursor, **clock}
    finally:
        conn.close()

def delete_archive_post(user, id):
    uid = dc._user_id(user)
    dc._limit(uid, 'write', 20)
    clock = dc.community_clock()
    conn = dc._connect()
    try:
        conn.execute('BEGIN IMMEDIATE')
        parent = _past_post(conn, id, clock['community_date'])
        if str(parent['user_id']) != uid:
            raise dc._http(403, 'You can only delete your own check-ins.')
        stamp = dc._now_iso()
        conn.execute("UPDATE daily_checkins SET deleted_at = ?, message = '' WHERE id = ?", (stamp, id))
        conn.execute("UPDATE daily_checkin_replies SET deleted_at = ?, message = '' WHERE checkin_id = ? AND deleted_at IS NULL", (stamp, id))
        conn.commit()
        return {'status': 'ok', 'deleted': True, **clock}
    finally:
        conn.close()

