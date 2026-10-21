"""Run the PR's real question-game UI/API with isolated local demo data.

Usage: python tools/local_question_preview.py --frontend ../frontend --port 8000
This runner binds to loopback and never uses production databases or credentials.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import secrets
import sys

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--frontend', type=Path, required=True)
parser.add_argument('--port', type=int, default=8000)
args = parser.parse_args()
FRONTEND = args.frontend.resolve()
DATA = ROOT / '.local-question-preview'
DATA.mkdir(exist_ok=True)
sys.path.insert(0, str(ROOT))
# Always override inherited configuration: this test runner cannot use live data.
for key, value in {
    'ALCOVE_QUESTION_GAME_DIR': DATA / 'games',
    'ALCOVE_STATE_DB_PATH': DATA / 'runtime.sqlite3',
    'ALCOVE_CUSTOM_PACKS_DB': DATA / 'packs.sqlite3',
    'ALCOVE_CUSTOM_PACK_ASSETS': DATA / 'pack-assets',
    'ALCOVE_AGENDA_DB': DATA / 'agendas.sqlite3',
    'LIVE_ROOM_TEST_DB': DATA / 'sessions.sqlite3',
}.items():
    os.environ[key] = str(value)
os.environ['BOT_SYNC_SECRET'] = secrets.token_urlsafe(24)
os.environ['LIVE_ROOM_TEST_HANDOFF_SECRET'] = secrets.token_urlsafe(24)

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
import uvicorn
from api import question_games as q, question_game_routes as routes
from api import custom_rewards as cr, participation_rewards as pr
from api import live_room_test as auth, agendas

app = FastAPI(title='Alcove local question-game preview')
app.include_router(routes.router)
app.include_router(cr.router)
app.include_router(agendas.router)

PERSONAS = {
    'host': {'id': 8385145826, 'display_name': 'Local host', 'first_name': 'Local host'},
    'ardyn': {'id': 9001, 'display_name': 'Ardyn · Test', 'first_name': 'Ardyn · Test'},
    'alex': {'id': 9002, 'display_name': 'Alex · Test', 'first_name': 'Alex · Test'},
}
BOTS = [(1001, 'Sam'), (1002, 'Jamie'), (1003, 'Riley'), (1004, 'Morgan'), (1005, 'Taylor'), (1006, 'Charlie')]
CROWD = ['Name a pizza topping.', 'Name a colour.', 'Name a breakfast food.', 'Name a household chore.', 'Name a fruit.', 'Name a hot drink.', 'Name a holiday destination.', 'Name a pet animal.']
SAMPLE = [
    ['Pepperoni', 'Pepperoni', 'Pepperoni', 'Mushroom', 'Mushroom', 'Pineapple'],
    ['Blue', 'Blue', 'Blue', 'Green', 'Green', 'Red'],
    ['Toast', 'Toast', 'Toast', 'Eggs', 'Eggs', 'Cereal'],
    ['Washing up', 'Washing up', 'Washing up', 'Vacuuming', 'Vacuuming', 'Dusting'],
    ['Apple', 'Apple', 'Apple', 'Banana', 'Banana', 'Orange'],
    ['Tea', 'Tea', 'Tea', 'Coffee', 'Coffee', 'Chocolate'],
    ['Spain', 'Spain', 'Spain', 'Italy', 'Italy', 'Greece'],
    ['Dog', 'Dog', 'Dog', 'Cat', 'Cat', 'Rabbit'],
]
QUIZ = [('Capital of France?', ['Paris']), ('How many sides does a hexagon have?', ['6', 'six']), ('Largest planet?', ['Jupiter']), ('How many days in a leap year?', ['366']), ('Capital of Italy?', ['Rome']), ('What is 7 × 8?', ['56']), ('Which planet is known as the red planet?', ['Mars']), ('How many minutes in an hour?', ['60', 'sixty'])]

def seed_packs():
    con = cr._conn()
    try:
        packs = [
            ('local-participation', 'Local participation pack', [{'type': 'color', 'id': 'local-teal', 'name': 'Test teal', 'swatch': '#176B67'}]),
            ('local-winner', 'Local winner pack · two items', [{'type': 'color', 'id': 'local-gold', 'name': 'Test gold', 'swatch': '#A66B22'}, {'type': 'sticker', 'id': 'local-star', 'name': 'Test star', 'emoji': '⭐'}]),
        ]
        for tid, name, items in packs:
            con.execute('INSERT OR IGNORE INTO custom_pack_templates(id,name,description,image,items_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)', (tid, name, 'Local testing only', '', json.dumps(items), cr._now(), cr._now()))
        con.commit()
    finally:
        con.close()

def preset(mode='crowd', team_size=1):
    config = {'title': ('Quiz' if mode == 'quiz' else 'Match the Crowd') + ' · Local test', 'mode': mode, 'team_size': team_size, 'seconds': 60,
              'questions': [{'text': t, 'answers': a} for t, a in QUIZ] if mode == 'quiz' else [{'text': t, 'answers': []} for t in CROWD],
              'participation_template_id': 'local-participation', 'winner_template_id': 'local-winner', 'repeat_participation': True}
    state = q.create_game(config)
    for uid, name in [(9001, 'Ardyn · Test'), (9002, 'Alex · Test')] + BOTS:
        state = q.join_game(state, {'user_id': str(uid), 'display_name': name, 'style': {'color': 'local', 'color_swatch': '#176B67' if uid % 2 else '#3B4B80'}})
    return q.mutate(None, lambda old: state)

seed_packs()
if q.load_state()['stage'] == 'idle':
    preset()

@app.middleware('http')
async def demo_answers(request: Request, call_next):
    action = None
    if request.url.path == '/api/question-game/action' and request.method == 'POST':
        try:
            action = (await request.json()).get('action')
        except Exception:
            pass
    response = await call_next(request)
    if action == 'begin' and response.status_code == 200:
        def fill(s):
            # Make the main tester a captain, so every team preset is easy to try.
            for entry in s['entries']:
                if any(m['user_id'] == '9001' for m in entry['members']):
                    entry['captain_id'] = '9001'
            for entry in s['entries']:
                uid = entry['captain_id']
                if uid not in {str(uid) for uid, _ in BOTS}:
                    continue
                index = int(uid) - 1001
                text = (s['questions'][s['round']-1]['answers'][0] if index % 2 == 0 else 'No idea') if s['mode'] == 'quiz' else SAMPLE[(s['round']-1) % len(SAMPLE)][index]
                q.submit_answer(s, uid, text)
            return s
        q.mutate(None, fill)
    return response

@app.get('/local/start/{mode}')
def start_demo(mode: str):
    if mode not in {'crowd', 'quiz', 'teams'}:
        return JSONResponse({'detail': 'Unknown preset'}, status_code=404)
    preset('quiz' if mode == 'quiz' else 'crowd', 3 if mode == 'teams' else 1)
    return HTMLResponse('<script>location.replace("/host-control.html")</script>')

@app.get('/local-bootstrap.js')
def bootstrap(persona: str = 'host'):
    role = persona if persona in PERSONAS else 'host'
    user = PERSONAS[role]
    token, _ = auth._issue_token(auth.PURPOSE_SESSION, user, 12 * 60 * 60)
    session = {'session_token': token, 'user': auth._public_user(user)}
    script = '''(() => {
const session=SESSION, user=session.user;
localStorage.setItem('alcove_api_base',location.origin+'/api');
localStorage.setItem('alcove_feature_admin_secret',SECRET);
sessionStorage.setItem('alcove_live_room_test_session',JSON.stringify(session));
localStorage.setItem('alcove_tg_user',JSON.stringify(user));
window.Telegram={WebApp:{initDataUnsafe:{user:{...user,id:user.user_id}},initData:'',ready(){},expand(){}}};
const original=window.fetch.bind(window);
window.fetch=(input,options={})=>{
const raw=typeof input==='string'?input:input.url;
let url=new URL(raw,location.href);
if(url.hostname==='alcove-api.onrender.com')url=new URL(url.pathname+url.search,location.origin);
if(url.origin!==location.origin)return Promise.reject(new Error('External API disabled in local preview'));
const headers=new Headers(options.headers||(typeof input!=='string'?input.headers:undefined));
if(url.pathname.startsWith('/api/'))headers.set('Authorization','Bearer '+session.session_token);
return original(url,{...options,headers});
};
})();'''.replace('SESSION', json.dumps(session)).replace('SECRET', json.dumps(os.environ['BOT_SYNC_SECRET']))
    return Response(script, media_type='application/javascript', headers={'Cache-Control': 'no-store'})

def inject(html, persona):
    return html.replace('<head>', '<head><script src="/local-bootstrap.js?persona='+persona+'"></script>', 1)

@app.get('/host-control.html')
def host_page():
    return HTMLResponse(inject((FRONTEND/'host-control.html').read_text(), 'host'))

@app.get('/player.html')
def player_page(persona: str = 'ardyn'):
    persona = persona if persona in {'ardyn', 'alex'} else 'ardyn'
    html = '''<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Local player</title>
<link rel="stylesheet" href="/assets/css/live-feed.css"><link rel="stylesheet" href="/assets/css/activity-stage.css"><link rel="stylesheet" href="/assets/css/question-game.css">
<style>body{margin:0;background:linear-gradient(145deg,#0b2136,#173f4c);color:#fff;font:14px Inter,system-ui,sans-serif}header{display:flex;justify-content:space-between;align-items:center;padding:16px}a{color:#9ddde9}main{max-width:720px;margin:0 auto;padding:12px}#activityDetail{border:1px solid #ffffff30;border-radius:18px;background:#091d2cb3;overflow:hidden}.activity-detail-inner{position:relative;padding:32px 12px 14px}video{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;opacity:.2;pointer-events:none}#activityDetailBody{position:relative}.act-stage{color:white!important}.local-note{font-size:.8rem;color:#bdd5df;line-height:1.5;margin-top:18px}</style></head><body>
<header><strong>PERSONA · Local test</strong><a href="/">Test room</a></header><main><div id="activityNowTitle"></div><div id="activityReserve"><section id="activityDetail"><div class="activity-detail-inner"><video id="activityDetailVideo" muted loop playsinline><source id="activityVideoSource"></video><div id="activityDetailTitle"></div><div id="activityDetailStage"></div><div id="activityDetailCopy"></div><div id="activityDetailSubtitle"></div><div id="activityDetailBody"></div></div></section></div><p class="local-note">Use Host Control in another tab to open questions and reveal groups. Six sample players answer automatically. Alex and Ardyn are manual players. Rewards and messages stay in this test room.</p></main>
<script src="/assets/js/live-room-status.js"></script><script src="/assets/js/activity-stage.js"></script><script src="/assets/js/live-feed.js"></script><script src="/assets/js/question-game-shared.js"></script><script src="/assets/js/question-game-member.js"></script></body></html>'''.replace('PERSONA', 'Ardyn' if persona == 'ardyn' else 'Alex')
    return HTMLResponse(inject(html, persona))

@app.get('/')
def home():
    return HTMLResponse('''<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Alcove game test room</title><style>body{margin:0;background:linear-gradient(130deg,#081d30,#153b43);color:#f6fbff;font:16px/1.55 Inter,system-ui,sans-serif}main{max-width:820px;padding:35px 22px;margin:auto}h1{font-size:34px;line-height:1.15}h2{font-size:19px;margin-top:28px}.tag{color:#8ed8cb;font-size:12px;letter-spacing:.15em}p{color:#bed1dc}.buttons{display:flex;gap:10px;flex-wrap:wrap}a{display:inline-block;text-decoration:none;color:white;border:1px solid #ffffff35;border-radius:12px;padding:12px 16px;background:#ffffff09}.primary{background:#be7031}.card{padding:20px;border:1px solid #ffffff24;border-radius:18px;background:#ffffff05;margin-top:20px}small{color:#bed1dc}</style></head><body><main><div class="tag">THE ALCOVE · LOCAL TEST ROOM</div><h1>Try the question games.</h1><p>This uses the real game engine and interface with separate test data. No messages, prizes or scores reach the live Alcove.</p>
<div class="buttons"><a class="primary" href="/host-control.html" target="_blank" rel="noopener">Open Host Control</a><a href="/player.html?persona=ardyn" target="_blank" rel="noopener">Play as Ardyn</a><a href="/player.html?persona=alex" target="_blank" rel="noopener">Play as Alex</a></div>
<div class="card"><h2>Ready-made games</h2><p>Choose a preset, then click OPEN QUESTION 1 in Host Control. Six sample players answer automatically; close answers whenever you are ready, then reveal each group.</p><div class="buttons"><a href="/local/start/crowd" target="_blank" rel="noopener">Load Match the Crowd</a><a href="/local/start/quiz" target="_blank" rel="noopener">Load Quiz</a><a href="/local/start/teams" target="_blank" rel="noopener">Load teams of three</a></div><p><small>Loading a preset resets only this local game. In team mode Ardyn becomes a captain so you can test submitting and team chat.</small></p></div>
<h2>OBS views</h2><div class="buttons"><a href="/question-game-overlay.html?view=answers" target="_blank" rel="noopener">Answer cards</a><a href="/question-game-overlay.html?view=leaderboard" target="_blank" rel="noopener">Leaderboard</a><a href="/activity-timer-overlay.html" target="_blank" rel="noopener">Timer</a></div><p>To build your own game: Host Control → Builder → Question Game. Other activities and Telegram integration are outside this local harness.</p></main></body></html>''')

@app.api_route('/api/{path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
def unrelated_api(path: str):
    # Supply harmless local defaults for the existing host shell's unrelated panels.
    if path in {'debate/state', 'team-game/state'}:
        return {'status': 'idle'}
    if path == 'state':
        return {'entries': [], 'room_discussion': {'status': 'idle'}, 'modules': {}, 'room_game': None}
    return {'status': 'ok', 'templates': [], 'questions': [], 'entries': [], 'rewards': [], 'items': []}

@app.get('/{page:path}')
def page(page: str):
    file = (FRONTEND / page).resolve()
    if FRONTEND not in file.parents or not file.is_file():
        return JSONResponse({'detail': 'Not included in this local preview'}, status_code=404)
    if file.suffix == '.html':
        return HTMLResponse(inject(file.read_text(), 'host'))
    from fastapi.responses import FileResponse
    return FileResponse(file)

if __name__ == '__main__':
    print(f'Local test room: http://127.0.0.1:{args.port}')
    uvicorn.run(app, host='127.0.0.1', port=args.port)
