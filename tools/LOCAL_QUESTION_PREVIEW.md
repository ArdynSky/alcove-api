# Run the question games locally

Requires Python 3.10+ and Git. Check out both repositories side by side:

```sh
git clone --branch codex/live-question-games https://github.com/ArdynSky/alcove-api.git
git clone --branch codex/live-question-games https://github.com/ArdynSky/miniapp_update.git
cd alcove-api
python -m venv .venv
```

Activate the environment:

```sh
# macOS / Linux
source .venv/bin/activate
# Windows PowerShell
.venv\Scripts\Activate.ps1
```

Install the local preview dependencies and start it:

```sh
python -m pip install fastapi uvicorn python-multipart python-dotenv
python tools/local_question_preview.py --frontend ../miniapp_update --port 8765
```

Open **http://127.0.0.1:8765/** in your browser. Keep the terminal running.
The frontend repository is private; use your normal GitHub access when cloning.

1. Open Host Control and Play as Ardyn in separate tabs.
2. Click **OPEN QUESTION 1** in Host Control, then enter an answer as Ardyn.
3. Six sample players answer automatically; Alex is another manual player.
4. Close answers and advance the reveal groups, then show all answers.
5. Try the quiz and teams presets from the landing page. Loading a preset resets
   the local game. Ardyn becomes a captain when a team game begins.
6. Open the answer, leaderboard and timer views to preview the OBS output.

Host Control uses the actual frontend; the player page is a small test shell
around the actual question-game component. This is not a full Telegram or
streaming integration test. Other Host Control activities are stubbed locally.

The launcher binds only to this computer and generates local test credentials.
Game state, chat and reward grants use `.local-question-preview/`, never the
production database. No Render configuration or live secrets are required.
The preview blocks external API fetches. To reset all local data, stop the
server and delete `.local-question-preview/`. Stop the server with Ctrl+C.

## Quiz answer review and winner overlays

New quizzes collect every question before showing answers. After closing the last
question, click START ANSWER REVEAL. For each question, review marking, reveal the
correct players, show incorrect answers, then review the next question. Scores
update during this results phase. Crowd games retain their round-by-round flow.

Separate OBS browser sources are available from Host Control:

- `question-game-overlay.html?view=winners`: customised winner cards with shimmer and twinkles.
- `question-game-overlay.html?view=fireworks`: transparent looping fireworks.
- `question-game-overlay.html?view=confetti`: transparent looping confetti.

All three follow Final Results and disappear when you end the game or start a
new one. Keep each desired source open in OBS; they remain transparent until the
final results. Pack sending is a separate host action. Start a new quiz after
updating to use the deferred answer-review flow; existing games keep their phase.
