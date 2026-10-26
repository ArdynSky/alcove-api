import asyncio
import io

import pytest
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.testclient import TestClient
from starlette.datastructures import Headers

from api import debate_games, homepage_settings, team_games


@pytest.fixture(params=[debate_games, team_games])
def activity(request, tmp_path, monkeypatch):
    module = request.param
    monkeypatch.setattr(module, 'DB_PATH', tmp_path / 'state.sqlite3')
    monkeypatch.setattr(module, 'BACKGROUND_DIR', tmp_path)
    return module


@pytest.mark.parametrize('extension,mime', [('png', 'image/png'), ('jpg', 'image/jpeg'), ('jpeg', 'image/jpeg'), ('webp', 'image/webp'), ('mp4', 'video/mp4'), ('webm', 'video/webm')])
def test_activity_background_preserves_uploaded_media(activity, extension, mime):
    upload = UploadFile(file=io.BytesIO(b'original-media-bytes'), filename='scene.' + extension, headers=Headers({'content-type': mime}))
    handler = activity.background_upload if activity is debate_games else activity.team_game_background
    result = asyncio.run(handler(upload))
    filename = result['background_url'].rsplit('/', 1)[1]
    assert filename.endswith('.' + extension)
    app = FastAPI()
    app.include_router(activity.router)
    response = TestClient(app).get(result['background_url'])
    assert response.status_code == 200
    assert response.content == b'original-media-bytes'
    assert response.headers['content-type'] == mime
    assert response.headers['cache-control'] == 'public, max-age=2592000'


class OversizeStream:
    def read(self, size=-1):
        assert size == 40 * 1024 * 1024 + 1
        return b'x' * size


def test_activity_oversize_background_reads_are_bounded(activity):
    upload = UploadFile(file=OversizeStream(), filename='scene.mp4')
    handler = activity.background_upload if activity is debate_games else activity.team_game_background
    with pytest.raises(HTTPException) as error:
        asyncio.run(handler(upload))
    assert error.value.status_code == 413
    assert list(activity.BACKGROUND_DIR.glob('*.mp4')) == []


@pytest.mark.parametrize('kind', ['homepage', 'team_upload'])
def test_public_media_cache_preserves_bytes_and_missing_response(tmp_path, monkeypatch, kind):
    app = FastAPI()
    if kind == 'homepage':
        monkeypatch.setattr(homepage_settings, '_media_dir', lambda: tmp_path)
        monkeypatch.setattr(homepage_settings, 'ensure_homepage_media_optimized', lambda: None)
        app.include_router(homepage_settings.router)
        prefix = '/api/homepage-media/'
    else:
        monkeypatch.setattr(team_games, 'UPLOAD_DIR', tmp_path)
        app.include_router(team_games.router)
        prefix = '/api/team-game/media/'
    (tmp_path / 'asset-1234abcd.webp').write_bytes(b'original-webp')
    client = TestClient(app)
    response = client.get(prefix + 'asset-1234abcd.webp')
    assert response.content == b'original-webp'
    assert response.headers['content-type'] == 'image/webp'
    assert response.headers['cache-control'] == 'public, max-age=2592000'
    missing = client.get(prefix + 'missing.webp')
    assert missing.status_code == 404
    assert 'cache-control' not in missing.headers


def test_activity_rejects_unsupported_background(activity):
    upload = UploadFile(file=io.BytesIO(b'svg'), filename='scene.svg', headers=Headers({'content-type': 'image/svg+xml'}))
    handler = activity.background_upload if activity is debate_games else activity.team_game_background
    with pytest.raises(HTTPException) as error:
        asyncio.run(handler(upload))
    assert error.value.status_code == 400
    assert list(activity.BACKGROUND_DIR.glob('*.svg')) == []


def test_activity_background_uses_mime_when_extension_is_unknown(activity):
    upload = UploadFile(file=io.BytesIO(b'webp'), filename='scene.bin', headers=Headers({'content-type': 'image/webp'}))
    handler = activity.background_upload if activity is debate_games else activity.team_game_background
    result = asyncio.run(handler(upload))
    assert result['background_url'].endswith('.webp')


def test_activity_missing_background_is_not_cached(activity):
    app = FastAPI()
    app.include_router(activity.router)
    prefix = '/api/debate' if activity is debate_games else '/api/team-game'
    response = TestClient(app).get(prefix + '/background-media/missing.png')
    assert response.status_code == 404
    assert 'cache-control' not in response.headers
