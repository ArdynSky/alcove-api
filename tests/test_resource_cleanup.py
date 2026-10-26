import asyncio
import io
import sqlite3
from unittest import mock

import pytest
from fastapi import HTTPException, UploadFile
from fastapi.testclient import TestClient

from api import home_alerts, homepage_settings, main


@pytest.mark.parametrize("operation", ["runtime", "alerts"])
def test_database_connections_close_without_waiting_for_garbage_collection(tmp_path, monkeypatch, operation):
    connections = []
    original_connect = sqlite3.connect

    def connect(*args, **kwargs):
        conn = original_connect(*args, **kwargs)
        connections.append(conn)  # Keep alive so GC cannot hide missing close().
        return conn

    module = main if operation == "runtime" else home_alerts
    monkeypatch.setattr(module, "STATE_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setattr(sqlite3, "connect", connect)
    if operation == "runtime":
        main.save_runtime_state_to_db({"hello": "world"})
        assert main.load_runtime_state_from_db() == {"hello": "world"}
    else:
        home_alerts.push_home_alert(home_alerts.HomeAlertPush(
            user_id="1", type="level_up", event_id="test-level"))
        assert len(home_alerts.list_home_alerts("1", None)["alerts"]) == 1
        assert home_alerts.consume_home_alerts(home_alerts.HomeAlertConsume(user_id="1"))["consumed"] == 1
        assert home_alerts.list_home_alerts("1", None)["alerts"] == []
    assert connections
    for conn in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            conn.execute("SELECT 1")


class ReadTrackingFile(io.BytesIO):
    def __init__(self, data):
        super().__init__(data)
        self.largest_read = 0

    def read(self, size=-1):
        assert size >= 0, "Uploads must not be read into RAM without a limit"
        self.largest_read = max(self.largest_read, size)
        return super().read(size)


@pytest.mark.parametrize("route", ["reward", "banner", "homepage_image", "homepage_video"])
def test_oversize_uploads_use_bounded_reads_and_keep_existing_rejections(monkeypatch, route):
    monkeypatch.setattr(main, "verify_admin_secret", lambda value: None)
    monkeypatch.setattr(homepage_settings, "_admin", lambda value: None)
    monkeypatch.setattr(main, "REWARD_ASSET_MAX_BYTES", 8)
    monkeypatch.setattr(main.fox_messages_store, "MAX_BANNER_BYTES", 8)
    monkeypatch.setattr(homepage_settings, "IMAGE_MAX_BYTES", 8)
    monkeypatch.setattr(homepage_settings, "VIDEO_MAX_BYTES", 8)
    stream = ReadTrackingFile(b"x" * 100)
    upload = UploadFile(file=stream, filename="asset.mp4" if route == "homepage_video" else "asset.png")
    with pytest.raises(HTTPException) as error:
        if route == "reward":
            asyncio.run(main.admin_reward_asset_upload("secret", upload, "", "icon"))
        elif route == "banner":
            with mock.patch.object(main.fox_messages_store, "load_fox_messages_state", return_value={}):
                asyncio.run(main.admin_fox_banner_upload("secret", upload, ""))
        else:
            asyncio.run(homepage_settings.upload_homepage_media("secret", upload, "", ""))
    assert error.value.status_code == 400
    assert "large" in error.value.detail.lower()
    assert stream.largest_read == 9


def test_public_reward_cache_preserves_file_bytes_and_missing_file_handling(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "REWARD_ASSETS_DIR", str(tmp_path))
    path = tmp_path / "reward-1234abcd.png"
    path.write_bytes(b"original-image-bytes")
    client = TestClient(main.app)
    response = client.get("/api/reward-assets/" + path.name)
    assert response.status_code == 200
    assert response.content == path.read_bytes()
    assert response.headers["cache-control"] == "public, max-age=2592000"
    assert response.headers["content-type"] == "image/png"
    assert "etag" in response.headers
    partial = client.get("/api/reward-assets/" + path.name, headers={"Range": "bytes=0-7"})
    assert partial.status_code == 206
    assert partial.content == b"original"
    assert client.get("/api/reward-assets/missing.png").status_code == 404
    assert "cache-control" not in client.get("/api/reward-assets/missing.png").headers


@pytest.mark.parametrize("module_name", ["runtime", "alerts"])
def test_database_connection_closes_when_sql_raises(tmp_path, monkeypatch, module_name):
    connections = []
    original_connect = sqlite3.connect

    class FailingConnection(sqlite3.Connection):
        def execute(self, *args, **kwargs):
            raise sqlite3.OperationalError("simulated database failure")

    def connect(*args, **kwargs):
        conn = original_connect(*args, **kwargs, factory=FailingConnection)
        connections.append(conn)
        return conn

    module = main if module_name == "runtime" else home_alerts
    monkeypatch.setattr(module, "STATE_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setattr(sqlite3, "connect", connect)
    with pytest.raises(sqlite3.OperationalError, match="simulated"):
        (main.ensure_state_store if module_name == "runtime" else home_alerts._ensure_store)()
    for conn in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            conn.cursor()


def test_reward_upload_still_accepts_limit_and_preserves_original_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "verify_admin_secret", lambda value: None)
    monkeypatch.setattr(main, "REWARD_ASSETS_DIR", str(tmp_path))
    monkeypatch.setattr(main, "REWARD_ASSETS_MANIFEST_PATH", str(tmp_path / "manifest.json"))
    monkeypatch.setattr(main, "REWARD_ASSET_MAX_BYTES", 8)
    upload = UploadFile(file=ReadTrackingFile(b"12345678"), filename="reward.png")
    result = asyncio.run(main.admin_reward_asset_upload("secret", upload, "Reward", "icon"))
    assert result["status"] == "ok"
    assert (tmp_path / result["asset"]["filename"]).read_bytes() == b"12345678"
    assert result["asset"]["size_bytes"] == 8
