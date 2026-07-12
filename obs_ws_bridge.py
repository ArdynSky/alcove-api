from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
from typing import Any

OBS_HOST = os.getenv("ALCOVE_OBS_HOST", "127.0.0.1")
OBS_PORT = int(os.getenv("ALCOVE_OBS_PORT", "4455"))
OBS_PASSWORD = os.getenv("ALCOVE_OBS_PASSWORD", "")


def _sha256_base64(message: str) -> str:
    digest = hashlib.sha256(message.encode("utf-8")).digest()
    return base64.b64encode(digest).decode("ascii")


async def _obs_request_async(
    request_type: str,
    request_data: dict[str, Any] | None = None,
    *,
    host: str = OBS_HOST,
    port: int = OBS_PORT,
    password: str = OBS_PASSWORD,
) -> dict[str, Any]:
    import websockets

    request_data = request_data or {}
    uri = f"ws://{host}:{port}"
    async with websockets.connect(uri, open_timeout=5, close_timeout=5) as ws:
        hello = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
        if hello.get("op") != 0:
            raise RuntimeError("Unexpected OBS WebSocket hello.")

        auth = hello.get("d", {}).get("authentication")
        authentication = None
        if auth:
            if not password:
                raise RuntimeError("OBS requires a WebSocket password.")
            secret = _sha256_base64(f"{password}{auth['salt']}")
            authentication = _sha256_base64(f"{secret}{auth['challenge']}")

        await ws.send(
            json.dumps(
                {
                    "op": 1,
                    "d": {
                        "rpcVersion": (hello.get("d") or {}).get("rpcVersion") or 1,
                        "authentication": authentication,
                    },
                }
            )
        )
        identified = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
        if identified.get("op") != 2:
            raise RuntimeError("OBS rejected the WebSocket connection.")

        request_id = "alcove-helper-req"
        await ws.send(
            json.dumps(
                {
                    "op": 6,
                    "d": {
                        "requestId": request_id,
                        "requestType": request_type,
                        "requestData": request_data,
                    },
                }
            )
        )

        while True:
            message = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
            if message.get("op") != 7:
                continue
            data = message.get("d") or {}
            if data.get("requestId") != request_id:
                continue
            status = data.get("requestStatus") or {}
            if status.get("result"):
                response = data.get("responseData")
                return response if isinstance(response, dict) else {}
            raise RuntimeError(str(status.get("comment") or "OBS request failed"))


def obs_request(
    request_type: str,
    request_data: dict[str, Any] | None = None,
    *,
    host: str = OBS_HOST,
    port: int = OBS_PORT,
    password: str = OBS_PASSWORD,
) -> dict[str, Any]:
    return asyncio.run(
        _obs_request_async(request_type, request_data, host=host, port=port, password=password)
    )


def obs_status(
    *,
    host: str = OBS_HOST,
    port: int = OBS_PORT,
    password: str = OBS_PASSWORD,
) -> dict[str, Any]:
    try:
        scene_data = obs_request("GetCurrentProgramScene", {}, host=host, port=port, password=password)
        return {
            "connected": True,
            "current_scene": scene_data.get("currentProgramSceneName") or "",
            "host": host,
            "port": port,
        }
    except Exception as exc:
        return {
            "connected": False,
            "message": str(exc),
            "host": host,
            "port": port,
        }
