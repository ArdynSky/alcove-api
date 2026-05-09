from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent
DEFAULT_CASES = ROOT / "media-test-cases.json"
DEFAULT_API = "https://alcove-api.onrender.com/api"
DEFAULT_HELPER = "http://127.0.0.1:8011/api"


def normalize_base(value: str, suffix: str = "/api") -> str:
    raw = str(value or "").strip().rstrip("/")
    if not raw:
        raise ValueError("Base URL is required")
    return raw if raw.endswith(suffix) else f"{raw}{suffix}"


def request_json(method: str, url: str, payload: dict | None = None, timeout: int = 120) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {"status": "ok"}
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"status": "error", "message": body, "http_status": exc.code}
    except URLError as exc:
        return {"status": "error", "message": str(exc.reason)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


def post(base: str, path: str, payload: dict | None = None, timeout: int = 120) -> dict:
    return request_json("POST", f"{base}{path}", payload or {}, timeout)


def get(base: str, path: str, timeout: int = 60) -> dict:
    return request_json("GET", f"{base}{path}", None, timeout)


def load_cases(path: Path, limit: int | None = None) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    cases = list(data.get("cases") or [])
    if limit:
        cases = cases[:limit]
    return cases


def process_case(case: dict, index: int, run_label: str, api_base: str, helper_base: str, timeout: int) -> dict:
    case_id = case.get("id") or f"case-{index:03d}"
    url = str(case.get("url") or "").strip()
    display_name = f"Batch {run_label}-{index:02d}"
    result = {"case_id": case_id, "url": url, "display_name": display_name}
    if not url:
        result["status"] = "skipped"
        result["message"] = "Missing URL"
        return result

    submit_payload = {
        "telegram_id": int(f"99{run_label}{index:02d}"),
        "username": f"batch_{run_label}_{index:02d}",
        "display_name": display_name,
        "link": url,
        "note": "Batch processing test via Show Runner flow",
        "video_longer_than_5_minutes": False,
        "clip_start_seconds": None,
        "clip_start_label": "",
    }
    submitted = post(api_base, "/wheel-entry", submit_payload, timeout=45)
    result["submit"] = submitted
    if submitted.get("status") != "ok":
        result["status"] = "submit_failed"
        result["message"] = submitted.get("message")
        return result

    entry_id = int(submitted.get("entry_id") or submitted.get("entry", {}).get("id") or 0)
    if not entry_id:
        state = get(api_base, "/app-state", timeout=45)
        matches = [
            entry for entry in state.get("entries", [])
            if entry.get("submitted_url") == url and entry.get("data", {}).get("display_name") == display_name
        ]
        entry_id = int(matches[-1]["id"]) if matches else 0
    result["entry_id"] = entry_id
    if not entry_id:
        result["status"] = "submit_failed"
        result["message"] = "Could not determine entry id"
        return result

    approved = post(api_base, f"/entry/approve/{entry_id}", {}, timeout=45)
    result["approve"] = approved
    if approved.get("status") != "ok":
        result["status"] = "approve_failed"
        result["message"] = approved.get("message")
        return result

    process_payload = {
        "entry_id": entry_id,
        "display_name": display_name,
        "submitted_url": url,
        "video_title": case_id,
        "api_base": api_base,
    }
    processed = post(helper_base, "/downloads/fetch-low-res", process_payload, timeout=timeout)
    result["process"] = processed
    result["status"] = processed.get("status") or "unknown"
    result["message"] = processed.get("message") or processed.get("download_method") or processed.get("stream_support")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", default=str(DEFAULT_CASES))
    parser.add_argument("--api-base", default=DEFAULT_API)
    parser.add_argument("--helper-base", default=DEFAULT_HELPER)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--pause", type=float, default=1.0)
    args = parser.parse_args()

    api_base = normalize_base(args.api_base)
    helper_base = normalize_base(args.helper_base)
    cases = load_cases(Path(args.cases), args.limit or None)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    out_dir = ROOT / "media-test-reports"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"show-runner-batch-{stamp}.json"

    results = []
    print(f"Running {len(cases)} cases through Show Runner flow")
    print(f"API: {api_base}")
    print(f"Helper: {helper_base}")
    for index, case in enumerate(cases, start=1):
        print(f"[{index:02d}/{len(cases):02d}] submit/approve/process {case.get('id')} {case.get('url')}", flush=True)
        item = process_case(case, index, stamp[-6:], api_base, helper_base, args.timeout)
        results.append(item)
        print(f"  -> {item.get('status')}: {item.get('message')}", flush=True)
        time.sleep(args.pause)

    summary = {
        "api_base": api_base,
        "helper_base": helper_base,
        "results": results,
    }
    out_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Report: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
