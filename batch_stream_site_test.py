from __future__ import annotations

import argparse
import csv
import json
import sys
import zipfile
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

from local_wheel_host_helper import browser_capture_media, inspect_remote_stream, resolve_direct_media


def normalize_domain(url: str) -> str:
    try:
        domain = urlparse(str(url or "").strip()).netloc.lower()
    except Exception:
        return ""
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def load_rows(input_path: Path) -> list[dict[str, str]]:
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        return load_csv_rows(input_path)
    if suffix == ".xlsx":
        return load_xlsx_rows(input_path)
    raise ValueError(f"Unsupported input format: {input_path.suffix}")


def load_csv_rows(input_path: Path) -> list[dict[str, str]]:
    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [{str(k or "").strip(): str(v or "").strip() for k, v in row.items()} for row in reader]


def load_xlsx_rows(input_path: Path) -> list[dict[str, str]]:
    ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rel_ns = {"rel": "http://schemas.openxmlformats.org/package/2006/relationships"}
    office_rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

    with zipfile.ZipFile(input_path) as archive:
        shared_strings = read_shared_strings(archive, ns)
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        workbook_rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))

        first_sheet = workbook.find("main:sheets/main:sheet", ns)
        if first_sheet is None:
            return []

        rel_id = first_sheet.attrib.get(f"{{{office_rel}}}id", "")
        target_map = {
            rel.attrib.get("Id", ""): rel.attrib.get("Target", "")
            for rel in workbook_rels.findall("rel:Relationship", rel_ns)
        }
        sheet_target = target_map.get(rel_id, "")
        if not sheet_target:
            raise ValueError("Could not locate the first worksheet inside the workbook.")

        sheet_path = "xl/" + sheet_target.lstrip("/")
        sheet_xml = ET.fromstring(archive.read(sheet_path))
        rows = parse_sheet_rows(sheet_xml, shared_strings, ns)
        if not rows:
            return []

        headers = [normalize_header(value) for value in rows[0]]
        data_rows: list[dict[str, str]] = []
        for row in rows[1:]:
            padded = row + [""] * max(0, len(headers) - len(row))
            data_rows.append({
                headers[index]: padded[index].strip()
                for index in range(len(headers))
                if headers[index]
            })
        return data_rows


def read_shared_strings(archive: zipfile.ZipFile, ns: dict[str, str]) -> list[str]:
    try:
        xml = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    except KeyError:
        return []

    values: list[str] = []
    for item in xml.findall("main:si", ns):
        chunks = [node.text or "" for node in item.findall(".//main:t", ns)]
        values.append("".join(chunks))
    return values


def parse_sheet_rows(sheet_xml: ET.Element, shared_strings: list[str], ns: dict[str, str]) -> list[list[str]]:
    rows: list[list[str]] = []
    for row in sheet_xml.findall("main:sheetData/main:row", ns):
        values: list[str] = []
        for cell in row.findall("main:c", ns):
            values.append(read_cell_value(cell, shared_strings, ns))
        rows.append(values)
    return rows


def read_cell_value(cell: ET.Element, shared_strings: list[str], ns: dict[str, str]) -> str:
    cell_type = cell.attrib.get("t", "")
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.findall(".//main:t", ns))

    value = cell.findtext("main:v", default="", namespaces=ns)
    if cell_type == "s":
        try:
            return shared_strings[int(value)]
        except Exception:
            return value
    return value or ""


def normalize_header(value: str) -> str:
    return str(value or "").strip().lower().replace(" ", "_")


def status_ok(payload: dict[str, object], *, strategy_key: str = "resolve_strategy") -> str:
    strategy = str(payload.get(strategy_key) or "").strip()
    return f"OK ({strategy})" if strategy else "OK"


def status_error(message: str) -> str:
    clean = " ".join(str(message or "").split())
    return f"Error: {clean}" if clean else "Error"


def parse_error(exc: Exception) -> tuple[str, dict[str, object]]:
    message = str(exc or "").strip()
    extra: dict[str, object] = {}
    if message.startswith("{"):
        try:
            extra = json.loads(message)
            message = str(extra.get("message") or message).strip()
        except Exception:
            extra = {}
    return message or exc.__class__.__name__, extra


def first_present(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = str(row.get(key, "")).strip()
        if value:
            return value
    return ""


def write_results(output_path: Path, rows: Iterable[dict[str, str]], fieldnames: list[str]) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="Batch test stream compatibility for site URLs.")
    parser.add_argument("input", help="CSV or XLSX file with a URL column.")
    parser.add_argument(
        "--output",
        help="Where to write the CSV results. Defaults to '<input>-results.csv'.",
    )
    parser.add_argument(
        "--browser-capture",
        action="store_true",
        help="Also try the browser-capture fallback for rows that fail direct resolve.",
    )
    parser.add_argument(
        "--inspect-stream",
        action="store_true",
        help="Inspect the resolved media URL to see whether the remote stream is directly reachable.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only test the first N populated URLs.",
    )
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve() if args.output else input_path.with_name(f"{input_path.stem}-results.csv")

    source_rows = load_rows(input_path)
    if not source_rows:
        print("No rows found in the input file.", file=sys.stderr)
        return 1

    fieldnames = [
        "site_name",
        "domain",
        "test_url",
        "direct_stream",
        "browser_capture",
        "clip_window_playback",
        "clip_download_test",
        "host_stream_ready",
        "download_fallback",
        "notes",
    ]

    results: list[dict[str, str]] = []
    tested_count = 0
    for row in source_rows:
        test_url = first_present(row, "test_url", "url", "video_url", "link")
        site_name = first_present(row, "site_name", "site", "name") or normalize_domain(test_url) or "Unknown"
        current = {
            "site_name": site_name,
            "domain": normalize_domain(test_url),
            "test_url": test_url,
            "direct_stream": row.get("direct_stream", "") or "Not tested",
            "browser_capture": row.get("browser_capture", "") or "Not tested",
            "clip_window_playback": row.get("clip_window_playback", "") or "Not tested",
            "clip_download_test": row.get("clip_download_test", "") or "Not tested",
            "host_stream_ready": row.get("host_stream_ready", "") or "Not tested",
            "download_fallback": row.get("download_fallback", "") or "Not tested",
            "notes": row.get("notes", "") or "",
        }

        if not test_url:
            current["notes"] = "Missing URL"
            results.append(current)
            continue

        if args.limit and tested_count >= args.limit:
            results.append(current)
            continue
        tested_count += 1

        note_parts: list[str] = []
        resolved_payload: dict[str, object] | None = None

        try:
            resolved_payload = resolve_direct_media(test_url)
            current["direct_stream"] = status_ok(resolved_payload)
            current["host_stream_ready"] = "Yes"
            current["download_fallback"] = "Not needed"
            extractor = str(resolved_payload.get("extractor") or "").strip()
            media_kind = str(resolved_payload.get("media_kind") or "").strip()
            if extractor:
                note_parts.append(f"extractor={extractor}")
            if media_kind:
                note_parts.append(f"media_kind={media_kind}")
        except Exception as exc:
            message, extra = parse_error(exc)
            current["direct_stream"] = status_error(message)
            current["host_stream_ready"] = "Fallback needed"
            current["download_fallback"] = "Recommended"
            failure_class = str(extra.get("failure_class") or "").strip()
            if failure_class:
                note_parts.append(f"direct_failure={failure_class}")

            if args.browser_capture:
                try:
                    captured = browser_capture_media(test_url)
                    current["browser_capture"] = status_ok(captured)
                    current["host_stream_ready"] = "Yes"
                    current["download_fallback"] = "Not needed"
                    resolved_payload = captured
                except Exception as browser_exc:
                    browser_message, _ = parse_error(browser_exc)
                    current["browser_capture"] = status_error(browser_message)
                    note_parts.append(f"browser_capture_failed={browser_message}")

        if args.inspect_stream and resolved_payload and resolved_payload.get("media_url"):
            try:
                inspection = inspect_remote_stream(
                    str(resolved_payload["media_url"]),
                    str(resolved_payload.get("webpage_url") or resolved_payload.get("submitted_url") or ""),
                )
                remote = str(inspection.get("remote") or "").strip()
                content_type = str(inspection.get("content_type") or "").strip()
                if remote:
                    note_parts.append(f"remote={remote}")
                if content_type:
                    note_parts.append(f"content_type={content_type}")
            except Exception as exc:
                note_parts.append(f"inspect_failed={parse_error(exc)[0]}")

        current["notes"] = " | ".join(part for part in note_parts if part)
        results.append(current)
        print(f"[{tested_count}] {site_name}: {current['host_stream_ready']}")

    write_results(output_path, results, fieldnames)
    print(f"Wrote {len(results)} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
