from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path
from typing import Iterable

MAX_SIDE = 1280
JPEG_QUALITY = 85
WEBP_QUALITY = 80
SKIP_EXTENSIONS = {".gif"}


def _suffix(filename: str) -> str:
    return Path(filename or "").suffix.lower()


def _resized(image, resample):
    width, height = image.size
    longest = max(width, height)
    if longest <= MAX_SIDE:
        return image
    scale = MAX_SIDE / longest
    return image.resize(
        (max(1, round(width * scale)), max(1, round(height * scale))),
        resample,
    )


def _flatten_opaque(image):
    if image.mode not in {"RGBA", "LA"}:
        return image
    if image.getchannel("A").getextrema()[0] < 255:
        return image
    return image.convert("RGB")


def _encode_png(image) -> bytes:
    candidates: list[bytes] = []

    def capture(img) -> None:
        try:
            output = BytesIO()
            img.save(output, format="PNG", optimize=True, compress_level=9)
            encoded = output.getvalue()
            if encoded:
                candidates.append(encoded)
        except Exception:
            return

    working = image
    if working.mode not in {"RGB", "RGBA", "P", "L", "LA"}:
        working = working.convert("RGBA")
    working = _flatten_opaque(working)
    capture(working)

    try:
        from PIL import Image

        if working.mode == "RGBA":
            quantized = working.quantize(colors=256, method=Image.Quantize.FASTOCTREE)
        else:
            quantized = working.convert("RGB").quantize(colors=256, method=Image.Quantize.MEDIANCUT)
        capture(quantized)
    except Exception:
        pass

    return min(candidates, key=len) if candidates else b""


def compress_image_bytes(data: bytes, filename: str = "image.png") -> bytes:
    """Resize and recompress still images. GIFs and unreadable bytes are left alone."""
    if not data:
        return data
    ext = _suffix(filename)
    if ext in SKIP_EXTENSIONS:
        return data
    try:
        from PIL import Image
    except ImportError:
        return data
    try:
        image = Image.open(BytesIO(data))
        image.load()
    except Exception:
        return data

    image = _resized(image, Image.Resampling.LANCZOS)
    output = BytesIO()
    try:
        if ext in {".jpg", ".jpeg"}:
            if image.mode not in {"RGB", "L"}:
                image = image.convert("RGB")
            image.save(output, format="JPEG", quality=JPEG_QUALITY, optimize=True)
            compressed = output.getvalue()
        elif ext == ".webp":
            image.save(output, format="WEBP", quality=WEBP_QUALITY, method=6)
            compressed = output.getvalue()
        else:
            compressed = _encode_png(image)
    except Exception:
        return data

    if not compressed or len(compressed) >= len(data):
        return data
    return compressed


def recompress_image_directory(directory: str | os.PathLike) -> list[dict]:
    folder = Path(directory)
    if not folder.is_dir():
        return []
    changed: list[dict] = []
    for path in sorted(folder.iterdir()):
        if not path.is_file():
            continue
        ext = path.suffix.lower()
        if ext not in {".png", ".jpg", ".jpeg", ".webp"}:
            continue
        try:
            original = path.read_bytes()
        except OSError:
            continue
        compressed = compress_image_bytes(original, path.name)
        if len(compressed) >= len(original):
            continue
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        try:
            tmp_path.write_bytes(compressed)
            os.replace(tmp_path, path)
        except OSError:
            try:
                tmp_path.unlink()
            except OSError:
                pass
            continue
        changed.append(
            {
                "filename": path.name,
                "before_bytes": len(original),
                "after_bytes": len(compressed),
            }
        )
    return changed


def update_manifest_sizes(manifest: dict, changes: Iterable[dict]) -> dict:
    sizes = {item["filename"]: item["after_bytes"] for item in changes}
    if not sizes:
        return manifest
    assets = list(manifest.get("assets") or [])
    for entry in assets:
        name = str(entry.get("filename") or "")
        if name in sizes:
            entry["size_bytes"] = sizes[name]
    manifest["assets"] = assets
    return manifest
