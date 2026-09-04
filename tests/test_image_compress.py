import tempfile
import unittest
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageDraw

from api.image_compress import compress_image_bytes, recompress_image_directory, update_manifest_sizes


def _box_art_png(width: int = 2400, height: int = 1600) -> bytes:
    image = Image.new("RGBA", (width, height), (18, 22, 48, 255))
    draw = ImageDraw.Draw(image)
    for index in range(80):
        draw.ellipse(
            (index * 20, index * 10, 420 + index * 25, 420 + index * 18),
            fill=(index * 3 % 255, 90, 200, 255),
        )
    buf = BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()


def _photo_jpeg(width: int = 2000, height: int = 1400) -> bytes:
    image = Image.new("RGB", (width, height), (12, 80, 160))
    draw = ImageDraw.Draw(image)
    draw.rectangle((40, 40, width - 40, height - 40), fill=(220, 90, 40))
    draw.ellipse((width // 5, height // 5, width * 4 // 5, height * 4 // 5), fill=(40, 180, 120))
    buf = BytesIO()
    image.save(buf, format="JPEG", quality=100)
    return buf.getvalue()


class ImageCompressTests(unittest.TestCase):
    def test_large_png_is_resized_and_smaller(self):
        original = _box_art_png()
        compressed = compress_image_bytes(original, "box-art.png")
        self.assertLess(len(compressed), len(original))
        image = Image.open(BytesIO(compressed))
        self.assertLessEqual(max(image.size), 1280)
        self.assertEqual(image.format, "PNG")

    def test_gif_is_left_alone(self):
        original = b"GIF89a-not-really"
        self.assertEqual(compress_image_bytes(original, "anim.gif"), original)

    def test_large_jpeg_is_resized_and_smaller(self):
        original = _photo_jpeg()
        compressed = compress_image_bytes(original, "photo.jpg")
        self.assertLess(len(compressed), len(original))
        out = Image.open(BytesIO(compressed))
        self.assertLessEqual(max(out.size), 1280)
        self.assertEqual(out.format, "JPEG")

    def test_directory_skips_gifs(self):
        with tempfile.TemporaryDirectory() as folder:
            gif_path = Path(folder) / "loop.gif"
            gif_path.write_bytes(b"GIF89a-not-really")
            changed = recompress_image_directory(folder)
            self.assertEqual(changed, [])
            self.assertEqual(gif_path.read_bytes(), b"GIF89a-not-really")

    def test_directory_recompress_rewrites_files(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "artist-skin.png"
            path.write_bytes(_box_art_png())
            before = path.stat().st_size
            changed = recompress_image_directory(folder)
            self.assertEqual(len(changed), 1)
            self.assertLess(path.stat().st_size, before)

    def test_manifest_sizes_update(self):
        manifest = {"assets": [{"filename": "a.png", "size_bytes": 999}, {"filename": "b.png", "size_bytes": 1}]}
        updated = update_manifest_sizes(
            manifest,
            [{"filename": "a.png", "before_bytes": 999, "after_bytes": 40}],
        )
        self.assertEqual(updated["assets"][0]["size_bytes"], 40)
        self.assertEqual(updated["assets"][1]["size_bytes"], 1)


if __name__ == "__main__":
    unittest.main()
