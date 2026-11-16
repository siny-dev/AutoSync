from __future__ import annotations

import hashlib
import re
import tempfile
import unittest
from pathlib import Path

from tools.seed_workboard_ids import MigrationError, _stable_id, seed_file, seed_text


SAMPLE = (
    "# Board title\n"
    "Intro text remains.\n\n"
    "| 순서 | 지시 | 상태 | 남은 일·완료 기준 |\n"
    "|---|---|---|---|\n"
    "| 1 | same text | 20% | keep one |\n"
    "| 2 | same text | 20% | keep one |\n\n"
    "Between tables.\n\n"
    "| 순서 | 업무 | 상태 | 완료 근거 |\n"
    "|---|---|---|---|\n"
    "| 완료 | old task | 완료 | verified |\n"
)


class WorkboardIdSeedTests(unittest.TestCase):
    def test_seeds_each_row_without_rewriting_existing_text(self):
        seeded, counts = seed_text(SAMPLE)
        self.assertEqual(counts, {
            "task_rows": 3,
            "explicit_ids_before": 0,
            "ids_added": 3,
            "ids_after": 3,
        })
        comments = [line for line in seeded.splitlines() if line.startswith("<!-- sync-id: ")]
        ids = [line.removeprefix("<!-- sync-id: ").removesuffix(" -->") for line in comments]
        self.assertEqual(len(ids), 3)
        self.assertEqual(len(set(ids)), 3)
        stripped = re.sub(r"(?m)^<!-- sync-id: [^>]+ -->(?:\r?\n|$)", "", seeded)
        self.assertEqual(stripped, SAMPLE)

        for row in (
            "| 1 | same text | 20% | keep one |",
            "| 2 | same text | 20% | keep one |",
            "| 완료 | old task | 완료 | verified |",
        ):
            self.assertEqual(seeded.count(row), 1)
        self.assertIn("# Board title\nIntro text remains.\n\n", seeded)
        self.assertIn("\n\nBetween tables.\n\n", seeded)

    def test_seeding_is_deterministic_and_idempotent(self):
        first, _ = seed_text(SAMPLE)
        second, counts = seed_text(first)
        self.assertEqual(second, first)
        self.assertEqual(counts["ids_added"], 0)
        self.assertEqual(counts["explicit_ids_before"], 3)

    def test_existing_id_is_preserved_and_duplicate_id_is_rejected(self):
        with_id = SAMPLE.replace("| 1 | same text | 20% | keep one |\n",
                                 "| 1 | same text | 20% | keep one |\n<!-- sync-id: original-1 -->\n", 1)
        seeded, counts = seed_text(with_id)
        self.assertIn("<!-- sync-id: original-1 -->", seeded)
        self.assertEqual(counts["ids_added"], 2)
        duplicated = with_id.replace("| 2 | same text | 20% | keep one |\n",
                                      "| 2 | same text | 20% | keep one |\n<!-- sync-id: original-1 -->\n", 1)
        with self.assertRaisesRegex(MigrationError, "duplicate"):
            seed_text(duplicated)

    def test_generated_id_cannot_collide_with_later_explicit_id(self):
        first_row = "| 1 | first | 20% | first row |"
        reserved_id = _stable_id("workboard", "active", 0, first_row, 1)
        markdown = (
            "| 순서 | 지시 | 상태 | 남은 일·완료 기준 |\n"
            "|---|---|---|---|\n"
            + first_row + "\n"
            "| 2 | second | 20% | second row |\n"
            f"<!-- sync-id: {reserved_id} -->\n"
        )
        with self.assertRaisesRegex(MigrationError, "collides"):
            seed_text(markdown)

    def test_rejects_orphan_id_and_malformed_task_row(self):
        with self.assertRaisesRegex(MigrationError, "orphan"):
            seed_text(SAMPLE.replace("| 1 | same text | 20% | keep one |\n",
                                     "<!-- sync-id: orphan -->\n| 1 | same text | 20% | keep one |\n", 1))
        malformed = SAMPLE.replace("| 2 | same text | 20% | keep one |\n", "| 2 | only three | cells |\n", 1)
        with self.assertRaisesRegex(MigrationError, "four cells"):
            seed_text(malformed)

    def test_file_mode_preserves_utf8_bom_and_crlf(self):
        with tempfile.TemporaryDirectory(prefix="autosync-workboard-seed-eol-") as folder:
            root = Path(folder)
            source = root / "source.md"
            destination = root / "seeded.md"
            original = b"\xef\xbb\xbf" + SAMPLE.replace("\n", "\r\n").encode("utf-8")
            source.write_bytes(original)
            seed_file(source, destination)
            output = destination.read_bytes()
            self.assertTrue(output.startswith(b"\xef\xbb\xbf"))
            body = output[3:].decode("utf-8")
            stripped = re.sub(r"(?m)^<!-- sync-id: [^>]+ -->(?:\r?\n|$)", "", body)
            self.assertEqual(stripped.encode("utf-8"), original[3:])
    def test_file_mode_never_overwrites_source_or_destination(self):
        with tempfile.TemporaryDirectory(prefix="autosync-workboard-seed-") as folder:
            root = Path(folder)
            source = root / "source.md"
            destination = root / "seeded.md"
            source.write_bytes(SAMPLE.encode("utf-8"))
            before_hash = hashlib.sha256(source.read_bytes()).hexdigest()
            report = seed_file(source, destination, dry_run=True)
            self.assertTrue(report["dry_run"])
            self.assertFalse(destination.exists())
            report = seed_file(source, destination)
            self.assertEqual(report["task_rows"], 3)
            self.assertTrue(destination.exists())
            self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(), before_hash)
            with self.assertRaisesRegex(MigrationError, "destination already exists"):
                seed_file(source, destination)
            with self.assertRaisesRegex(MigrationError, "different files"):
                seed_file(source, source.with_suffix(".md"))


if __name__ == "__main__":
    unittest.main()