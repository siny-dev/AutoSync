"""Create a separate Workboard Markdown copy with persistent row IDs.

The source is read-only. The destination must not exist. This utility is a
migration aid; it does not sync, publish, or modify a production Workboard file.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path


ID_LINE = re.compile(r"^\s*<!--\s*sync-id:\s*([A-Za-z0-9][A-Za-z0-9._:-]{0,127})\s*-->\s*$")
TABLE_LINE = re.compile(r"^\s*\|.*\|\s*$")
SEPARATOR_CELL = re.compile(r"^:?-{3,}:?$")


class MigrationError(ValueError):
    pass


def line_body(line: str) -> str:
    return line[:-2] if line.endswith("\r\n") else line[:-1] if line.endswith(("\r", "\n")) else line


def table_cells(line: str) -> list[str]:
    text = line.strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|"):
        text = text[:-1]
    return [cell.strip() for cell in re.split(r"(?<!\\)\|", text)]


def is_separator(line: str) -> bool:
    if not TABLE_LINE.fullmatch(line):
        return False
    cells = table_cells(line)
    return bool(cells) and all(SEPARATOR_CELL.fullmatch(cell) for cell in cells)


def find_tables(lines: list[str]) -> list[tuple[int, int, int, str]]:
    tables: list[tuple[int, int, int, str]] = []
    i = 0
    while i + 1 < len(lines):
        header = table_cells(line_body(lines[i]))
        if (
            len(header) == 4
            and header[0] == "순서"
            and header[1] in ("지시", "업무")
            and is_separator(line_body(lines[i + 1]))
        ):
            section = "completed" if re.search(r"완료\s*근거", header[3]) else "active"
            end = i + 2
            while end < len(lines):
                body = line_body(lines[end])
                if TABLE_LINE.fullmatch(body) or ID_LINE.fullmatch(body):
                    end += 1
                    continue
                break
            tables.append((i, i + 1, end, section))
            i = end
        else:
            i += 1
    return tables


def _stable_id(app_id: str, section: str, position: int, row: str, occurrence: int) -> str:
    material = "\0".join((app_id, section, str(position), row, str(occurrence))).encode("utf-8")
    return "wb-" + hashlib.sha256(material).hexdigest()[:32]


def seed_text(markdown: str, app_id: str = "workboard") -> tuple[str, dict[str, int]]:
    if not isinstance(markdown, str) or not isinstance(app_id, str) or not re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", app_id):
        raise MigrationError("invalid markdown or app_id")
    if "\x00" in markdown:
        raise MigrationError("Markdown contains a NUL character")
    lines = markdown.splitlines(keepends=True)
    if not lines:
        raise MigrationError("Markdown is empty")
    bodies = [line_body(line) for line in lines]
    tables = find_tables(bodies)
    if not tables:
        raise MigrationError("no four-column Workboard task table was found")

    # Reserve persisted IDs first. This keeps reruns idempotent and prevents
    # generated IDs from colliding with explicit IDs later in the document.
    existing_ids: set[str] = set()
    for _, separator_index, end_index, _ in tables:
        index = separator_index + 1
        while index < end_index:
            body = bodies[index]
            if ID_LINE.fullmatch(body):
                raise MigrationError("orphan sync-id comment found")
            if not TABLE_LINE.fullmatch(body) or is_separator(body):
                index += 1
                continue
            if index + 1 < end_index:
                match = ID_LINE.fullmatch(bodies[index + 1])
                if match:
                    explicit_id = match.group(1)
                    if explicit_id in existing_ids:
                        raise MigrationError("duplicate sync-id found")
                    existing_ids.add(explicit_id)
                    index += 1
            index += 1

    insert_after: dict[int, str] = {}
    used_ids = set(existing_ids)
    record_count = 0
    explicit_count = 0

    for header_index, separator_index, end_index, section in tables:
        position = 0
        occurrences: dict[str, int] = {}
        index = separator_index + 1
        while index < end_index:
            body = bodies[index]
            if ID_LINE.fullmatch(body):
                raise MigrationError("orphan sync-id comment found")
            if not TABLE_LINE.fullmatch(body) or is_separator(body):
                index += 1
                continue
            cells = table_cells(body)
            if len(cells) != 4:
                raise MigrationError("every task row must have exactly four cells")
            fingerprint = hashlib.sha256(json.dumps(cells, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()
            occurrences[fingerprint] = occurrences.get(fingerprint, 0) + 1
            stable_id = _stable_id(app_id, section, position, body, occurrences[fingerprint])
            has_following_id = index + 1 < end_index and ID_LINE.fullmatch(bodies[index + 1])
            if not has_following_id:
                if stable_id in used_ids:
                    raise MigrationError("generated sync-id collides with an existing ID")
                used_ids.add(stable_id)
            record_count += 1
            position += 1

            if has_following_id:
                explicit_count += 1
                index += 1
            else:
                insert_after[index] = stable_id
            index += 1
    if record_count == 0:
        raise MigrationError("Workboard tables contain no task rows")

    output: list[str] = []
    default_newline = "\r\n" if "\r\n" in markdown else "\n"
    for index, line in enumerate(lines):
        output.append(line)
        stable_id = insert_after.get(index)
        if stable_id is None:
            continue
        ending = "\r\n" if line.endswith("\r\n") else "\r" if line.endswith("\r") else "\n" if line.endswith("\n") else default_newline
        final_without_newline = index == len(lines) - 1 and not line.endswith(("\r", "\n"))
        output.append(f"<!-- sync-id: {stable_id} -->" + ("" if final_without_newline else ending))
    return "".join(output), {
        "task_rows": record_count,
        "explicit_ids_before": explicit_count,
        "ids_added": len(insert_after),
        "ids_after": explicit_count + len(insert_after),
    }


def seed_file(source: Path, destination: Path, app_id: str = "workboard", dry_run: bool = False) -> dict[str, object]:
    source = source.expanduser().resolve(strict=True)
    destination = destination.expanduser().absolute()
    if source == destination.resolve(strict=False):
        raise MigrationError("source and destination must be different files")
    if destination.exists():
        raise MigrationError("destination already exists; refusing overwrite")
    if not destination.parent.is_dir():
        raise MigrationError("destination parent directory must already exist")

    source_bytes = source.read_bytes()
    source_hash = hashlib.sha256(source_bytes).hexdigest()
    if source_bytes.startswith(b"\xef\xbb\xbf"):
        text = source_bytes[3:].decode("utf-8")
        bom = b"\xef\xbb\xbf"
    else:
        text = source_bytes.decode("utf-8")
        bom = b""
    seeded, counts = seed_text(text, app_id)
    output_bytes = bom + seeded.encode("utf-8")
    if not dry_run:
        if hashlib.sha256(source.read_bytes()).hexdigest() != source_hash:
            raise MigrationError("source changed during migration; refusing output")
        with destination.open("xb") as stream:
            stream.write(output_bytes)
            stream.flush()
            os.fsync(stream.fileno())
        if hashlib.sha256(destination.read_bytes()).digest() != hashlib.sha256(output_bytes).digest():
            raise MigrationError("destination verification failed")
    return {
        "source": str(source),
        "destination": None if dry_run else str(destination),
        "dry_run": dry_run,
        "source_sha256": source_hash,
        "output_sha256": hashlib.sha256(output_bytes).hexdigest(),
        **counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a separate Workboard Markdown copy with persistent sync IDs.")
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--app-id", default="workboard")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(seed_file(args.source, args.destination, args.app_id, args.dry_run), ensure_ascii=False))
        return 0
    except (OSError, UnicodeError, MigrationError) as exc:
        print(json.dumps({"status": "refused", "reason": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())