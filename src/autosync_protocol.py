"""Small protocol primitives shared by the AutoSync client and server."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone


API_VERSION = "1.0"
APP_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")
RECORD_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
OP_RE = re.compile(r"^[A-Za-z0-9._:-]{16,128}$")


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canon(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def check_id(value, pattern):
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValueError("invalid identifier")
    return value
