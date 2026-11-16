"""Run the pinned AutoSync Firebase pair without prompting or exposing credentials.

This is an out-of-package staging helper. It does not change AutoSync 0.1.30.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.util
import json
import re
import sys
from pathlib import Path, PurePosixPath
from types import SimpleNamespace

DEFAULT_CREDENTIALS_FILE = Path(r"\\H255\AI-Work\관리자\Firebase id pw key.txt")
DEFAULT_MODULE_ROOT = Path(
    r"\\H255\AI-Work\제작중인 프로그램\AutoUpdate\review-candidates\20261030"
    r"\S2-AutoSync-0.1.30-review1\package"
)
DEFAULT_DATABASE_URL = (
    "https://autosync-6766d-default-rtdb.asia-southeast1.firebasedatabase.app"
)
CREDENTIAL_TARGET = "AutoSync/autosync-test-spark/shared"
EXPECTED_MANIFEST_SHA256 = "420d4850535f4b3dcdba5146ff0d92e1e552e0de18dc0d75b11f6fcb2ec01cc1"
EXPECTED_VERSION = "0.1.30"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{5,63}$")


class RunnerError(RuntimeError):
    """A sanitized runner failure."""


def read_pair_identity(path: Path | str = DEFAULT_CREDENTIALS_FILE) -> tuple[str, str]:
    """Return only email and API key; never retain or return the password."""
    try:
        lines = Path(path).read_text(encoding="utf-8-sig").splitlines()
    except (OSError, UnicodeError, ValueError):
        raise RunnerError("shared_credential_file_unavailable") from None
    if len(lines) != 3 or any(not line.strip() for line in lines):
        raise RunnerError("shared_credential_file_must_have_three_nonempty_lines")
    email = lines[0].strip()
    password_line = lines[1]
    api_key = lines[2].strip()
    if len(email) > 320 or "@" not in email:
        raise RunnerError("shared_credential_email_invalid")
    if (not password_line.strip() or len(password_line) > 1024
            or _is_placeholder(password_line)):
        raise RunnerError("shared_credential_password_field_invalid")
    if not api_key or len(api_key) > 512 or _is_placeholder(api_key):
        raise RunnerError("shared_credential_api_key_invalid")
    del lines, password_line
    return email, api_key


def _is_placeholder(value: str) -> bool:
    folded = value.casefold()
    return (
        "placeholder" in folded
        or "여기에" in value
        or "입력" in value
        or (value.startswith("<") and value.endswith(">"))
    )


def verify_candidate(module_root: Path | str = DEFAULT_MODULE_ROOT) -> tuple[Path, dict]:
    root = Path(module_root)
    manifest_path = root / "module.json"
    payload_root = root / "payload"
    try:
        manifest_bytes = manifest_path.read_bytes()
        actual_hash = hashlib.sha256(manifest_bytes).hexdigest()
    except OSError:
        raise RunnerError("pinned_candidate_unavailable") from None
    if actual_hash != EXPECTED_MANIFEST_SHA256:
        raise RunnerError("pinned_manifest_hash_mismatch")
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        raise RunnerError("pinned_manifest_invalid") from None
    if (manifest.get("module_id") != "AutoSync"
            or manifest.get("version") != EXPECTED_VERSION
            or not isinstance(manifest.get("files"), list)
            or len(manifest["files"]) != 16):
        raise RunnerError("pinned_candidate_identity_mismatch")

    try:
        resolved_payload = payload_root.resolve(strict=True)
    except OSError:
        raise RunnerError("pinned_payload_unavailable") from None
    for entry in manifest["files"]:
        if not isinstance(entry, dict):
            raise RunnerError("pinned_manifest_entry_invalid")
        relative = entry.get("path")
        source = entry.get("source")
        if not isinstance(relative, str) or not isinstance(source, str):
            raise RunnerError("pinned_manifest_entry_invalid")
        rel_path = PurePosixPath(relative)
        source_path = PurePosixPath(source)
        if (rel_path.is_absolute() or source_path.is_absolute()
                or ".." in rel_path.parts or ".." in source_path.parts
                or not source.startswith("payload/")):
            raise RunnerError("pinned_manifest_path_invalid")
        target = payload_root.joinpath(*rel_path.parts)
        try:
            target_resolved = target.resolve(strict=True)
            if not target_resolved.is_relative_to(resolved_payload):
                raise RunnerError("pinned_manifest_path_invalid")
            data = target_resolved.read_bytes()
        except OSError:
            raise RunnerError("pinned_payload_file_missing") from None
        if (len(data) != entry.get("size")
                or hashlib.sha256(data).hexdigest() != entry.get("sha256")):
            raise RunnerError("pinned_payload_hash_mismatch")
    return resolved_payload, manifest


def _load_pinned_acceptance(payload_root: Path):
    tools_root = payload_root / "tools"
    for name in tuple(sys.modules):
        if (name == "src" or name.startswith("src.")
                or name in ("autosync_firebase_spark_acceptance", "autosync_firebase_two_pc_acceptance")):
            sys.modules.pop(name, None)
    sys.path.insert(0, str(payload_root))
    sys.path.insert(0, str(tools_root))
    tool_path = tools_root / "autosync_firebase_two_pc_acceptance.py"
    spec = importlib.util.spec_from_file_location(
        "pinned_autosync_firebase_two_pc_acceptance", tool_path
    )
    if spec is None or spec.loader is None:
        raise RunnerError("pinned_acceptance_tool_unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(spec.name, None)
        raise RunnerError("pinned_acceptance_tool_load_failed") from None
    return module


def _load_saved_auth_session(api_key: str, email: str, credential_target: str):
    """Use the existing local refresh token only; this runner never prompts."""
    try:
        auth_class = importlib.import_module("src.autosync_firebase_auth").FirebaseAuthSession
        auth = auth_class.from_credential_manager(
            api_key, credential_target, username=email
        )
        auth.get_id_token()
        return auth
    except Exception as exc:
        # Authentication exceptions carry allowlisted diagnostic codes; discard
        # arbitrary messages so no credential-bearing response can reach output.
        code = getattr(exc, "code", "")
        if isinstance(code, str) and re.fullmatch(r"[a-z0-9_]{1,80}", code):
            raise RunnerError(code) from None
        raise RunnerError("saved_firebase_auth_unavailable") from None


def execute(args, *, credentials_file=DEFAULT_CREDENTIALS_FILE,
            module_root=DEFAULT_MODULE_ROOT, database_url=DEFAULT_DATABASE_URL):
    if not RUN_ID_RE.fullmatch(args.run_id):
        raise RunnerError("run_id_must_be_6_to_64_safe_characters")
    payload_root, manifest = verify_candidate(module_root)
    email, api_key = read_pair_identity(credentials_file)
    acceptance = _load_pinned_acceptance(payload_root)

    if args.preflight:
        _load_saved_auth_session(api_key, email, CREDENTIAL_TARGET)
        return {
            "status": "ready",
            "mode": "auth_preflight",
            "candidate_version": EXPECTED_VERSION,
            "manifest_sha256": EXPECTED_MANIFEST_SHA256,
            "role": args.role,
            "run_id": args.run_id,
            "credential_target": CREDENTIAL_TARGET,
            "auth_checked": True,
            "rtdb_accessed": False,
            "writes_performed": False,
        }

    if not args.execute:
        raise RunnerError("explicit_preflight_or_execute_required")

    # Patch only this process's imported staging CLI. An absent Credential
    # Manager target fails closed instead of prompting for or saving passwords.
    acceptance.load_auth_session = _load_saved_auth_session
    run_args = SimpleNamespace(
        role=args.role,
        run_id=args.run_id,
        device_label=args.device_label,
        database_url=database_url,
        api_key=api_key,
        email=email,
        credential_target=CREDENTIAL_TARGET,
        timeout_seconds=args.timeout_seconds,
    )
    return acceptance.run(run_args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Noninteractive AutoSync 0.1.30 Firebase pair runner. "
            "Run H255 peer first, then 5600G initiator with the same run-id."
        )
    )
    parser.add_argument("--role", choices=("peer", "initiator"), required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--device-label", default="")
    parser.add_argument("--timeout-seconds", type=float, default=120)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--preflight", action="store_true",
                       help="verify pinned files and refresh the saved auth token; no RTDB access")
    modes.add_argument("--execute", action="store_true",
                       help="run the bounded synthetic two-PC RTDB acceptance and tombstone its test record")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = execute(args)
    except RunnerError as exc:
        result = {"status": "failed", "errors": [{"code": str(exc)}]}
    except Exception:
        result = {"status": "failed", "errors": [{"code": "runner_failed"}]}
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("status") in ("ready", "passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())