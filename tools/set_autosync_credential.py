"""Provision or remove one AutoSync token in the current Windows user's Credential Manager."""
from __future__ import annotations

import argparse
import getpass
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.autosync_credentials import CredentialError, delete_secret, write_secret


def main() -> int:
    parser = argparse.ArgumentParser(description="Store an AutoSync token in Windows Credential Manager.")
    sub = parser.add_subparsers(dest="action", required=True)
    set_parser = sub.add_parser("set", help="store or rotate a token")
    set_parser.add_argument("--target", required=True, help="e.g. AutoSync/server/autosync-pilot-probe")
    set_parser.add_argument("--generate", action="store_true", help="generate a random token and display it once")
    delete_parser = sub.add_parser("delete", help="delete a token")
    delete_parser.add_argument("--target", required=True)
    args = parser.parse_args()

    try:
        if args.action == "delete":
            removed = delete_secret(args.target)
            print("deleted" if removed else "not-found")
            return 0
        token = secrets.token_urlsafe(48) if args.generate else getpass.getpass("AutoSync token (input hidden): ")
        write_secret(args.target, token)
        print(f"stored: {args.target}")
        if args.generate:
            print("Copy this token once to the corresponding client credential target:")
            print(token)
        else:
            print("The token was stored; it was not printed.")
        return 0
    except (CredentialError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())