from __future__ import annotations

import io
import json
import urllib.error
import sys
import unittest
import urllib.parse
from pathlib import Path
from unittest.mock import patch

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT))

from src.autosync_firebase_auth import (
    FirebaseAuthenticationError,
    FirebaseAuthenticationTransientError,
    FirebaseAuthSession,
)


class FirebaseAuthSessionTests(unittest.TestCase):
    def setUp(self):
        self.writes = []
        self.secrets = (
            lambda _target: "stored-refresh-token",
            lambda target, value, username="": self.writes.append((target, value, username)),
        )

    def test_password_sign_in_persists_refresh_token_only(self):
        calls = []

        def post(url, fields):
            calls.append((url, dict(fields)))
            return 200, {"idToken": "id-token", "refreshToken": "refresh-token-new", "expiresIn": "3600"}

        session = FirebaseAuthSession.sign_in_with_password(
            "public-api-key", " person@example.test ", "typed-password", "AutoSync/test/app",
            http_post=post, secret_store=self.secrets,
        )

        self.assertEqual(session.get_id_token(), "id-token")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][1]["email"], "person@example.test")
        self.assertEqual(calls[0][1]["password"], "typed-password")
        self.assertEqual(self.writes, [("AutoSync/test/app", "refresh-token-new", "person@example.test")])
        self.assertNotIn("typed-password", repr(self.writes))

    def test_refresh_rotates_saved_token_and_reuses_fresh_id_token(self):
        calls = []

        def post(url, fields):
            calls.append((url, dict(fields)))
            return 200, {"id_token": "new-id-token", "refresh_token": "rotated-refresh-token", "expires_in": "3600"}

        session = FirebaseAuthSession(
            "public-api-key", "old-refresh-token", "AutoSync/test/app", username="person@example.test",
            http_post=post, secret_store=self.secrets,
        )

        self.assertEqual(session.get_id_token(), "new-id-token")
        self.assertEqual(session.get_id_token(), "new-id-token")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][1]["refresh_token"], "old-refresh-token")
        self.assertEqual(self.writes, [("AutoSync/test/app", "rotated-refresh-token", "person@example.test")])

    def test_transient_refresh_failure_is_retryable_and_does_not_overwrite_secret(self):
        def post(_url, _fields):
            return 503, {"error": "temporarily unavailable"}

        session = FirebaseAuthSession(
            "public-api-key", "stored-refresh-token", "AutoSync/test/app",
            http_post=post, secret_store=self.secrets,
        )

        with self.assertRaises(FirebaseAuthenticationTransientError) as raised:
            session.get_id_token()
        self.assertEqual(str(raised.exception), "firebase_auth_http_503")
        self.assertEqual(self.writes, [])

    def test_invalid_refresh_response_is_rejected_without_exposing_response_body(self):
        def post(_url, _fields):
            return 200, {"error": "private diagnostic", "id_token": "", "refresh_token": "", "expires_in": "?"}

        session = FirebaseAuthSession(
            "public-api-key", "stored-refresh-token", "AutoSync/test/app",
            http_post=post, secret_store=self.secrets,
        )

        with self.assertRaises(FirebaseAuthenticationError) as raised:
            session.get_id_token()
        self.assertEqual(str(raised.exception), "firebase_refresh_response_incomplete")
        self.assertNotIn("private diagnostic", str(raised.exception))
        self.assertEqual(self.writes, [])

    def test_http_auth_errors_expose_only_allowlisted_diagnostic_codes(self):
        cases = (
            ("INVALID_GRANT", "firebase_auth_invalid_grant"),
            ("API_KEY_INVALID", "firebase_auth_api_key_invalid"),
            ("unknown secret value", "firebase_auth_http_400"),
        )
        for provider_message, expected in cases:
            with self.subTest(provider_message=provider_message):
                body = json.dumps({"error": {"code": 400, "message": provider_message}}).encode("utf-8")
                http_error = urllib.error.HTTPError(
                    "https://securetoken.googleapis.com/v1/token", 400, "Bad Request", {}, io.BytesIO(body),
                )
                with patch("src.autosync_firebase_auth.urllib.request.urlopen", side_effect=http_error):
                    with self.assertRaises(FirebaseAuthenticationError) as raised:
                        FirebaseAuthSession._post_firebase_auth(
                            "https://securetoken.googleapis.com/v1/token?key=public-api-key",
                            {"grant_type": "refresh_token", "refresh_token": "secret-refresh-token"},
                        )
                self.assertEqual(str(raised.exception), expected)
                self.assertNotIn(provider_message, str(raised.exception))
                self.assertNotIn("secret-refresh-token", str(raised.exception))

    def test_invalid_refresh_token_does_not_replace_credential_manager_secret(self):
        existing = {"AutoSync/test/app": "stored-refresh-token"}
        writes = []

        def read_secret(target):
            return existing[target]

        def write_secret(target, value, username=""):
            writes.append((target, value, username))
            existing[target] = value

        body = json.dumps({"error": {"code": 400, "message": "INVALID_REFRESH_TOKEN"}}).encode("utf-8")
        http_error = urllib.error.HTTPError(
            "https://securetoken.googleapis.com/v1/token", 400, "Bad Request", {}, io.BytesIO(body),
        )
        with patch("src.autosync_firebase_auth.urllib.request.urlopen", side_effect=http_error):
            session = FirebaseAuthSession.from_credential_manager(
                "public-api-key", "AutoSync/test/app", username="person@example.test",
                secret_store=(read_secret, write_secret),
            )
            with self.assertRaises(FirebaseAuthenticationError) as raised:
                session.get_id_token()

        self.assertEqual(str(raised.exception), "firebase_auth_invalid_refresh_token")
        self.assertEqual(existing["AutoSync/test/app"], "stored-refresh-token")
        self.assertEqual(writes, [])
    def test_sign_in_failure_does_not_persist_any_credential(self):
        def post(_url, _fields):
            return 400, {"error": {"message": "EMAIL_NOT_FOUND"}}

        with self.assertRaises(FirebaseAuthenticationError) as raised:
            FirebaseAuthSession.sign_in_with_password(
                "public-api-key", "person@example.test", "typed-password", "AutoSync/test/app",
                http_post=post, secret_store=self.secrets,
            )
        self.assertEqual(str(raised.exception), "firebase_sign_in_failed")
        self.assertEqual(self.writes, [])

    def test_rest_auth_endpoints_use_their_required_encodings(self):
        class _Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _size):
                return b'{"ok":true}'

        def check_request(url, fields, expected_type):
            captured = {}

            def fake_urlopen(request, timeout):
                captured["method"] = request.get_method()
                captured["content_type"] = request.get_header("Content-type")
                captured["body"] = request.data
                captured["timeout"] = timeout
                return _Response()

            with patch("src.autosync_firebase_auth.urllib.request.urlopen", fake_urlopen):
                status, result = FirebaseAuthSession._post_firebase_auth(url, fields)
            self.assertEqual(status, 200)
            self.assertEqual(result, {"ok": True})
            self.assertEqual(captured["method"], "POST")
            self.assertEqual(captured["content_type"], expected_type)
            self.assertGreater(captured["timeout"], 0)
            return captured["body"]

        sign_in_body = check_request(
            "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=public-api-key",
            {"email": "person@example.test", "password": "typed-password", "returnSecureToken": True},
            "application/json",
        )
        self.assertEqual(json.loads(sign_in_body), {
            "email": "person@example.test", "password": "typed-password", "returnSecureToken": True,
        })

        refresh_body = check_request(
            "https://securetoken.googleapis.com/v1/token?key=public-api-key",
            {"grant_type": "refresh_token", "refresh_token": "refresh-token"},
            "application/x-www-form-urlencoded",
        )
        self.assertEqual(urllib.parse.parse_qs(refresh_body.decode("ascii")), {
            "grant_type": ["refresh_token"], "refresh_token": ["refresh-token"],
        })


if __name__ == "__main__":
    unittest.main()