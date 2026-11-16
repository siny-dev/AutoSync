"""Firebase Authentication REST session with refresh-token storage in Windows Credential Manager."""
from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


class FirebaseAuthenticationError(RuntimeError):
    pass


class FirebaseAuthenticationTransientError(FirebaseAuthenticationError):
    pass


_FIREBASE_AUTH_DIAGNOSTIC_CODES = frozenset({
    "API_KEY_INVALID",
    "API_KEY_SERVICE_BLOCKED",
    "CREDENTIAL_TOO_OLD_LOGIN_AGAIN",
    "EMAIL_NOT_FOUND",
    "INVALID_GRANT",
    "INVALID_LOGIN_CREDENTIALS",
    "INVALID_PASSWORD",
    "INVALID_REFRESH_TOKEN",
    "OPERATION_NOT_ALLOWED",
    "PROJECT_NOT_FOUND",
    "TOO_MANY_ATTEMPTS_TRY_LATER",
    "USER_DISABLED",
    "USER_NOT_FOUND",
})


def _safe_firebase_auth_http_code(error):
    """Return only a short allow-listed provider code, never its response body."""
    status = int(getattr(error, "code", 0) or 0)
    diagnostic = None
    try:
        body = error.read(8193)
        if len(body) <= 8192:
            payload = json.loads(body.decode("utf-8"))
            detail = payload.get("error") if isinstance(payload, dict) else None
            message = detail.get("message") if isinstance(detail, dict) else detail
            if isinstance(message, str):
                candidate = message.split(":", 1)[0].strip().upper()
                if candidate in _FIREBASE_AUTH_DIAGNOSTIC_CODES:
                    diagnostic = candidate.lower()
    except Exception:
        pass
    finally:
        try:
            error.close()
        except Exception:
            pass
    return "firebase_auth_" + diagnostic if diagnostic else f"firebase_auth_http_{status}"


class FirebaseAuthSession:
    """Returns short-lived Firebase ID tokens without storing the account password."""

    def __init__(self, api_key, refresh_token, credential_target, *, username="",
                 id_token=None, expires_in=0, http_post=None, secret_store=None):
        if not isinstance(api_key, str) or not api_key.strip():
            raise ValueError("Firebase Web API key is required")
        if not isinstance(refresh_token, str) or not refresh_token.strip():
            raise ValueError("Firebase refresh token is required")
        if not isinstance(credential_target, str) or not credential_target.strip():
            raise ValueError("credential_target is required")
        self.api_key = api_key.strip()
        self.refresh_token = refresh_token.strip()
        self.credential_target = credential_target
        self.username = username
        self._id_token = id_token
        self._expires_at = time.time() + max(0, int(expires_in))
        self._lock = threading.RLock()
        self._http_post = http_post or self._post_firebase_auth
        if secret_store is None:
            from .autosync_credentials import read_secret, write_secret
            secret_store = (read_secret, write_secret)
        self._read_secret, self._write_secret = secret_store

    @staticmethod
    def _post_firebase_auth(url, fields):
        """Call only Firebase's documented Auth and Secure Token REST endpoints.

        Identity Toolkit requires a JSON request body, while Secure Token requires
        form encoding. Using form encoding for both silently breaks initial login.
        """
        parsed = urllib.parse.urlsplit(url)
        host = (parsed.hostname or "").lower()
        if host == "identitytoolkit.googleapis.com" and parsed.path.startswith("/v1/accounts:"):
            data = json.dumps(fields, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            content_type = "application/json"
        elif host == "securetoken.googleapis.com" and parsed.path == "/v1/token":
            data = urllib.parse.urlencode(fields).encode("utf-8")
            content_type = "application/x-www-form-urlencoded"
        else:
            raise FirebaseAuthenticationError("firebase_auth_endpoint_not_allowed")
        request = urllib.request.Request(url, data=data,
                                         headers={"Content-Type": content_type,
                                                  "Accept": "application/json",
                                                  "Connection": "close"}, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                body = response.read(262_145)
                if len(body) > 262_144:
                    raise FirebaseAuthenticationError("firebase_auth_response_too_large")
                return response.status, json.loads(body.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            code = exc.code
            diagnostic = _safe_firebase_auth_http_code(exc)
            error = (FirebaseAuthenticationTransientError if code in (408, 425, 429, 500, 502, 503, 504)
                     else FirebaseAuthenticationError)
            raise error(diagnostic) from None
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            raise FirebaseAuthenticationTransientError("firebase_auth_network_error") from None
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise FirebaseAuthenticationError("firebase_auth_invalid_response") from None

    @classmethod
    def sign_in_with_password(cls, api_key, email, password, credential_target,
                              *, http_post=None, secret_store=None):
        """Sign in once; persist only the refresh token, never the password."""
        if not isinstance(email, str) or not email.strip() or not isinstance(password, str) or not password:
            raise ValueError("email and password are required for initial sign-in")
        post = http_post or cls._post_firebase_auth
        url = "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?" + urllib.parse.urlencode({"key": api_key})
        status, result = post(url, {"email": email.strip(), "password": password, "returnSecureToken": True})
        if status in (408, 425, 429, 500, 502, 503, 504):
            raise FirebaseAuthenticationTransientError(f"firebase_auth_http_{status}")
        if status != 200 or not isinstance(result, dict):
            raise FirebaseAuthenticationError("firebase_sign_in_failed")
        id_token = result.get("idToken")
        refresh_token = result.get("refreshToken")
        expires_in = result.get("expiresIn", 0)
        if (not isinstance(id_token, str) or not id_token or not isinstance(refresh_token, str) or
            not refresh_token or not str(expires_in).isdigit()):
            raise FirebaseAuthenticationError("firebase_sign_in_response_incomplete")
        if secret_store is None:
            from .autosync_credentials import read_secret, write_secret
            secret_store = (read_secret, write_secret)
        read_secret, write_secret = secret_store
        write_secret(credential_target, refresh_token, username=email.strip())
        return cls(api_key, refresh_token, credential_target, username=email.strip(),
                   id_token=id_token, expires_in=int(expires_in), http_post=post,
                   secret_store=(read_secret, write_secret))

    @classmethod
    def from_credential_manager(cls, api_key, credential_target, *, username="",
                                http_post=None, secret_store=None):
        if secret_store is None:
            from .autosync_credentials import read_secret, write_secret
            secret_store = (read_secret, write_secret)
        read_secret, write_secret = secret_store
        try:
            refresh_token = read_secret(credential_target)
        except Exception:
            raise FirebaseAuthenticationError("firebase_refresh_token_not_found") from None
        return cls(api_key, refresh_token, credential_target, username=username,
                   http_post=http_post, secret_store=(read_secret, write_secret))

    def get_id_token(self):
        with self._lock:
            if self._id_token and time.time() < self._expires_at - 60:
                return self._id_token
            url = "https://securetoken.googleapis.com/v1/token?" + urllib.parse.urlencode({"key": self.api_key})
            status, result = self._http_post(url, {"grant_type": "refresh_token",
                                                   "refresh_token": self.refresh_token})
            if status in (408, 425, 429, 500, 502, 503, 504):
                self._id_token = None
                raise FirebaseAuthenticationTransientError(f"firebase_auth_http_{status}")
            if status != 200 or not isinstance(result, dict):
                self._id_token = None
                raise FirebaseAuthenticationError("firebase_refresh_failed")
            id_token = result.get("id_token")
            refresh_token = result.get("refresh_token", self.refresh_token)
            expires_in = result.get("expires_in", 0)
            if (not isinstance(id_token, str) or not id_token or not isinstance(refresh_token, str) or
                not refresh_token or not str(expires_in).isdigit()):
                self._id_token = None
                raise FirebaseAuthenticationError("firebase_refresh_response_incomplete")
            if refresh_token != self.refresh_token:
                self._write_secret(self.credential_target, refresh_token, username=self.username or "AutoSync")
                self.refresh_token = refresh_token
            self._id_token = id_token
            self._expires_at = time.time() + int(expires_in)
            return self._id_token
