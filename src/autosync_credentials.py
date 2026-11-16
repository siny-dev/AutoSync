"""Windows Credential Manager helpers for AutoSync app-scoped secrets."""
from __future__ import annotations

import ctypes
import re
import sys
from ctypes import wintypes

if __package__:
    from .autosync_protocol import APP_RE, check_id
else:
    from autosync_protocol import APP_RE, check_id

CRED_TYPE_GENERIC = 1
CRED_PERSIST_LOCAL_MACHINE = 2
ERROR_NOT_FOUND = 1168
MAX_CREDENTIAL_BLOB = 5120
TARGET_RE = re.compile(r"^[A-Za-z0-9._/-]{1,256}$")


class CredentialError(RuntimeError):
    pass


class FILETIME(ctypes.Structure):
    _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]


class CREDENTIALW(ctypes.Structure):
    _fields_ = [
        ("Flags", wintypes.DWORD),
        ("Type", wintypes.DWORD),
        ("TargetName", wintypes.LPWSTR),
        ("Comment", wintypes.LPWSTR),
        ("LastWritten", FILETIME),
        ("CredentialBlobSize", wintypes.DWORD),
        ("CredentialBlob", ctypes.POINTER(ctypes.c_ubyte)),
        ("Persist", wintypes.DWORD),
        ("AttributeCount", wintypes.DWORD),
        ("Attributes", ctypes.c_void_p),
        ("TargetAlias", wintypes.LPWSTR),
        ("UserName", wintypes.LPWSTR),
    ]


def _validate_target(target: str) -> str:
    if not isinstance(target, str) or not TARGET_RE.fullmatch(target) or any(part in (".", "..") for part in target.split("/")):
        raise CredentialError("invalid AutoSync credential target")
    return target


def _wincred():
    if sys.platform != "win32":
        raise CredentialError("Windows Credential Manager is available only on Windows")
    advapi = ctypes.WinDLL("Advapi32.dll", use_last_error=True)
    advapi.CredReadW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
        ctypes.POINTER(ctypes.POINTER(CREDENTIALW)),
    ]
    advapi.CredReadW.restype = wintypes.BOOL
    advapi.CredWriteW.argtypes = [ctypes.POINTER(CREDENTIALW), wintypes.DWORD]
    advapi.CredWriteW.restype = wintypes.BOOL
    advapi.CredDeleteW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD]
    advapi.CredDeleteW.restype = wintypes.BOOL
    advapi.CredFree.argtypes = [ctypes.c_void_p]
    advapi.CredFree.restype = None
    return advapi


def read_secret(target: str) -> str:
    target = _validate_target(target)
    advapi = _wincred()
    pointer = ctypes.POINTER(CREDENTIALW)()
    if not advapi.CredReadW(target, CRED_TYPE_GENERIC, 0, ctypes.byref(pointer)):
        code = ctypes.get_last_error()
        if code == ERROR_NOT_FOUND:
            raise CredentialError("required AutoSync credential is not provisioned")
        raise CredentialError("Windows Credential Manager read failed")
    try:
        item = pointer.contents
        size = int(item.CredentialBlobSize)
        if size < 32 or size > MAX_CREDENTIAL_BLOB or not item.CredentialBlob:
            raise CredentialError("stored AutoSync credential has an invalid size")
        raw = ctypes.string_at(item.CredentialBlob, size)
        try:
            secret = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CredentialError("stored AutoSync credential is not valid UTF-8") from exc
        if len(secret.encode("utf-8")) < 32:
            raise CredentialError("stored AutoSync credential is too short")
        return secret
    finally:
        advapi.CredFree(pointer)


def write_secret(target: str, secret: str, username: str = "AutoSync") -> None:
    target = _validate_target(target)
    if not isinstance(secret, str):
        raise CredentialError("credential must be text")
    raw = secret.encode("utf-8")
    if len(raw) < 32 or len(raw) > MAX_CREDENTIAL_BLOB:
        raise CredentialError("credential must contain 32 to 5120 UTF-8 bytes")
    if not isinstance(username, str) or len(username) > 256:
        raise CredentialError("invalid credential label")
    advapi = _wincred()
    blob = (ctypes.c_ubyte * len(raw)).from_buffer_copy(raw)
    item = CREDENTIALW()
    item.Flags = 0
    item.Type = CRED_TYPE_GENERIC
    item.TargetName = target
    item.Comment = "AutoSync app-scoped credential"
    item.CredentialBlobSize = len(raw)
    item.CredentialBlob = ctypes.cast(blob, ctypes.POINTER(ctypes.c_ubyte))
    item.Persist = CRED_PERSIST_LOCAL_MACHINE
    item.AttributeCount = 0
    item.Attributes = None
    item.TargetAlias = None
    item.UserName = username
    if not advapi.CredWriteW(ctypes.byref(item), 0):
        raise CredentialError("Windows Credential Manager write failed")


def delete_secret(target: str) -> bool:
    target = _validate_target(target)
    advapi = _wincred()
    if advapi.CredDeleteW(target, CRED_TYPE_GENERIC, 0):
        return True
    if ctypes.get_last_error() == ERROR_NOT_FOUND:
        return False
    raise CredentialError("Windows Credential Manager delete failed")


def load_server_tokens(app_ids, target_prefix: str = "AutoSync/server/") -> dict[str, str]:
    if not isinstance(target_prefix, str) or not TARGET_RE.fullmatch(target_prefix + "x"):
        raise CredentialError("invalid server credential target prefix")
    ids = list(app_ids)
    if not ids or len(ids) != len(set(ids)):
        raise CredentialError("server credential app IDs must be nonempty and unique")
    tokens = {}
    for app_id in ids:
        check_id(app_id, APP_RE)
        tokens[app_id] = read_secret(target_prefix + app_id)
    return tokens