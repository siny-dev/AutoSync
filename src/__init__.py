"""Shared AutoSync client module (pilot API; production rollout remains gated)."""

__version__ = "0.1.31"
from .autosync_client import AutoSyncClient, SyncRejected
from .autosync_firebase_rtdb import FirebaseRtdbClient
from .autosync_firebase_auth import FirebaseAuthSession

__all__ = ["AutoSyncClient", "FirebaseRtdbClient", "FirebaseAuthSession", "SyncRejected", "__version__"]
