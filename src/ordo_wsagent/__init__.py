"""Ordo WebSocket client: library + CLI."""

from .protocol import (
    DEFAULT_URL,
    TERMINAL_JOBSTATES,
    jobstate_of,
    is_terminal,
    command_reply_name,
    broadcast_name,
)
from .watches import Watch, WatchRegistry

try:
    from .client import OrdoClient
except ImportError:  # websocket-client not installed
    OrdoClient = None  # type: ignore

try:
    from .async_client import AsyncOrdoClient
except ImportError:  # websockets not installed
    AsyncOrdoClient = None  # type: ignore

__all__ = [
    "DEFAULT_URL",
    "TERMINAL_JOBSTATES",
    "jobstate_of",
    "is_terminal",
    "command_reply_name",
    "broadcast_name",
    "Watch",
    "WatchRegistry",
    "OrdoClient",
    "AsyncOrdoClient",
]
__version__ = "0.3.0"
