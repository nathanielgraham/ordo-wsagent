"""Shared Ordo protocol helpers.

Terminal detection uses jobstate *names* only. state_id is a server
implementation detail (e.g. 5 == complete) and must not be used by clients.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

DEFAULT_URL = "wss://ordoscheduler.com/websocket"

# Names the server uses when a job or cluster has left live states.
TERMINAL_JOBSTATES = frozenset({"complete", "failed", "zombie", "killed"})

_JOB_BROADCASTS = frozenset(
    {"jobs_changed", "job_changed", "job_updated", "jobs_updated"}
)
_CLUSTER_BROADCASTS = frozenset(
    {"clusters_changed", "cluster_changed", "cluster_updated", "clusters_updated"}
)


def jobstate_of(obj: Optional[Dict[str, Any]]) -> str:
    if not obj or not isinstance(obj, dict):
        return ""
    return str(obj.get("jobstate") or "").strip().lower()


def is_terminal(obj: Optional[Dict[str, Any]]) -> bool:
    """True when jobstate is complete, failed, zombie, or killed."""
    return jobstate_of(obj) in TERMINAL_JOBSTATES


def command_reply_name(data: Optional[Dict[str, Any]]) -> Optional[str]:
    if not data or not isinstance(data, dict):
        return None
    name = data.get("command_reply")
    return str(name) if name else None


def broadcast_name(data: Optional[Dict[str, Any]]) -> Optional[str]:
    if not data or not isinstance(data, dict):
        return None
    name = data.get("broadcast")
    return str(name) if name else None


def broadcast_kind(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    if name in _JOB_BROADCASTS:
        return "job"
    if name in _CLUSTER_BROADCASTS:
        return "cluster"
    return None


def kind_broadcast(kind: str) -> str:
    return "jobs_changed" if kind == "job" else "clusters_changed"
