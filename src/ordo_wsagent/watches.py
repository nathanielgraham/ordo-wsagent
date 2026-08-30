"""Multi-watch registry driven by Ordo broadcasts.

Arm many job/cluster watches. A child job going terminal does not finish
a cluster watch. Matching uses jobstate names only.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .protocol import broadcast_kind, is_terminal, jobstate_of


@dataclass
class Watch:
    kind: str  # "job" or "cluster"
    oid: int
    jobstate: Optional[str] = None  # specific name, or None = any terminal
    label: str = ""
    once: bool = True
    started_at: Optional[int] = None
    snapshot_pending: bool = True
    created: float = field(default_factory=time.time)
    on_fire: Optional[Callable[[Dict[str, Any]], None]] = None


def _id_match(obj: Dict[str, Any], oid: int) -> bool:
    try:
        return int(obj.get("id")) == int(oid)
    except (TypeError, ValueError):
        return False


def _state_match(obj: Dict[str, Any], watch: Watch) -> bool:
    if watch.jobstate:
        return jobstate_of(obj) == str(watch.jobstate).strip().lower()
    return is_terminal(obj)


def _started_ok(obj: Dict[str, Any], watch: Watch) -> bool:
    if watch.started_at is None or obj.get("started") is None:
        return True
    try:
        return int(obj["started"]) >= int(watch.started_at)
    except (TypeError, ValueError):
        return True


class WatchRegistry:
    def __init__(self) -> None:
        self._watches: List[Watch] = []

    def __len__(self) -> int:
        return len(self._watches)

    def add(
        self,
        kind: str,
        oid: int,
        *,
        jobstate: Optional[str] = None,
        label: str = "",
        once: bool = True,
        on_fire: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Watch:
        kind = "job" if kind == "job" else "cluster"
        w = Watch(
            kind=kind,
            oid=int(oid),
            jobstate=(str(jobstate).strip().lower() if jobstate else None),
            label=label or f"{kind} {oid}",
            once=once,
            on_fire=on_fire,
        )
        self._watches.append(w)
        return w

    def note_start(self, kind: str, oid: int, started_at: Any) -> None:
        if started_at is None:
            return
        try:
            started = int(started_at)
        except (TypeError, ValueError):
            return
        for w in self._watches:
            if w.kind == kind and w.oid == int(oid) and w.started_at is None:
                w.started_at = started

    def match_snapshot(self, kind: str, obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(obj, dict):
            return []
        fired: List[Dict[str, Any]] = []
        remaining: List[Watch] = []
        for w in self._watches:
            if w.kind != kind or not w.snapshot_pending:
                remaining.append(w)
                continue
            w.snapshot_pending = False
            if _id_match(obj, w.oid) and _state_match(obj, w) and _started_ok(obj, w):
                fired.append(self._fire(w, obj, source="snapshot"))
                if not w.once:
                    remaining.append(w)
            else:
                remaining.append(w)
        self._watches = remaining
        return fired

    def match_broadcast(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        kind = broadcast_kind(data.get("broadcast") if isinstance(data, dict) else None)
        if not kind:
            return []
        updates = data.get("updates") or []
        if not isinstance(updates, list):
            updates = []
        fired: List[Dict[str, Any]] = []
        remaining: List[Watch] = []
        for w in self._watches:
            if w.kind != kind:
                remaining.append(w)
                continue
            matched = None
            for row in updates:
                if (
                    isinstance(row, dict)
                    and _id_match(row, w.oid)
                    and _state_match(row, w)
                    and _started_ok(row, w)
                ):
                    matched = row
                    break
            if matched is None:
                remaining.append(w)
                continue
            fired.append(self._fire(w, matched, source="broadcast"))
            if not w.once:
                remaining.append(w)
        self._watches = remaining
        return fired

    def observe_payload(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(data, dict):
            return []
        reply = data.get("command_reply")
        if reply in {"start_cluster", "start_job"} and data.get("success"):
            kind = "cluster" if reply == "start_cluster" else "job"
            oid = data.get("cluster_id") if kind == "cluster" else data.get("job_id")
            if oid is None:
                oid = data.get("id")
            if oid is not None:
                self.note_start(kind, int(oid), data.get("started_at") or data.get("started"))
        if reply == "read_cluster":
            return self.match_snapshot("cluster", data)
        if reply == "read_job":
            return self.match_snapshot("job", data)
        if data.get("broadcast"):
            return self.match_broadcast(data)
        return []

    def clear(self) -> int:
        n = len(self._watches)
        self._watches.clear()
        return n

    def _fire(self, watch: Watch, obj: Dict[str, Any], source: str) -> Dict[str, Any]:
        event = {
            "event": "watch_done",
            "kind": watch.kind,
            "id": watch.oid,
            "label": watch.label,
            "jobstate": jobstate_of(obj) or obj.get("jobstate"),
            "name": obj.get("name"),
            "started": obj.get("started"),
            "ended": obj.get("ended"),
            "exit_code": obj.get("exit_code"),
            "source": source,
            "object": obj,
        }
        if watch.on_fire:
            watch.on_fire(event)
        return event
