"""Synchronous Ordo WebSocket client.

Built for scripts and for ordo-bot to wrap. One connection, many watches.
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any, Callable, Dict, List, Optional

from .protocol import DEFAULT_URL
from .watches import Watch, WatchRegistry

try:
    import websocket
except ImportError as exc:  # pragma: no cover
    raise ImportError("pip install websocket-client") from exc


MessageHandler = Callable[[Dict[str, Any]], None]


class OrdoClient:
    def __init__(
        self,
        token: str,
        url: str = DEFAULT_URL,
        *,
        ping_interval: float = 20.0,
        ping_timeout: float = 10.0,
    ) -> None:
        self.token = token
        self.url = url
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.watches = WatchRegistry()
        self.on_message: Optional[MessageHandler] = None
        self.on_watch: Optional[MessageHandler] = None

        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._logged_in = threading.Event()
        self._login_failed = threading.Event()
        self._closed = threading.Event()
        self._lock = threading.Lock()
        self._pending: Dict[str, threading.Event] = {}
        self._replies: Dict[str, Dict[str, Any]] = {}
        self._login_reply: Optional[Dict[str, Any]] = None

    @classmethod
    def from_env(cls) -> "OrdoClient":
        token = os.environ.get("ORDO_TOKEN") or ""
        if not token:
            raise RuntimeError("ORDO_TOKEN is required")
        return cls(token=token, url=os.environ.get("ORDO_WS", DEFAULT_URL))

    @property
    def is_logged_in(self) -> bool:
        return self._logged_in.is_set()

    def connect(self, login_timeout: float = 30.0) -> Dict[str, Any]:
        if self._ws is not None:
            raise RuntimeError("Already connected")

        def on_open(ws: websocket.WebSocketApp) -> None:
            ws.send(json.dumps({"command": "login_user", "token": self.token}))

        def on_message(ws: websocket.WebSocketApp, message: str) -> None:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                return
            if not isinstance(data, dict):
                return
            self._dispatch(data)

        def on_close(ws: websocket.WebSocketApp, code, msg) -> None:
            self._closed.set()
            self._logged_in.clear()

        def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
            pass

        self._ws = websocket.WebSocketApp(
            self.url,
            on_open=on_open,
            on_message=on_message,
            on_close=on_close,
            on_error=on_error,
        )
        self._thread = threading.Thread(
            target=lambda: self._ws.run_forever(
                ping_interval=self.ping_interval, ping_timeout=self.ping_timeout
            ),
            daemon=True,
            name="ordo-ws",
        )
        self._thread.start()
        if not self._logged_in.wait(timeout=login_timeout):
            self.close()
            if self._login_failed.is_set():
                raise RuntimeError("Ordo login failed")
            raise TimeoutError("Ordo login timed out")
        return self._login_reply or {}

    def close(self) -> None:
        self._closed.set()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass
        self._ws = None

    def send_command(
        self, command: Dict[str, Any], *, timeout: float = 60.0, wait: bool = True
    ) -> Optional[Dict[str, Any]]:
        if not self.is_logged_in or self._ws is None:
            raise RuntimeError("Not logged in")
        name = str(command.get("command") or "")
        if not name:
            raise ValueError("command dict must contain 'command'")
        if wait:
            ev = threading.Event()
            with self._lock:
                self._pending[name] = ev
        self._ws.send(json.dumps(command))
        if not wait:
            return None
        if not ev.wait(timeout=timeout):
            with self._lock:
                self._pending.pop(name, None)
            raise TimeoutError(f"Timed out waiting for {name}")
        with self._lock:
            self._pending.pop(name, None)
            return self._replies.pop(name, None)

    def command(self, name: str, **fields: Any) -> Dict[str, Any]:
        reply = self.send_command({"command": name, **fields})
        return reply or {}

    def read_org(self) -> Dict[str, Any]:
        return self.command("read_org")

    def find_cluster(self, name: str = "/root") -> Dict[str, Any]:
        return self.command("find_cluster", name=name)

    def read_cluster(self, cluster_id: int) -> Dict[str, Any]:
        return self.command("read_cluster", id=int(cluster_id))

    def read_job(self, job_id: int) -> Dict[str, Any]:
        return self.command("read_job", id=int(job_id))

    def start_cluster(self, cluster_id: int) -> Dict[str, Any]:
        return self.command("start_cluster", id=int(cluster_id))

    def start_job(self, job_id: int) -> Dict[str, Any]:
        return self.command("start_job", id=int(job_id))

    def watch(
        self,
        kind: str,
        oid: int,
        *,
        jobstate: Optional[str] = None,
        label: str = "",
        snapshot: bool = True,
    ) -> Watch:
        w = self.watches.add(kind, oid, jobstate=jobstate, label=label)
        if snapshot:
            cmd = "read_cluster" if w.kind == "cluster" else "read_job"
            try:
                self.send_command({"command": cmd, "id": int(oid)})
            except Exception:
                pass
        return w

    def watch_cluster(self, cluster_id: int, **kwargs: Any) -> Watch:
        return self.watch("cluster", cluster_id, **kwargs)

    def watch_job(self, job_id: int, **kwargs: Any) -> Watch:
        return self.watch("job", job_id, **kwargs)

    def wait(
        self,
        kind: str,
        oid: int,
        *,
        timeout: float = 120.0,
        jobstate: Optional[str] = None,
    ) -> Dict[str, Any]:
        done = threading.Event()
        holder: List[Dict[str, Any]] = []

        def _on(event: Dict[str, Any]) -> None:
            holder.append(event)
            done.set()

        self.watches.add(kind, oid, jobstate=jobstate, on_fire=_on)
        cmd = "read_cluster" if kind == "cluster" else "read_job"
        self.send_command({"command": cmd, "id": int(oid)})
        if not done.wait(timeout=timeout):
            raise TimeoutError(f"watch {kind} {oid} timed out")
        return holder[0]

    def wait_cluster(self, cluster_id: int, **kwargs: Any) -> Dict[str, Any]:
        return self.wait("cluster", cluster_id, **kwargs)

    def wait_job(self, job_id: int, **kwargs: Any) -> Dict[str, Any]:
        return self.wait("job", job_id, **kwargs)

    def _dispatch(self, data: Dict[str, Any]) -> None:
        reply = data.get("command_reply")
        if reply == "login_user":
            if data.get("success"):
                self._login_reply = data
                self._logged_in.set()
            else:
                self._login_failed.set()
        elif reply:
            with self._lock:
                ev = self._pending.get(str(reply))
                self._replies[str(reply)] = data
            if ev:
                ev.set()

        fired = self.watches.observe_payload(data)
        if self.on_watch:
            for event in fired:
                try:
                    self.on_watch(event)
                except Exception:
                    pass
        if self.on_message:
            try:
                self.on_message(data)
            except Exception:
                pass
