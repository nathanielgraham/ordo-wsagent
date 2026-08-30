"""Async Ordo WebSocket client (websockets).

Same protocol and WatchRegistry as the sync OrdoClient. The caller
decides when to close(). Requires: pip install 'ordo-wsagent[async]'
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

from .protocol import DEFAULT_URL
from .watches import Watch, WatchRegistry

try:
    import websockets
except ImportError as exc:  # pragma: no cover
    raise ImportError("pip install 'ordo-wsagent[async]'") from exc


MessageHandler = Callable[[Dict[str, Any]], Union[None, Awaitable[None]]]
Pending = Tuple[int, str, Any, asyncio.Future]


class AsyncOrdoClient:
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

        self._ws: Any = None
        self._reader: Optional[asyncio.Task] = None
        self._logged_in = asyncio.Event()
        self._login_failed = asyncio.Event()
        self._closed = asyncio.Event()
        self._pending: List[Pending] = []
        self._wait_seq = 0
        self._login_reply: Optional[Dict[str, Any]] = None

    @classmethod
    def from_env(cls) -> "AsyncOrdoClient":
        token = os.environ.get("ORDO_TOKEN") or ""
        if not token:
            raise RuntimeError("ORDO_TOKEN is required")
        return cls(token=token, url=os.environ.get("ORDO_WS", DEFAULT_URL))

    @property
    def is_logged_in(self) -> bool:
        return self._logged_in.is_set()

    async def connect(self, login_timeout: float = 30.0) -> Dict[str, Any]:
        if self._ws is not None:
            raise RuntimeError("Already connected")
        self._logged_in = asyncio.Event()
        self._login_failed = asyncio.Event()
        self._closed = asyncio.Event()
        self._ws = await websockets.connect(
            self.url,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
        )
        self._reader = asyncio.create_task(self._read_loop(), name="ordo-ws-async")
        await self._ws.send(json.dumps({"command": "login_user", "token": self.token}))
        try:
            await asyncio.wait_for(self._logged_in.wait(), timeout=login_timeout)
        except asyncio.TimeoutError:
            await self.close()
            if self._login_failed.is_set():
                raise RuntimeError("Ordo login failed")
            raise TimeoutError("Ordo login timed out") from None
        if self._login_failed.is_set() and not self._logged_in.is_set():
            await self.close()
            raise RuntimeError("Ordo login failed")
        return self._login_reply or {}

    async def close(self) -> None:
        self._closed.set()
        self._logged_in.clear()
        reader = self._reader
        self._reader = None
        if reader:
            reader.cancel()
            try:
                await reader
            except (asyncio.CancelledError, Exception):
                pass
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

    async def _read_loop(self) -> None:
        ws = self._ws
        if ws is None:
            return
        try:
            async for raw in ws:
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(data, dict):
                    await self._dispatch(data)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        finally:
            self._closed.set()
            self._logged_in.clear()

    async def send_command(
        self, command: Dict[str, Any], *, timeout: float = 60.0, wait: bool = True
    ) -> Optional[Dict[str, Any]]:
        if not self.is_logged_in or self._ws is None:
            raise RuntimeError("Not logged in")
        name = str(command.get("command") or "")
        if not name:
            raise ValueError("command dict must contain 'command'")
        rid = command.get("request_id")
        fut: Optional[asyncio.Future] = None
        token = 0
        if wait:
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            self._wait_seq += 1
            token = self._wait_seq
            self._pending.append((token, name, rid, fut))
        await self._ws.send(json.dumps(command))
        if not wait:
            return None
        assert fut is not None
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        except asyncio.TimeoutError:
            self._pending = [p for p in self._pending if p[0] != token]
            raise TimeoutError(f"Timed out waiting for {name}") from None

    async def command(self, name: str, **fields: Any) -> Dict[str, Any]:
        return (await self.send_command({"command": name, **fields})) or {}

    async def read_org(self) -> Dict[str, Any]:
        return await self.command("read_org")

    async def find_cluster(self, name: str = "/root") -> Dict[str, Any]:
        return await self.command("find_cluster", name=name)

    async def read_cluster(self, cluster_id: int) -> Dict[str, Any]:
        return await self.command("read_cluster", id=int(cluster_id))

    async def read_job(self, job_id: int) -> Dict[str, Any]:
        return await self.command("read_job", id=int(job_id))

    async def start_cluster(self, cluster_id: int) -> Dict[str, Any]:
        return await self.command("start_cluster", id=int(cluster_id))

    async def start_job(self, job_id: int) -> Dict[str, Any]:
        return await self.command("start_job", id=int(job_id))

    async def watch(
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
                await self.send_command({"command": cmd, "id": int(oid)})
            except Exception:
                pass
        return w

    async def watch_cluster(self, cluster_id: int, **kwargs: Any) -> Watch:
        return await self.watch("cluster", cluster_id, **kwargs)

    async def watch_job(self, job_id: int, **kwargs: Any) -> Watch:
        return await self.watch("job", job_id, **kwargs)

    async def wait(
        self,
        kind: str,
        oid: int,
        *,
        timeout: float = 120.0,
        jobstate: Optional[str] = None,
    ) -> Dict[str, Any]:
        done = asyncio.Event()
        holder: List[Dict[str, Any]] = []

        def _on(event: Dict[str, Any]) -> None:
            holder.append(event)
            done.set()

        self.watches.add(kind, oid, jobstate=jobstate, on_fire=_on)
        cmd = "read_cluster" if kind == "cluster" else "read_job"
        await self.send_command({"command": cmd, "id": int(oid)})
        try:
            await asyncio.wait_for(done.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"watch {kind} {oid} timed out") from None
        return holder[0]

    async def wait_cluster(self, cluster_id: int, **kwargs: Any) -> Dict[str, Any]:
        return await self.wait("cluster", cluster_id, **kwargs)

    async def wait_job(self, job_id: int, **kwargs: Any) -> Dict[str, Any]:
        return await self.wait("job", job_id, **kwargs)

    async def _emit(self, handler: Optional[MessageHandler], payload: Dict[str, Any]) -> None:
        if not handler:
            return
        try:
            result = handler(payload)
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            pass

    async def _dispatch(self, data: Dict[str, Any]) -> None:
        reply = data.get("command_reply")
        if reply == "login_user":
            if data.get("success"):
                self._login_reply = data
                self._logged_in.set()
            else:
                self._login_failed.set()
        elif reply:
            rid = data.get("request_id")
            idx = None
            if rid is not None:
                for i, (_tok, name, want_rid, _f) in enumerate(self._pending):
                    if name == str(reply) and want_rid == rid:
                        idx = i
                        break
            if idx is None:
                for i, (_tok, name, want_rid, _f) in enumerate(self._pending):
                    if name == str(reply) and want_rid is None:
                        idx = i
                        break
            if idx is not None:
                _token, _n, _r, fut = self._pending.pop(idx)
                if not fut.done():
                    fut.set_result(data)

        fired = self.watches.observe_payload(data)
        for event in fired:
            await self._emit(self.on_watch, event)
        await self._emit(self.on_message, data)
