#!/usr/bin/env python3
"""wsagent.py – minimal Ordo WebSocket client for agents.

ORDO_TOKEN=... python3 wsagent.py [--timeout 600]
ORDO_TOKEN=... python3 wsagent.py --watch-cluster 18 --watch-exit

Send JSON commands on stdin. Read NDJSON on stdout.
Server data is line['payload'] when line['type']=='message'.

Client-only commands (not forwarded): quit, exit, watch_cluster, watch_job.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from typing import Any, Optional

try:
    import websocket
except ImportError:
    print("Missing dependency: pip install websocket-client", file=sys.stderr)
    sys.exit(1)

DEFAULT_URL = "wss://ordoscheduler.com/websocket"
DEFAULT_TIMEOUT = 600
TERMINAL_JOBSTATES = ("complete", "failed", "zombie")

AGENT_BOOTSTRAP = {
    "event": "agent_bootstrap",
    "protocol": (
        "Send one JSON object per line on stdin. Read NDJSON on stdout. "
        "Each stdout line is a wrapper: type, ts, elapsed, payload. "
        "Server data is in payload when type=message. "
        "payload.command_reply = ack/result of a command you sent. "
        "payload.broadcast = unsolicited live update with updates/deletes. "
        "Broadcasts do not have command_reply."
    ),
    "first_step": (
        "Call get_documentation (section 'overview' or 'quickstart', "
        "format 'markdown') before doing real work in a new session."
    ),
    "useful_commands": [
        {"command": "get_documentation", "section": "overview", "format": "markdown"},
        {"command": "get_documentation", "section": "quickstart", "format": "markdown"},
        {"command": "read_org"},
        {"command": "find_cluster", "name": "/root"},
        {"command": "start_cluster", "id": 18},
        {"command": "watch_cluster", "id": 18},
    ],
    "minimal_create_examples": {
        "create_cluster": {"command": "create_cluster", "name": "my-cluster", "parent_id": 1},
        "create_job": {
            "command": "create_job",
            "name": "my-job",
            "parent_id": "<cluster_id from create_cluster or find_cluster>",
            "server_id": 4,
            "script": "#!/bin/sh\necho hello\nsleep 5\necho done\n",
            "_note": "Prefer plain script. Replace parent_id and server_id before sending.",
        },
    },
    "doc_sections": [
        "overview", "quickstart", "core-concepts", "jobs-and-clusters",
        "calendars-and-crons", "advanced-topics", "connecting-to-ai", "api", "cli", "faq",
    ],
    "agent_loop": (
        "1. send a command on stdin  "
        "2. wait for payload.command_reply if you need the ack  "
        "3. start-and-wait: start_cluster/start_job, then watch_cluster/watch_job "
        "(client-only; not forwarded)  "
        "4. wait for type=info event=watch_done  "
        "5. start command_reply is an ack, not completion  "
        "6. disconnect with quit"
    ),
    "wait_for_terminal": {
        "envelope": "Server fields are under payload when type=message.",
        "ack_is_not_done": "start_* success=1 means accepted. Keep waiting.",
        "where": "payload.broadcast in jobs_changed|clusters_changed; scan payload.updates",
        "job": "watch_job id N: jobs_changed updates[] id=N terminal",
        "cluster": "watch_cluster id C: clusters_changed updates[] id=C terminal. A child job complete is not the cluster.",
        "terminal_jobstate": ["complete", "failed", "zombie"],
        "complete_state_id": 5,
        "exit_code": "exit_code is null while not terminal (ready/waiting/starting/running). Null is not failure.",
        "start_rules": "start allowed unless already running or starting. Starting a completed cluster is fine.",
        "watch": "Client-only watch_cluster/watch_job or --watch-cluster/--watch-job. One read snapshot, then broadcasts. Emits watch_done.",
        "request_id": "Optional. Copied onto that command_reply only. Never on broadcasts. Omit it.",
        "stdin": "EOF after watch_cluster does not abort the watch. --timeout is a safety net.",
    },
    "notes": [
        "request_id only correlates a command to its command_reply. Broadcasts never include it.",
        "Broadcasts: {broadcast, updates, deletes}. No command_reply on broadcasts.",
        "watch_cluster / watch_job are client-side only and are not forwarded.",
        "Disconnect with quit/exit. Neither is forwarded to the server.",
        "If the socket drops after start, reconnect and read_cluster; that is not job failure.",
        "MCP: https://ordoscheduler.com/mcp",
        "Clear-semantics: omit a field = leave unchanged; send null / \"\" / [] to clear.",
        "Start-and-wait: start_cluster then watch_cluster. Done on complete, failed, or zombie.",
    ],
}

LOGIN_FAILED_BLURB = {
    "event": "login_failed",
    "message": "Login failed. A valid Ordo API token is required.",
    "how_to_get_a_token": [
        "1. Sign up (or log in) at https://ordoscheduler.com",
        "2. Open Settings and copy your API token",
        "3. Provide it as ORDO_TOKEN env var or --token argument",
    ],
    "examples": [
        "ORDO_TOKEN=your_token_here python3 wsagent.py --timeout 120",
        "python3 wsagent.py --token your_token_here --timeout 120",
    ],
    "next_steps_after_valid_token": [
        "On successful login this client emits an agent_bootstrap message.",
        'First useful call is usually: {"command":"get_documentation","section":"overview","format":"markdown"}',
        "Then explore with read_org / find_cluster.",
    ],
    "links": {
        "sign_up": "https://ordoscheduler.com",
        "docs": "https://ordoscheduler.com",
        "mcp_endpoint": "https://ordoscheduler.com/mcp",
        "ws_client_repo": "https://github.com/nathanielgraham/ordo-wsagent",
    },
}

MISSING_TOKEN_BLURB = {
    "event": "missing_token",
    "message": "ORDO_TOKEN (or --token) is required. No token was supplied.",
    "how_to_get_a_token": LOGIN_FAILED_BLURB["how_to_get_a_token"],
    "examples": LOGIN_FAILED_BLURB["examples"],
    "links": LOGIN_FAILED_BLURB["links"],
}


def emit(msg_type: str, payload: Any, start: float) -> None:
    print(
        json.dumps(
            {"type": msg_type, "ts": time.time(), "elapsed": round(time.time() - start, 3), "payload": payload},
            separators=(",", ":"),
        ),
        flush=True,
    )


def _is_terminal(obj: dict) -> bool:
    state = str(obj.get("jobstate") or "").lower()
    return state in TERMINAL_JOBSTATES or obj.get("state_id") == 5


def main() -> None:
    p = argparse.ArgumentParser(description="Ordo WebSocket agent client")
    p.add_argument("--token", default=os.environ.get("ORDO_TOKEN", ""))
    p.add_argument("--url", default=os.environ.get("ORDO_WS", DEFAULT_URL))
    p.add_argument("--timeout", type=int, default=int(os.environ.get("ORDO_TIMEOUT", DEFAULT_TIMEOUT)))
    p.add_argument("--watch-cluster", type=int, default=None, metavar="ID")
    p.add_argument("--watch-job", type=int, default=None, metavar="ID")
    p.add_argument("--watch-exit", action="store_true")
    args = p.parse_args()

    if args.watch_cluster is not None and args.watch_job is not None:
        print("Use only one of --watch-cluster / --watch-job", file=sys.stderr)
        sys.exit(2)

    start = time.time()
    if not args.token:
        emit("error", MISSING_TOKEN_BLURB, start)
        print("ORDO_TOKEN or --token required.", file=sys.stderr)
        sys.exit(1)

    done = threading.Event()
    logged_in = threading.Event()
    login_failed = threading.Event()
    pending = [0]
    pending_lock = threading.Lock()
    pending_event = threading.Event()
    pending_event.set()
    ws_app: Optional[websocket.WebSocketApp] = None
    watch_lock = threading.Lock()
    watch: dict[str, Any] = {"kind": None, "id": None, "started_at": None, "snapshot_pending": False, "done": False}
    eof_seen = threading.Event()

    def _pending_inc() -> None:
        with pending_lock:
            pending[0] += 1
            pending_event.clear()

    def _pending_dec() -> None:
        with pending_lock:
            if pending[0] > 0:
                pending[0] -= 1
            if pending[0] == 0:
                pending_event.set()

    def _send_server(cmd: dict) -> None:
        emit("info", {"event": "sending", "command": cmd}, start)
        _pending_inc()
        if ws_app:
            ws_app.send(json.dumps(cmd))

    def _arm_watch(kind: str, oid: int, reason: str) -> None:
        with watch_lock:
            watch.update(kind=kind, id=int(oid), started_at=None, snapshot_pending=True, done=False)
        emit(
            "info",
            {
                "event": "watch_armed",
                "kind": kind,
                "id": int(oid),
                "reason": reason,
                "terminal_jobstate": list(TERMINAL_JOBSTATES),
                "note": "Client-side watch. One read snapshot, then broadcasts. Done on complete, failed, or zombie.",
            },
            start,
        )
        _send_server({"command": "read_cluster" if kind == "cluster" else "read_job", "id": int(oid)})

    def _shutdown(reason: str) -> None:
        if not pending_event.wait(timeout=3.0):
            emit("info", {"event": "shutdown_grace_timeout", "pending": pending[0]}, start)
        emit("info", {"event": "shutdown", "reason": reason, "note": "Client shutting down."}, start)
        done.set()
        try:
            if ws_app:
                ws_app.close()
        except Exception:
            pass

    def _finish_watch(obj: dict, source: str) -> None:
        with watch_lock:
            if watch["done"] or watch["kind"] is None:
                return
            watch["done"] = True
            kind, oid = watch["kind"], watch["id"]
        emit(
            "info",
            {
                "event": "watch_done",
                "kind": kind,
                "id": oid,
                "jobstate": obj.get("jobstate"),
                "state_id": obj.get("state_id"),
                "exit_code": obj.get("exit_code"),
                "started": obj.get("started"),
                "ended": obj.get("ended"),
                "source": source,
                "name": obj.get("name"),
            },
            start,
        )
        if args.watch_exit or eof_seen.is_set():
            _shutdown("watch_done")

    def _maybe_watch_obj(obj: dict, expect_kind: str, source: str) -> None:
        if not isinstance(obj, dict):
            return
        with watch_lock:
            kind, oid, already, started_at = watch["kind"], watch["id"], watch["done"], watch["started_at"]
        if already or kind is None or kind != expect_kind or obj.get("id") != oid:
            return
        if started_at is not None and obj.get("started") is not None:
            try:
                if int(obj["started"]) < int(started_at):
                    return
            except (TypeError, ValueError):
                pass
        if _is_terminal(obj):
            _finish_watch(obj, source)

    def _observe_payload(data: dict) -> None:
        reply = data.get("command_reply")
        if reply in {"start_cluster", "start_job"} and data.get("success"):
            with watch_lock:
                kind, oid = watch["kind"], watch["id"]
            started = data.get("started_at") or data.get("started")
            match = (
                (reply == "start_cluster" and kind == "cluster" and oid in {data.get("cluster_id"), data.get("id")})
                or (reply == "start_job" and kind == "job" and oid in {data.get("job_id"), data.get("id")})
            )
            if match and started is not None:
                with watch_lock:
                    watch["started_at"] = started
        if reply == "read_cluster":
            with watch_lock:
                pending_snap = watch["snapshot_pending"] and watch["kind"] == "cluster"
                if pending_snap:
                    watch["snapshot_pending"] = False
            if pending_snap:
                _maybe_watch_obj(data, "cluster", "snapshot")
            return
        if reply == "read_job":
            with watch_lock:
                pending_snap = watch["snapshot_pending"] and watch["kind"] == "job"
                if pending_snap:
                    watch["snapshot_pending"] = False
            if pending_snap:
                _maybe_watch_obj(data, "job", "snapshot")
            return
        bcast = data.get("broadcast")
        if bcast == "clusters_changed":
            for row in data.get("updates") or []:
                _maybe_watch_obj(row, "cluster", "broadcast")
        elif bcast == "jobs_changed":
            for row in data.get("updates") or []:
                _maybe_watch_obj(row, "job", "broadcast")

    def on_open(ws: websocket.WebSocketApp) -> None:
        emit("info", {"event": "connected", "url": args.url}, start)
        ws.send(json.dumps({"command": "login_user", "token": args.token}))

    def on_message(ws: websocket.WebSocketApp, message: str) -> None:
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            emit("raw", {"text": message}, start)
            return
        emit("message", data, start)
        if isinstance(data, dict):
            _observe_payload(data)
        reply = data.get("command_reply") if isinstance(data, dict) else None
        if reply and reply != "login_user":
            _pending_dec()
        if reply != "login_user":
            return
        if logged_in.is_set() or login_failed.is_set():
            return
        if data.get("success"):
            logged_in.set()
            emit("info", AGENT_BOOTSTRAP, start)
            if args.watch_cluster is not None:
                _arm_watch("cluster", args.watch_cluster, "cli")
            elif args.watch_job is not None:
                _arm_watch("job", args.watch_job, "cli")
        else:
            login_failed.set()
            emit("error", LOGIN_FAILED_BLURB, start)
            done.set()
            try:
                ws.close()
            except Exception:
                pass

    def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
        emit("error", {"message": str(error)}, start)

    def on_close(ws: websocket.WebSocketApp, code, msg) -> None:
        emit("info", {"event": "closed", "code": code, "msg": msg}, start)
        done.set()

    def stdin_reader() -> None:
        if not logged_in.wait(timeout=args.timeout):
            if not login_failed.is_set():
                emit("error", {"message": "login timed out"}, start)
            done.set()
            return
        try:
            for line in sys.stdin:
                line = line.strip()
                if not line:
                    continue
                if line.lower() in {"quit", "exit"}:
                    _shutdown("quit")
                    return
                try:
                    cmd = json.loads(line)
                except json.JSONDecodeError:
                    emit("error", {"message": "invalid JSON on stdin"}, start)
                    continue
                if not isinstance(cmd, dict):
                    emit("error", {"message": "stdin JSON must be an object"}, start)
                    continue
                name = str(cmd.get("command", "")).lower()
                if name in {"quit", "exit"}:
                    _shutdown("quit")
                    return
                if name in {"watch_cluster", "watch_job"}:
                    try:
                        oid_int = int(cmd.get("id"))
                    except (TypeError, ValueError):
                        emit("error", {"message": f"{name} requires integer id"}, start)
                        continue
                    _arm_watch("cluster" if name == "watch_cluster" else "job", oid_int, "stdin")
                    continue
                emit("info", {"event": "sending", "command": cmd}, start)
                _pending_inc()
                if ws_app:
                    ws_app.send(json.dumps(cmd))
        except Exception as e:
            emit("error", {"message": f"stdin error: {e}"}, start)
        eof_seen.set()
        with watch_lock:
            watching = watch["kind"] is not None and not watch["done"]
        if watching:
            emit(
                "info",
                {
                    "event": "stdin_eof_watch_active",
                    "note": "Stdin closed; watch still running until terminal state or timeout.",
                    "kind": watch["kind"],
                    "id": watch["id"],
                },
                start,
            )
            return
        _shutdown("eof")

    ws_app = websocket.WebSocketApp(args.url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)

    def run_ws() -> None:
        ws_app.run_forever(ping_interval=20, ping_timeout=10)

    threading.Thread(target=run_ws, daemon=True).start()
    threading.Thread(target=stdin_reader, daemon=True).start()
    finished = done.wait(timeout=args.timeout)
    if not finished:
        emit("info", {"event": "timeout", "timeout": args.timeout}, start)
        try:
            ws_app.close()
        except Exception:
            pass
    emit("summary", {"elapsed": round(time.time() - start, 3), "timed_out": not finished}, start)


if __name__ == "__main__":
    main()
