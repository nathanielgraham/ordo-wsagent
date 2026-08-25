#!/usr/bin/env python3
"""
wsagent.py – minimal Ordo WebSocket client for agents

    pip install websocket-client

    ORDO_TOKEN=... python3 wsagent.py [--timeout 600]

    Send JSON commands on stdin, one per line.
    Read NDJSON on stdout.  Only lines with type=message contain
    server data (command replies + live broadcasts).

    When finished, send quit/exit, close stdin, or kill the process.
    --timeout is only a safety net.
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

# ---------------------------------------------------------------------------
# Bootstrap messages shown to the agent after login success / failure
# ---------------------------------------------------------------------------

AGENT_BOOTSTRAP = {
    "event": "agent_bootstrap",
    "protocol": (
        "Send one JSON object per line on stdin. "
        "Read NDJSON on stdout. "
        "Only lines with type=message contain server data "
        "(command replies and live broadcasts)."
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
        {"command": "find_cluster", "name": "/root/ops"},
    ],
    "doc_sections": [
        "overview",
        "quickstart",
        "core-concepts",
        "jobs-and-clusters",
        "calendars-and-crons",
        "advanced-topics",
        "connecting-to-ai",
        "api",
        "cli",
        "faq",
    ],
    "agent_loop": (
        "1. send a command  "
        "2. wait for the matching command_reply "
        "(and any jobs_changed / clusters_changed broadcasts)  "
        "3. when you have what you need, send quit (or close stdin / "
        "kill the process) to disconnect"
    ),
    "notes": [
        "This is a long-lived WebSocket. Broadcasts arrive unsolicited.",
        "Keep stdin open until you are finished; only send quit or close when done.",
        "MCP tools remain available at https://ordoscheduler.com/mcp "
        "for request/response use.",
        "Full docs: https://ordoscheduler.com (or public/docs/ on GitHub).",
        "Clear-semantics: omit a field = leave unchanged; "
        "send null / \"\" / [] to clear.",
    ],
}

LOGIN_FAILED_BLURB = {
    "event": "login_failed",
    "message": (
        "Login failed. A valid Ordo API token is required."
    ),
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
        "First useful call is usually: "
        '{"command":"get_documentation","section":"overview","format":"markdown"}',
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
    "message": (
        "ORDO_TOKEN (or --token) is required. "
        "No token was supplied."
    ),
    "how_to_get_a_token": LOGIN_FAILED_BLURB["how_to_get_a_token"],
    "examples": LOGIN_FAILED_BLURB["examples"],
    "links": LOGIN_FAILED_BLURB["links"],
}


def emit(msg_type: str, payload: Any, start: float) -> None:
    print(
        json.dumps(
            {
                "type": msg_type,
                "ts": time.time(),
                "elapsed": round(time.time() - start, 3),
                "payload": payload,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Ordo WebSocket agent client")
    p.add_argument("--token", default=os.environ.get("ORDO_TOKEN", ""))
    p.add_argument("--url", default=os.environ.get("ORDO_WS", DEFAULT_URL))
    p.add_argument(
        "--timeout",
        type=int,
        default=int(os.environ.get("ORDO_TIMEOUT", DEFAULT_TIMEOUT)),
    )
    args = p.parse_args()

    start = time.time()

    if not args.token:
        emit("error", MISSING_TOKEN_BLURB, start)
        print(
            "ORDO_TOKEN or --token required. "
            "Sign up at https://ordoscheduler.com and copy the token from Settings.",
            file=sys.stderr,
        )
        sys.exit(1)

    done = threading.Event()
    logged_in = threading.Event()
    login_failed = threading.Event()
    # Track in-flight commands so quit/EOF does not drop replies (one-shot safe)
    pending = [0]
    pending_lock = threading.Lock()
    pending_event = threading.Event()
    pending_event.set()  # no pending initially
    ws_app: Optional[websocket.WebSocketApp] = None

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

        reply = data.get("command_reply")
        if reply and reply != "login_user":
            _pending_dec()

        if reply != "login_user":
            return
        if logged_in.is_set() or login_failed.is_set():
            return

        if data.get("success"):
            logged_in.set()
            emit("info", AGENT_BOOTSTRAP, start)
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

    def _shutdown(reason: str) -> None:
        # Wait briefly for in-flight command_reply so one-shot
        #   printf 'cmd\nquit\n' | python3 wsagent.py
        # still receives the reply.
        if not pending_event.wait(timeout=3.0):
            emit(
                "info",
                {
                    "event": "shutdown_grace_timeout",
                    "note": "Proceeding with shutdown; some replies may be missing.",
                    "pending": pending[0],
                },
                start,
            )
        emit(
            "info",
            {
                "event": "shutdown",
                "reason": reason,
                "note": "Client shutting down.",
            },
            start,
        )
        done.set()
        try:
            if ws_app:
                ws_app.close()
        except Exception:
            pass

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
                # quit/exit → drain in-flight replies, then shut down
                if line.lower() in {"quit", "exit"}:
                    _shutdown("quit")
                    return
                try:
                    cmd = json.loads(line)
                except json.JSONDecodeError:
                    emit("error", {"message": "invalid JSON on stdin"}, start)
                    continue
                emit("info", {"event": "sending", "command": cmd}, start)
                _pending_inc()
                if ws_app:
                    ws_app.send(json.dumps(cmd))
        except Exception as e:
            emit("error", {"message": f"stdin error: {e}"}, start)
        # EOF on stdin → drain in-flight replies, then shut down
        _shutdown("eof")

    ws_app = websocket.WebSocketApp(
        args.url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

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

    emit(
        "summary",
        {"elapsed": round(time.time() - start, 3), "timed_out": not finished},
        start,
    )


if __name__ == "__main__":
    main()
