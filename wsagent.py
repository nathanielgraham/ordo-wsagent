#!/usr/bin/env python3
"""
wsagent.py – minimal Ordo WebSocket client for agents

    pip install websocket-client

    ORDO_TOKEN=... ./wsagent.py [--timeout 600]
    # send JSON commands on stdin, one per line
    # "quit", "exit", or EOF means "no more commands"
    # the process stays alive until --timeout or the socket closes
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

    if not args.token:
        print("ORDO_TOKEN or --token required", file=sys.stderr)
        sys.exit(1)

    start = time.time()
    done = threading.Event()          # set when we should exit
    logged_in = threading.Event()
    ws_app: Optional[websocket.WebSocketApp] = None

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

        if (
            data.get("command_reply") == "login_user"
            and data.get("success")
            and not logged_in.is_set()
        ):
            logged_in.set()

    def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
        emit("error", {"message": str(error)}, start)

    def on_close(ws: websocket.WebSocketApp, code, msg) -> None:
        emit("info", {"event": "closed", "code": code, "msg": msg}, start)
        done.set()

    def stdin_reader() -> None:
        if not logged_in.wait(timeout=args.timeout):
            emit("error", {"message": "login timed out"}, start)
            done.set()
            return

        try:
            for line in sys.stdin:
                line = line.strip()
                if not line:
                    continue
                if line.lower() in {"quit", "exit"}:
                    emit("info", {"event": "stdin_closed", "reason": "quit"}, start)
                    # do NOT set done here – just stop reading commands
                    break
                try:
                    cmd = json.loads(line)
                except json.JSONDecodeError:
                    emit("error", {"message": "invalid JSON on stdin"}, start)
                    continue
                emit("info", {"event": "sending", "command": cmd}, start)
                if ws_app:
                    ws_app.send(json.dumps(cmd))
        except Exception as e:
            emit("error", {"message": f"stdin error: {e}"}, start)
        # EOF or quit → stop accepting commands, but keep the socket open
        # until timeout or on_close

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
