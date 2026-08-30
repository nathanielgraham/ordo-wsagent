#!/usr/bin/env python3
"""NDJSON stdin CLI over OrdoClient.

The process stays connected until the agent sends quit/exit, the safety
--timeout fires, or --watch-exit and every armed watch has completed.
watch_done never closes the socket by itself.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from typing import Any

from .client import OrdoClient
from .protocol import DEFAULT_URL, TERMINAL_JOBSTATES

LOGIN_FAILED_BLURB = {
    "event": "login_failed",
    "message": "Login failed. A valid Ordo API token is required.",
    "how_to_get_a_token": [
        "1. Sign up (or log in) at https://ordoscheduler.com",
        "2. Open Settings and copy your API token",
        "3. Provide it as ORDO_TOKEN env var or --token argument",
    ],
    "examples": [
        "ORDO_TOKEN=your_token_here python3 -m ordo_wsagent --timeout 120",
        "python3 -m ordo_wsagent --token your_token_here --timeout 120",
    ],
    "links": {
        "sign_up": "https://ordoscheduler.com",
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

AGENT_BOOTSTRAP = {
    "event": "agent_bootstrap",
    "first_step": "Call get_documentation section overview or agent-protocol.",
    "useful_commands": [
        {"command": "get_documentation", "section": "overview", "format": "markdown"},
        {"command": "read_org"},
        {"command": "find_cluster", "name": "/root"},
        {"command": "watch_cluster", "id": 18},
    ],
    "terminal_jobstate": sorted(TERMINAL_JOBSTATES),
    "notes": [
        "watch_cluster / watch_job are client-side; they are not forwarded.",
        "start_* success is an ack. Wait for watch_done or a terminal jobstate.",
        "Terminal jobstate names: complete, failed, zombie, killed.",
        "Keep the socket open across steps. Disconnect with quit when you are finished.",
        "A dropped connection is not a job failure; reconnect and read_*.",
        "exit_code is only meaningful after jobstate is terminal.",
        "Clear-semantics: omit a field to leave it; send null / \"\" / [] to clear.",
    ],
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


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Ordo WebSocket agent client")
    p.add_argument("--token", default=os.environ.get("ORDO_TOKEN", ""))
    p.add_argument("--url", default=os.environ.get("ORDO_WS", DEFAULT_URL))
    p.add_argument("--timeout", type=int, default=int(os.environ.get("ORDO_TIMEOUT", "600")))
    p.add_argument("--watch-cluster", type=int, default=None, metavar="ID")
    p.add_argument("--watch-job", type=int, default=None, metavar="ID")
    p.add_argument(
        "--watch-exit",
        action="store_true",
        help="After every armed watch fires, exit. Default is stay connected until quit.",
    )
    args = p.parse_args(argv)

    start = time.time()
    if not args.token:
        emit("error", MISSING_TOKEN_BLURB, start)
        print("ORDO_TOKEN or --token required.", file=sys.stderr)
        sys.exit(1)

    done = threading.Event()
    client = OrdoClient(token=args.token, url=args.url)

    def on_message(data: dict) -> None:
        emit("message", data, start)

    def on_watch(event: dict) -> None:
        slim = {k: v for k, v in event.items() if k != "object"}
        slim["watches_remaining"] = len(client.watches)
        emit("info", slim, start)
        if args.watch_exit and len(client.watches) == 0:
            _shutdown("watch_exit")

    client.on_message = on_message
    client.on_watch = on_watch

    def _shutdown(reason: str) -> None:
        emit("info", {"event": "shutdown", "reason": reason, "note": "Client shutting down."}, start)
        done.set()
        client.close()

    def _arm(kind: str, oid: int, reason: str) -> None:
        emit(
            "info",
            {
                "event": "watch_armed",
                "kind": kind,
                "id": int(oid),
                "reason": reason,
                "terminal_jobstate": sorted(TERMINAL_JOBSTATES),
                "note": "Client-side watch. Socket stays open after watch_done unless you send quit or pass --watch-exit.",
            },
            start,
        )
        client.watch(kind, oid)

    try:
        emit("info", {"event": "connected", "url": args.url}, start)
        client.connect(login_timeout=min(args.timeout, 30))
    except Exception as exc:
        emit("error", {**LOGIN_FAILED_BLURB, "detail": str(exc)}, start)
        emit("summary", {"elapsed": round(time.time() - start, 3), "timed_out": False}, start)
        sys.exit(1)

    emit("info", AGENT_BOOTSTRAP, start)
    if args.watch_cluster is not None:
        _arm("cluster", args.watch_cluster, "cli")
    if args.watch_job is not None:
        _arm("job", args.watch_job, "cli")

    def stdin_reader() -> None:
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
                        oid = int(cmd.get("id"))
                    except (TypeError, ValueError):
                        emit("error", {"message": f"{name} requires integer id"}, start)
                        continue
                    _arm("cluster" if name == "watch_cluster" else "job", oid, "stdin")
                    continue
                emit("info", {"event": "sending", "command": cmd}, start)
                try:
                    client.send_command(cmd, wait=False)
                except Exception as e:
                    emit("error", {"message": str(e)}, start)
        except Exception as e:
            emit("error", {"message": f"stdin error: {e}"}, start)
        emit(
            "info",
            {
                "event": "stdin_eof",
                "note": "Stdin closed; socket stays open until quit, --watch-exit (all watches done), or --timeout.",
                "watches": len(client.watches),
            },
            start,
        )

    threading.Thread(target=stdin_reader, daemon=True).start()
    finished = done.wait(timeout=args.timeout)
    if not finished:
        emit("info", {"event": "timeout", "timeout": args.timeout}, start)
        client.close()
    emit("summary", {"elapsed": round(time.time() - start, 3), "timed_out": not finished}, start)


if __name__ == "__main__":
    main()
