#!/usr/bin/env python3
"""
wsagent.py – minimal Ordo WebSocket client for agents

    pip install websocket-client

    ORDO_TOKEN=... python3 wsagent.py [--timeout 600]
    ORDO_TOKEN=... python3 wsagent.py --watch-cluster 18 --watch-exit

    Send JSON commands on stdin, one per line.
    Read NDJSON on stdout.  Server data is line[\"payload\"] when
    line[\"type\"] == \"message\" (command replies + live broadcasts).

    Command replies have payload.command_reply. Broadcasts have
    payload.broadcast plus updates/deletes — they do not have
    command_reply. Wait for terminal jobstate on broadcast updates,
    not on the start command_reply.

    Client-only commands (not sent to the server):
        quit / exit
        {\"command\":\"watch_cluster\",\"id\":N}
        {\"command\":\"watch_job\",\"id\":N}

    When finished, send quit, close stdin (after watch completes),
    or kill the process.  --timeout is only a safety net.
"""
