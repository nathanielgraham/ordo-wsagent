# Ordo Agent Client (`ordo-wsagent`)

**Preferred way for AI agents to control Ordo over WebSocket.**

This repo is now an installable library *and* a stdin NDJSON CLI.

- immediate command replies
- live broadcasts when jobs/clusters change
- a **watch registry** (many watches at once)

MCP tools are still request/response only. When you need to start work and know when it finished, use this client.

## Install

```bash
git clone https://github.com/nathanielgraham/ordo-wsagent.git
cd ordo-wsagent
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

```bash
export ORDO_TOKEN="your_token_here"
python3 -m ordo_wsagent --timeout 120
# same as: python3 wsagent.py --timeout 120
```

## Library

```python
from ordo_wsagent import OrdoClient

c = OrdoClient.from_env()   # ORDO_TOKEN
c.connect()
c.start_cluster(18)
done = c.wait_cluster(18, timeout=120)   # jobstate complete|failed|zombie|killed
print(done["jobstate"], done["name"])
c.close()
```

Several watches at once:

```python
c.watch_cluster(18)
c.watch_job(10, jobstate="complete")
# later: c.on_watch = lambda event: ...
```

Terminal detection uses **jobstate names only**: `complete`, `failed`, `zombie`, `killed`.
Do not use `state_id` (that is a server-side id; `5` happens to mean complete).

`wait_*` / `watch_*` are client-side. They take one `read_*` snapshot so an already-terminal target does not hang, then wait on broadcasts. A child job completing does **not** finish a cluster watch.

## CLI quick start

Send JSON commands on stdin (one per line). Disconnect with `quit`.

```bash
printf '{"command":"read_org"}\nquit\n' | ORDO_TOKEN=... python3 -m ordo_wsagent --timeout 10
```

Start a cluster and wait:

```bash
printf '{"command":"start_cluster","id":18}\n{"command":"watch_cluster","id":18}\n' \
  | ORDO_TOKEN=... python3 -m ordo_wsagent --timeout 120 --watch-exit
```

Or:

```bash
python3 -m ordo_wsagent --watch-cluster 18 --watch-exit --timeout 120
```

Wait for `type=info` / `event=watch_done`. Do not treat the `start_*` ack as done.

## Output format (NDJSON)

Every stdout line:

```json
{"type":"message","ts":1788067850.1,"elapsed":0.2,"payload":{ }}
```

| type | Meaning |
|------|---------|
| info | Client events (`connected`, `agent_bootstrap`, `watch_armed`, `watch_done`, …) |
| message | Server JSON in **payload** (replies **and** broadcasts) |
| error | Client-side problems or login failure |
| summary | Final line on exit |

`payload.command_reply` = ack/result of a command you sent.  
`payload.broadcast` = unsolicited live update (`updates` / `deletes`). Broadcasts have no `command_reply`.

Login is automatic (`login_user`). After success the client emits `agent_bootstrap`.

## Useful commands

| Goal | Command |
|------|---------|
| Docs | `{"command":"get_documentation","section":"overview","format":"markdown"}` |
| Org | `{"command":"read_org"}` |
| Tree | `{"command":"find_cluster","name":"/root"}` |
| Inspect | `read_cluster` / `read_job` / `read_log` |
| Start | `start_cluster` / `start_job` |
| Watch (client-only) | `watch_cluster` / `watch_job` |
| Stop | `kill_cluster` / `kill_job` |

Client-only (never forwarded): `quit`, `exit`, `watch_cluster`, `watch_job`.

## Sharing with ordo-bot

`WatchRegistry`, `is_terminal`, and `OrdoClient` are the intended shared layer for [ordo-bot](https://github.com/nathanielgraham/ordo-bot). Bot-only pieces (LLM, frontend chat protocol) stay in that repo.

## Token

Treat `ORDO_TOKEN` like a password. Get one at https://ordoscheduler.com → Settings.

## License

MIT
