# Ordo Agent Client (`ordo-wsagent`)

**Preferred way for AI agents to control Ordo over WebSocket.**

Installable library *and* a stdin NDJSON CLI.

- immediate command replies
- live broadcasts when jobs/clusters change
- a **watch registry** (many watches at once)
- the **agent** decides when to disconnect
- sync client for scripts/CLI; **async** client for ordo-bot

MCP tools are request/response only. When you need to start work and know when it finished, use this client.

## Install

```bash
git clone https://github.com/nathanielgraham/ordo-wsagent.git
cd ordo-wsagent
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

```bash
export ORDO_TOKEN="your_token_here"
python3 -m ordo_wsagent --timeout 600
# same as: python3 wsagent.py --timeout 600
```

## Library (sync)

Scripts and the stdin CLI. Blocks the calling thread until the ack or watch fires.

```python
from ordo_wsagent import OrdoClient

c = OrdoClient.from_env()   # ORDO_TOKEN
c.connect()
c.start_cluster(18)
done = c.wait_cluster(18, timeout=120)   # complete|failed|zombie|killed
print(done["jobstate"], done["name"])
c.close()   # you close when the workflow is finished
```

## Library (async)

For ordo-bot and any asyncio app. Same protocol and watches; does not block the event loop.

```bash
pip install 'ordo-wsagent[async]'
```

```python
from ordo_wsagent import AsyncOrdoClient

async def main():
    c = AsyncOrdoClient.from_env()
    await c.connect()
    await c.start_cluster(18)
    done = await c.wait_cluster(18, timeout=120)
    print(done["jobstate"], done["name"])
    await c.close()
```

`on_message` / `on_watch` may be sync or async callables.

Several watches at once:

```python
c.watch_cluster(18)
c.watch_job(10, jobstate="complete")
c.on_watch = lambda event: print(event["kind"], event["id"], event["jobstate"])
```

Terminal detection uses **jobstate names only**: `complete`, `failed`, `zombie`, `killed`.
Do not use `state_id` (`5` is the server id for complete).

`wait_*` / `watch_*` are client-side. One `read_*` snapshot so an already-terminal target does not hang, then broadcasts. A child job completing does **not** finish a cluster watch.

`exit_code` is only meaningful after `jobstate` is terminal. Null while `ready` / `waiting` / `starting` / `running` is not a failure.

If the socket drops after start, that is not a job failure. Reconnect and `read_cluster` / `read_job`.

Clear-semantics on updates: omit a field to leave it; send `null` / `""` / `[]` to clear.

## Disconnect

Keep the WebSocket open across a multi-step workflow.

| How | When to use |
|-----|-------------|
| `quit` / `exit` (word or JSON) | Agent is finished |
| `client.close()` / `await client.close()` | Library caller is finished |
| `--timeout` | Safety net only |
| `--watch-exit` | One-shot scripts: exit after **every** armed watch has fired |

`watch_done` does **not** close the connection. Stdin EOF does **not** close it either.

## CLI

```bash
printf '{"command":"read_org"}\n{"command":"quit"}\n' \
  | ORDO_TOKEN=... python3 -m ordo_wsagent --timeout 10
```

Start two things and stay connected:

```bash
printf '{"command":"start_cluster","id":18}\n{"command":"watch_cluster","id":18}\n{"command":"watch_job","id":10}\n' \
  | ORDO_TOKEN=... python3 -m ordo_wsagent --timeout 600
```

One-shot (exit when all watches complete):

```bash
python3 -m ordo_wsagent --watch-cluster 18 --watch-job 10 --watch-exit --timeout 120
```

Wait for `type=info` / `event=watch_done`. Do not treat the `start_*` ack as done.

## Output format (NDJSON)

```json
{"type":"message","ts":1788067850.1,"elapsed":0.2,"payload":{ }}
```

| type | Meaning |
|------|---------|
| info | Client events (`connected`, `agent_bootstrap`, `watch_armed`, `watch_done`, `stdin_eof`, …) |
| message | Server JSON in **payload** (replies **and** broadcasts) |
| error | Client-side problems or login failure |
| summary | Final line on exit |

`payload.command_reply` = ack/result of a command you sent.  
`payload.broadcast` = unsolicited live update. Broadcasts have no `command_reply`.

Optional `request_id` on a command is copied onto that command’s reply only (never on broadcasts). The client uses it to match overlapping `read_*` calls.

## Sharing with ordo-bot

`WatchRegistry`, `is_terminal`, and `AsyncOrdoClient` are the shared layer for [ordo-bot](https://github.com/nathanielgraham/ordo-bot). Bot-only pieces (LLM, frontend chat protocol) stay there. Scripts keep using sync `OrdoClient`.

## Token

Treat `ORDO_TOKEN` like a password. https://ordoscheduler.com → Settings.

## License

MIT
