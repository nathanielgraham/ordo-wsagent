# Ordo Agent Client (`ordo-wsagent`)

**Preferred way for AI agents to control Ordo.**

This client gives you a long-lived WebSocket connection that delivers:
- immediate command replies, **and**
- live broadcasts when jobs/clusters change state.

MCP tools are still available, but they are request/response only. When you need to start work and know when it finished, use this client.

## 1. Quick start (copy-paste)

```bash
git clone https://github.com/nathanielgraham/ordo-wsagent.git
cd ordo-wsagent
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export ORDO_TOKEN="your_token_here"
python3 wsagent.py --timeout 120
```

Prefer `python3 wsagent.py` over `./wsagent.py` for portability (some environments reject the shebang).

You are now connected. Send JSON commands on stdin (one per line).  
When finished, disconnect with `quit` (bare word **or** `{"command":"quit"}`), by closing stdin, or by killing the process.  
`--timeout` is only a safety net; the server closing the socket also ends the client.

### Minimal one-shot example

```bash
printf '{"command":"read_org"}\nquit\n' | ORDO_TOKEN=... python3 wsagent.py --timeout 10
```

(The JSON form also works: `printf '{"command":"read_org"}\n{"command":"quit"}\n' | ...`)

### Start a cluster and wait (recommended)

`watch_cluster` / `watch_job` are **client-only**. They are not sent to the server. The client takes one snapshot so an already-terminal target does not hang, then waits on broadcasts.

```bash
printf '{"command":"start_cluster","id":18}\n{"command":"watch_cluster","id":18}\n' \
  | ORDO_TOKEN=... python3 wsagent.py --timeout 120 --watch-exit
```

Or arm the watch from the CLI (stdin may close; the watch still runs):

```bash
python3 wsagent.py --watch-cluster 18 --watch-exit --timeout 120
```

Wait for a `type=info` line with `payload.event=watch_done`. That is the completion signal. Do not treat the `start_*` ack as done.

## 2. Output format (NDJSON)

Every **stdout line** is a client wrapper:

```json
{"type":"message","ts":1788067850.1,"elapsed":0.2,"payload":{ }}
```

| `type`    | Meaning |
|-----------|---------|
| `info`    | Client events (`connected`, `agent_bootstrap`, `watch_armed`, `watch_done`, `sending`, `timeout`, …) |
| `message` | Server JSON is in **`payload`** (command replies **and** broadcasts) |
| `error`   | Client-side problems or login failure |
| `summary` | Final line when the process exits |

Look at `payload`, not the wrapper keys. Examples later in this README show **`payload` contents only**.

How to tell the two server shapes apart:

| If `payload` has | It is | Completion? |
|------------------|-------|-------------|
| `command_reply` | Ack / result of a command you sent | No. `start_cluster` success means accepted, not finished. |
| `broadcast` | Unsolicited live update | Maybe. Scan `payload.updates`. |

A broadcast has **no** `command_reply`. Do not drop a line because `command_reply` is missing.

### Bootstrap messages

After a successful login the client emits an `info` event with `event: "agent_bootstrap"` (`type` is `info`, recipe is in `payload`). It contains:

- protocol reminder
- first recommended call (`get_documentation`)
- useful starter commands (including `watch_cluster`)
- doc section list
- the core agent loop
- a `wait_for_terminal` recipe

If login fails (or no token was supplied) the client emits a structured error with instructions on how to obtain a token and get started.

## 3. Login (automatic)

On connect the client automatically sends:

```json
{"command":"login_user","token":"..."}
```

You will see a `type=message` line whose `payload` contains:

```json
"command_reply": "login_user",
"success": 1
```

Only after this succeeds does the client start reading your commands from stdin. Immediately after login success it also emits the `agent_bootstrap` info event described above.

## 4. Core agent pattern: start something and wait for it to finish

This is the most important pattern.

1. Send `start_cluster` or `start_job`. Keep stdin open **or** use `--watch-cluster` / `--watch-job`.
2. The `command_reply` with `success: 1` is an **ack**, not completion.
3. Send `{"command":"watch_cluster","id":C}` or `{"command":"watch_job","id":N}` (client-only).
4. Wait for `type=info` / `event=watch_done`, **or** scan broadcasts yourself.

Starting a **completed** cluster or job is allowed. The server refuses start only when the target is already `running` or `starting`.

Terminal `jobstate` values: `complete`, `failed`, `zombie`. `state_id` 5 also means `complete`. Any of those ends a watch.

`exit_code` is **null** while the job is not terminal (`ready`, `waiting`, `starting`, `running`). Null is not a failure. Read `exit_code` only after the job is terminal (or use `read_log`).

### What “done” means

| You started / watched | Done when |
|-----------------------|-----------|
| `start_job` / `watch_job` id N | `broadcast: "jobs_changed"` `updates[]` row with `id` == N is terminal |
| `start_cluster` / `watch_cluster` id C | `broadcast: "clusters_changed"` `updates[]` row with `id` == C is terminal |

One child job going `complete` is **not** the cluster finishing. `prep` completing does not mean `Bork da Cake` completed. `watch_cluster` waits for the **cluster** row.

### Example – start a cluster and watch

```json
{"command":"start_cluster","id":18}
{"command":"watch_cluster","id":18}
```

Immediate `payload` (ack only):

```json
{
  "command_reply": "start_cluster",
  "success": 1,
  "cluster_id": 18,
  "started_at": 1788067850
}
```

Client then emits `watch_armed`, sends one `read_cluster` snapshot, and listens.

Later `payload` (a job in that cluster moved; cluster may still be running):

```json
{
  "broadcast": "jobs_changed",
  "updates": [
    {
      "id": 11,
      "name": "prep",
      "cluster_id": 18,
      "jobstate": "complete",
      "state_id": 5,
      "started": 1788067851,
      "exit_code": 0
    }
  ],
  "deletes": []
}
```

That does **not** finish `watch_cluster`. This does:

```json
{
  "broadcast": "clusters_changed",
  "updates": [
    {
      "id": 18,
      "name": "Bork da Cake",
      "jobstate": "complete",
      "state_id": 5,
      "started": 1788067850
    }
  ],
  "deletes": []
}
```

Then a client `info` event:

```json
{
  "event": "watch_done",
  "kind": "cluster",
  "id": 18,
  "jobstate": "complete",
  "state_id": 5,
  "source": "broadcast"
}
```

`--watch-exit` shuts the process down at that point. Without it, send `quit` when you are finished (`read_log` is still available).

If stdin hits EOF while a watch is active, the client **keeps running** until `watch_done` or `--timeout`.

### How to disconnect

Disconnect is **client-side only** — nothing is sent to the Ordo server.

Any of these work:

- bare word: `quit` or `exit`
- JSON: `{"command":"quit"}` or `{"command":"exit"}`
- close stdin (EOF) — after the watch has finished, or when no watch is armed
- `--watch-exit` after `watch_done`
- kill the process

`quit` is the clean in-band way for long-lived agent sessions.

## 5. Most useful commands for agents

| Goal | Command |
|------|---------|
| Docs (start here) | `{"command":"get_documentation","section":"overview","format":"markdown"}` |
| See org info | `{"command":"read_org"}` |
| Discover structure | `{"command":"find_cluster","name":"/root"}` or `"/root/ops"` |
| Inspect one cluster | `{"command":"read_cluster","id":24}` |
| Inspect one job | `{"command":"read_job","id":10}` |
| Read latest log | `{"command":"read_log","id":10}` |
| Start a cluster | `{"command":"start_cluster","id":17}` |
| Start a job | `{"command":"start_job","id":42}` |
| Watch until terminal (client-only) | `{"command":"watch_cluster","id":17}` or `watch_job` |
| Stop / kill | `{"command":"kill_cluster","id":17}` or `kill_job` |
| Reset | `{"command":"reset_cluster","id":24}` |

(There are also `create_*`, `update_*`, `delete_*` commands. Prefer the clear-semantics rules: omit a field = leave unchanged; send `null` or `[]` / `""` to clear.)

Client-only (never forwarded): `quit`, `exit`, `watch_cluster`, `watch_job`.

## 6. Broadcasts you will see

These arrive unsolicited whenever something changes. On stdout they are `type: "message"` lines. The server object is `payload`. `payload` has **no** `command_reply`.

```json
{
  "broadcast": "jobs_changed",
  "updates": [ … ],
  "deletes": [ {"id": 18} ]
}
```

| `payload.broadcast` | Meaning |
|---------------------|---------|
| `jobs_changed` | Job created, updated, started, finished, or deleted. `updates` fields match `read_job`. |
| `clusters_changed` | Cluster state or membership changed. `updates` fields match `read_cluster`. |
| `servers_changed` | Server metrics or status. Not a completion signal for jobs. |
| `cals_changed` | Calendar created, updated, or deleted. Not a completion signal for jobs. |

Recognize a broadcast by `payload.broadcast` (and `updates` / `deletes`). Do **not** look for `command_reply` or a top-level `jobs` / `clusters` array — those keys are not present on broadcasts.

`deletes` means the id is gone, not that it completed.

## 7. Recommended agent workflow

`request_id` is optional. If you send it on a command, it is copied onto **that command's** `command_reply` only. It is never present on broadcasts and cannot tell you a job finished. You can omit it.

1. Start `python3 wsagent.py` with a generous timeout (120–600 s) as a safety net
2. Wait for `login_user` success + the `agent_bootstrap` info event
3. Call `get_documentation` (overview / quickstart, format markdown) if this is a new session
4. Send `find_cluster` / `read_cluster` to understand current state
5. Send `start_cluster` or `start_job` unless the target is already `running`/`starting`
6. Send `watch_cluster` / `watch_job` (or pass `--watch-cluster` / `--watch-job`)
7. Wait for `event=watch_done` (or the matching `clusters_changed` / `jobs_changed` broadcast)
8. (optional) send `read_log`
9. Disconnect with `quit` or `--watch-exit`

**Note:** Keep stdin open until you are finished unless a watch is armed. A watch survives stdin EOF.

## 8. Clear-semantics reminder (important)

When updating jobs or clusters:

- **Omit** a field → leave it unchanged
- Send `null`, `""`, or `[]` → clear the field
- This applies to `json`, `cal_id`, `needs_ids`, `needs_any_ids`, `handlers`, `server_id`, `max_runtime_*`, `description`, etc.

## 9. Token & security

- Treat `ORDO_TOKEN` like a password.
- Prefer short-lived or scoped tokens when possible.
- Never commit tokens into the repo.

Get a token: sign up / log in at https://ordoscheduler.com → Settings → copy API token.

## License

MIT
