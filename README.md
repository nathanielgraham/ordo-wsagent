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

To start work and wait for it to finish, **do not** close stdin after the start command. Keep the process running and read lines until a broadcast says the target is terminal, then send `quit`.

## 2. Output format (NDJSON)

Every **stdout line** is a client wrapper:

```json
{"type":"message","ts":1788067850.1,"elapsed":0.2,"payload":{ }}
```

| `type`    | Meaning |
|-----------|---------|
| `info`    | Client events (`connected`, `agent_bootstrap`, `sending`, `timeout`, …) |
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
- useful starter commands
- doc section list
- the core agent loop
- a `wait_for_terminal` recipe for start-and-wait

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

1. Send a start command. Keep stdin open.
2. The next useful `payload` is a `command_reply` (`start_cluster` / `start_job`) with `success: 1`. That is an **ack**, not completion.
3. Later `payload`s have `broadcast` set. Ignore `servers_changed` / `cals_changed` for this wait.
4. You are done only when the **thing you started** is terminal, not when some other row in `updates` is terminal.

Terminal `jobstate` values: `complete`, `failed`, `error`, `killed`. `state_id` 5 also means `complete`. Treat `failed` / `error` / `killed` as finished-unsuccessfully, not as “still running.”

### What “done” means

| You started | Done when |
|-------------|-----------|
| `start_job` id N | A `broadcast: "jobs_changed"` `updates[]` row with `id` == N is terminal |
| `start_cluster` id C | A `broadcast: "clusters_changed"` `updates[]` row with `id` == C is terminal |

One child job going `complete` is **not** the cluster finishing. `prep` completing does not mean `Bork da Cake` completed. Wait for the **cluster** row (or every member job you care about).

`reset_cluster` also emits broadcasts. Those are not the new run. After `start_*`, ignore updates whose `started` is missing or less than the `started_at` from the start reply.

### Example – start a cluster and watch

```json
{"command":"start_cluster","id":17}
```

Immediate `payload` (ack only):

```json
{
  "command_reply": "start_cluster",
  "success": 1,
  "cluster_id": 17,
  "started_at": 1788067850
}
```

Later `payload` (a job in that cluster moved; cluster may still be running):

```json
{
  "broadcast": "jobs_changed",
  "updates": [
    {
      "id": 10,
      "name": "verify",
      "cluster_id": 17,
      "jobstate": "complete",
      "state_id": 5,
      "started": 1788067851,
      "exit_code": 0
    }
  ],
  "deletes": []
}
```

Later `payload` (the cluster itself is done — this is what you wait for):

```json
{
  "broadcast": "clusters_changed",
  "updates": [
    {
      "id": 17,
      "name": "deploy-saas",
      "jobstate": "complete",
      "state_id": 5,
      "started": 1788067850
    }
  ],
  "deletes": []
}
```

**Agent logic:**

- Read stdout line by line. Parse JSON. If `type != "message"`, ignore for this wait (except `error` / `timeout` / `closed`).
- Use `payload = line["payload"]`.
- If `payload.command_reply` is the start ack, store `started_at` and keep waiting.
- If `payload.broadcast` is `clusters_changed` (cluster start) or `jobs_changed` (job start), scan `payload.updates`.
- Match `updates[].id` to the id you started. For a cluster run, prefer the `clusters_changed` row.
- Optional extra guard: `updates[].started >= started_at` from the ack.
- Then you may `read_log` and send `quit`.
- If nothing matching arrives after ~20s, send `read_cluster` / `read_job` once and use that `jobstate`. Do not wait on `command_reply` for completion.
- `request_id` is not part of this wait. Broadcasts never echo it.

### How to disconnect

Disconnect is **client-side only** — nothing is sent to the Ordo server.

Any of these work:

- bare word: `quit` or `exit`
- JSON: `{"command":"quit"}` or `{"command":"exit"}`
- close stdin (EOF)
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
| Stop / kill | `{"command":"kill_cluster","id":17}` or `kill_job` |
| Reset | `{"command":"reset_cluster","id":24}` |

(There are also `create_*`, `update_*`, `delete_*` commands. Prefer the clear-semantics rules: omit a field = leave unchanged; send `null` or `[]` / `""` to clear.)

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
5. Send `start_cluster` or `start_job`. Store `payload.started_at` from the ack. Keep stdin open.
6. Read `type=message` lines. Completion is `payload.broadcast` whose `updates` contain the **started id** in a terminal `jobstate`. For a cluster, wait for `clusters_changed` on that cluster id.
7. (optional) send `read_log`
8. Disconnect with `quit` (bare word or `{"command":"quit"}`) or by closing stdin

**Note:** Keep stdin open until you are finished. Closing stdin immediately triggers shutdown (correct for one-shots, wrong for start-and-wait).

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
