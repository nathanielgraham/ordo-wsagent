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

## 2. Output format (NDJSON)

Every line is a JSON object:

| `type`    | Meaning |
|-----------|---------|
| `info`    | Client events (`connected`, `agent_bootstrap`, `sending`, `timeout`, …) |
| `message` | Everything from the Ordo server (command replies **and** broadcasts) |
| `error`   | Client-side problems or login failure |
| `summary` | Final line when the process exits |

**Always look at `type == "message"`.** That is the data you care about.

Command replies have `command_reply`. Broadcasts do **not** — they have `broadcast` plus `updates` / `deletes`. Do not require `command_reply` to treat a message as useful.

### Bootstrap messages

After a successful login the client emits an `info` event with `event: "agent_bootstrap"`. It contains:

- protocol reminder
- first recommended call (`get_documentation`)
- useful starter commands
- doc section list
- the core agent loop

If login fails (or no token was supplied) the client emits a structured error with instructions on how to obtain a token and get started.

## 3. Login (automatic)

On connect the client automatically sends:

```json
{"command":"login_user","token":"..."}
```

You will see a message with:

```json
"command_reply": "login_user",
"success": 1
```

Only after this succeeds does the client start reading your commands from stdin. Immediately after login success it also emits the `agent_bootstrap` info event described above.

## 4. Core agent pattern: start something and wait for it to finish

This is the most important pattern.

1. Send a start command.
2. You immediately receive a `command_reply` saying the start was accepted.
3. Later you receive one or more **broadcasts** when the real state changes.
4. When a broadcast `updates` entry for the job or cluster you started reaches a terminal `jobstate` (`complete`, `failed`, `error`, `killed`, or `state_id` 5 = complete), you are done — disconnect.

### Example – start a cluster and watch

```json
{"command":"start_cluster","id":17}
```

Immediate reply (example):

```json
{
  "command_reply": "start_cluster",
  "success": 1,
  "cluster_id": 17,
  "started_at": 1788067850
}
```

Later you will see broadcasts that look like:

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
      "exit_code": 0
    }
  ],
  "deletes": []
}
```

```json
{
  "broadcast": "clusters_changed",
  "updates": [
    {
      "id": 17,
      "name": "deploy-saas",
      "jobstate": "complete",
      "state_id": 5
    }
  ],
  "deletes": []
}
```

**Agent logic:**

- Keep reading the NDJSON stream. Keep stdin open.
- For each `type == "message"` payload, if `broadcast` is `jobs_changed` or `clusters_changed`, scan `updates`.
- When the job/cluster you care about (match `id`, and prefer `started` >= the `started_at` from `start_*` so a prior run is not mistaken for this one) has terminal `jobstate`, treat the work as finished.
- Then disconnect (or issue a `read_log` first if you need logs).
- If no matching broadcast arrives within a short window, `read_cluster` / `read_job` once as a fallback. Do not wait only on `command_reply`.

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

These arrive unsolicited whenever something changes. They are `type: "message"` payloads with **no** `command_reply`.

```json
{
  "broadcast": "jobs_changed",
  "updates": [ … ],
  "deletes": [ {"id": 18} ]
}
```

| `broadcast` | Meaning |
|-------------|---------|
| `jobs_changed` | Job created, updated, started, finished, or deleted. `updates` fields match `read_job`. |
| `clusters_changed` | Cluster state or membership changed. `updates` fields match `read_cluster`. |
| `servers_changed` | Server metrics or status. |
| `cals_changed` | Calendar created, updated, or deleted. |

Recognize a broadcast by the `broadcast` key (and `updates` / `deletes`). Do **not** look for `command_reply` or a top-level `jobs` / `clusters` array — those keys are not present on broadcasts.

## 7. Recommended agent workflow

Optional `request_id` on a command is echoed on that `command_reply` only (never on broadcasts). Omit it for today's protocol.

1. Start `python3 wsagent.py` with a generous timeout (120–600 s) as a safety net
2. Wait for `login_user` success + the `agent_bootstrap` info event
3. Call `get_documentation` (overview / quickstart, format markdown) if this is a new session
4. Send `find_cluster` / `read_cluster` to understand current state
5. Send `start_cluster` or `start_job` and note `started_at` from the reply
6. Keep reading the stream. Completion is a `broadcast` whose `updates` contain the target id in a terminal `jobstate`
7. (optional) send `read_log`
8. Disconnect with `quit` (bare word or `{"command":"quit"}`) or by closing stdin

**Note:** Keep stdin open until you are finished. Closing stdin immediately triggers shutdown (correct for one-shots, sharp for long-lived runners).

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
