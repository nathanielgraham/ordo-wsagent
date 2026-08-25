# Ordo Agent Client (`ordo-wsagent`)

**Preferred way for AI agents to control Ordo.**

This client gives you a long-lived WebSocket connection that delivers:
- immediate command replies, **and**
- live broadcasts when jobs/clusters change state.

MCP tools are still available, but they are request/response only. When you need to start work and know when it finished, use this client.

## 1. Quick start (copy-paste)

    git clone https://github.com/nathanielgraham/ordo-wsagent.git
    cd ordo-wsagent
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

    export ORDO_TOKEN="your_token_here"
    ./wsagent.py --timeout 120

You are now connected. Send JSON commands on stdin (one per line).  
Type `quit` or close stdin when you have no more commands. The process stays alive until the timeout so you can receive broadcasts.

### Minimal one-shot example

    printf '{"command":"read_org"}\nquit\n' | ORDO_TOKEN=... ./wsagent.py --timeout 10

## 2. Output format (NDJSON)

Every line is a JSON object:

| `type`    | Meaning |
|-----------|---------|
| `info`    | Client events (`connected`, `sending`, `timeout`, `stdin_closed`…) |
| `message` | Everything from the Ordo server (command replies **and** broadcasts) |
| `error`   | Client-side problems |
| `summary` | Final line when the process exits |

**Always look at `type == "message"`.** That is the data you care about.

## 3. Login (automatic)

On connect the client automatically sends:

    {"command":"login_user","token":"..."}

You will see a message with:

    "command_reply": "login_user",
    "success": 1

Only after this succeeds does the client start reading your commands from stdin.

## 4. Core agent pattern: start something and wait for it to finish

This is the most important pattern.

1. Send a start command.
2. You immediately receive a `command_reply` saying the start was accepted.
3. Later you receive one or more **broadcasts** (`jobs_changed` / `clusters_changed`) when the real state changes.
4. When you see the job or cluster reach a terminal state (`complete`, `failed`, etc.), you are done.

### Example – start a cluster and watch

    {"command":"start_cluster","id":17}

Immediate reply (example):

    {
      "command_reply": "start_cluster",
      "success": 1,
      "message": "cluster started"
    }

Later you will see broadcasts that look roughly like:

    {
      "command_reply": "jobs_changed",
      "jobs": [
        {
          "id": 10,
          "name": "verify",
          "jobstate": "complete",
          "state_id": 5,
          "exit_code": 0
        }
      ]
    }

**Agent logic:**

- Keep reading the NDJSON stream.
- When you see a broadcast where the job/cluster you care about has `jobstate` / state in `["complete","failed","error"]` (or `state_id` 5 = complete, etc.), treat the work as finished.
- Then you can issue a `read_log` or move on.

## 5. Most useful commands for agents

| Goal | Command |
|------|---------|
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

These arrive unsolicited whenever something changes:

- `jobs_changed` – one or more jobs updated (state, exit code, etc.)
- `clusters_changed` – cluster state or membership changed
- `servers_changed` – server metrics or status

A broadcast is just another `type: "message"` object. Look at the `command_reply` field (or the presence of a `jobs` / `clusters` array) to recognise them.

## 7. Recommended agent workflow

1. Start wsagent.py with a generous timeout (120–600 s)
2. Wait for login_user success
3. send find_cluster / read_cluster to understand current state
4. send start_cluster or start_job
5. keep reading the stream until you see the relevant completion broadcast
6. (optional) send read_log
7. send quit or close stdin

## 8. Clear-semantics reminder (important)

When updating jobs or clusters:

- **Omit** a field → leave it unchanged
- Send `null`, `""`, or `[]` → clear the field
- This applies to `json`, `cal_id`, `needs_ids`, `needs_any_ids`, `handlers`, `server_id`, `max_runtime_*`, `description`, etc.

## 9. Token & security

- Treat `ORDO_TOKEN` like a password.
- Prefer short-lived or scoped tokens when possible.
- Never commit tokens into the repo.
