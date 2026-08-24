# ordo-wsagent

Minimal WebSocket client for [Ordo](https://ordoscheduler.com) agents.

It streams **command replies and live broadcasts** as NDJSON so an agent can drive workflows and wait for completion events.

This is the preferred way for agents to interact with Ordo when they need real-time feedback (start a job/cluster and know when it finished).

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Or simply:

```bash
pip install websocket-client
```

## Usage

```bash
export ORDO_TOKEN=your_token_here
./wsagent.py --timeout 600
```

Then send JSON commands on stdin, one per line:

```json
{"command":"read_org"}
{"command":"find_cluster","name":"/root/ops"}
{"command":"manage_cluster","action":"start","id":17}
```

Type `quit` or close stdin when you have no more commands.  
The process stays alive until `--timeout` (default 600 s) or the WebSocket closes, giving the server time to deliver replies and broadcasts.

### One-shot example

```bash
printf '{"command":"read_org"}\nquit\n' | ORDO_TOKEN=... ./wsagent.py --timeout 10
```

### Output format

Every line is a self-contained JSON object (NDJSON):

| `type`    | Meaning                          |
|-----------|----------------------------------|
| `info`    | Client events (connected, sending, timeout, …) |
| `message` | Payload from the Ordo server (replies + broadcasts) |
| `error`   | Client-side errors               |
| `summary` | Final line when the process exits |

## Environment variables

| Variable       | Default                              | Purpose          |
|----------------|--------------------------------------|------------------|
| `ORDO_TOKEN`   | (required)                           | Auth token       |
| `ORDO_WS`      | `wss://ordoscheduler.com/websocket`  | WebSocket URL    |
| `ORDO_TIMEOUT` | `600`                                | Overall timeout  |

## License

MIT
