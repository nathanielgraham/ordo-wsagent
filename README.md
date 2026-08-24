# ordo-wsagent

Minimal WebSocket client for [Ordo](https://ordoscheduler.com) agents.

Streams command replies **and** live broadcasts as NDJSON so an agent can drive workflows and wait for completion events.

## Install

```bash
pip install -r requirements.txt
# or
pip install websocket-client
```

## Usage

```bash
export ORDO_TOKEN=your_token_here
./wsagent.py --timeout 600
```

Then send JSON commands on stdin (one per line). Type `quit` or close stdin when finished.

Full documentation and the final script will be added after testing.
