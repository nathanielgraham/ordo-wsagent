import asyncio
from types import SimpleNamespace

from ordo_wsagent.async_client import AsyncOrdoClient


class FakeWS:
    def __init__(self):
        self.sent = []

    async def send(self, raw: str) -> None:
        self.sent.append(raw)

    async def close(self) -> None:
        pass


def test_dispatch_login_and_watch():
    async def _run():
        c = AsyncOrdoClient(token="x")
        c._ws = FakeWS()
        await c._dispatch({"command_reply": "login_user", "success": 1, "email": "a@b.c"})
        assert c.is_logged_in
        assert c._login_reply["email"] == "a@b.c"

        hits = []

        def on_watch(ev):
            hits.append(ev["jobstate"])

        c.on_watch = on_watch
        c.watches.add("cluster", 18)
        await c._dispatch(
            {
                "broadcast": "clusters_changed",
                "updates": [{"id": 18, "name": "Bork da Cake", "jobstate": "complete"}],
            }
        )
        assert hits == ["complete"]
        assert len(c.watches) == 0

        # request_id matching: two overlapping read_cluster waiters
        f_a = asyncio.get_running_loop().create_future()
        f_b = asyncio.get_running_loop().create_future()
        c._pending.append((1, "read_cluster", "a", f_a))
        c._pending.append((2, "read_cluster", "b", f_b))
        await c._dispatch(
            {"command_reply": "read_cluster", "request_id": "b", "id": 2}
        )
        assert f_b.result()["id"] == 2
        assert not f_a.done()
        await c._dispatch({"command_reply": "read_cluster", "request_id": "a", "id": 1})
        assert f_a.result()["id"] == 1

    asyncio.run(_run())
