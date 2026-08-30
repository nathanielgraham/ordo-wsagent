from ordo_wsagent.protocol import is_terminal, jobstate_of, TERMINAL_JOBSTATES
from ordo_wsagent.watches import WatchRegistry


def test_terminal_names_include_killed():
    assert TERMINAL_JOBSTATES == frozenset({"complete", "failed", "zombie", "killed"})
    assert is_terminal({"jobstate": "killed"})
    assert is_terminal({"jobstate": "Complete"})
    assert not is_terminal({"jobstate": "running", "state_id": 5})
    assert not is_terminal({"state_id": 5})
    assert jobstate_of({"jobstate": "FAILED"}) == "failed"


def test_registry_cluster_not_fired_by_child_job():
    fired = []
    reg = WatchRegistry()
    reg.add("cluster", 18, on_fire=lambda e: fired.append(e))
    reg.observe_payload(
        {
            "broadcast": "jobs_changed",
            "updates": [{"id": 11, "cluster_id": 18, "jobstate": "complete"}],
        }
    )
    assert fired == []
    assert len(reg) == 1
    reg.observe_payload(
        {
            "broadcast": "clusters_changed",
            "updates": [{"id": 18, "name": "Bork da Cake", "jobstate": "complete"}],
        }
    )
    assert len(fired) == 1
    assert fired[0]["kind"] == "cluster"
    assert fired[0]["id"] == 18
    assert fired[0]["jobstate"] == "complete"
    assert len(reg) == 0


def test_snapshot_already_terminal():
    fired = []
    reg = WatchRegistry()
    reg.add("job", 14, on_fire=lambda e: fired.append(e))
    reg.observe_payload({"command_reply": "read_job", "id": 14, "jobstate": "failed"})
    assert len(fired) == 1
    assert fired[0]["source"] == "snapshot"
    assert fired[0]["jobstate"] == "failed"


def test_specific_jobstate_filter():
    fired = []
    reg = WatchRegistry()
    reg.add("job", 10, jobstate="complete", on_fire=lambda e: fired.append(e))
    reg.observe_payload(
        {"broadcast": "jobs_changed", "updates": [{"id": 10, "jobstate": "killed"}]}
    )
    assert fired == []
    reg.observe_payload(
        {"broadcast": "jobs_changed", "updates": [{"id": 10, "jobstate": "complete"}]}
    )
    assert len(fired) == 1


def test_multiple_watches():
    hits = []
    reg = WatchRegistry()
    reg.add("job", 11, on_fire=lambda e: hits.append(("prep", e["jobstate"])))
    reg.add("cluster", 18, on_fire=lambda e: hits.append(("cake", e["jobstate"])))
    reg.observe_payload(
        {"broadcast": "jobs_changed", "updates": [{"id": 11, "jobstate": "killed"}]}
    )
    reg.observe_payload(
        {"broadcast": "clusters_changed", "updates": [{"id": 18, "jobstate": "failed"}]}
    )
    assert hits == [("prep", "killed"), ("cake", "failed")]
