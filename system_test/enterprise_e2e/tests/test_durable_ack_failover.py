"""
Durable-ack failover e2e tests for the Go client.

Ports the Enterprise reference suite
(``questdb-ent/e2e/tests/test_durable_ack_failover.py`` plus the
orphan-drainer case from ``test_failover.py``) to the Go sidecar. These
exercise the store-and-forward invariant under a ``request_durable_ack=on``
sender:

  - the producer is unaware of server topology and keeps appending to
    on-disk SF across a replica-only failover window;
  - the SF drainer never gives up on a wall-clock reconnect budget;
  - a BackgroundDrainer adopting an orphan slot must honour durable-ack
    (trim on STATUS_DURABLE_ACK, not on plain OK).

The Go client now implements durable-ack (qwp_sf_durable.go + the receiver
in qwp_sf_send_loop.go), so these are real tests, not xfail: connect
negotiates durable-ack on the upgrade and the SF drainer trims/replays off
DURABLE_ACK frames, so the outage-window rows must survive.
"""

from __future__ import annotations

import logging
import time

import pytest

from lib import lifecycle as lc
from lib.pg_query import count_rows, wait_for_dense_sequence

from go_sidecar import GoSidecar, GoSidecarError

LOG = logging.getLogger(__name__)

pytestmark = [pytest.mark.go_client]

_TABLE = "go_durable_ack_failover"

# Rows ingested into A (durably acked) before the primary is killed.
_INITIAL_ROWS = 30
# Rows ingested DURING the replica-only window (no primary reachable). The
# producer keeps going, unaware; these cannot be durably acked until B is
# promoted, so they must be retained in on-disk SF and survive the outage.
_WINDOW_ROWS = 40
# Rows ingested after promotion, still unaware anything happened.
_POST_ROWS = 20
_INGEST_BATCH = 10
_INGEST_BATCH_INTERVAL_S = 0.2

_DURABLE_ACK_AWAIT_TIMEOUT_MS = 60_000
_AWAIT_ROLE_TIMEOUT_S = 60.0
_POLL_INTERVAL_S = 0.3
# Deliberately short reconnect budget for the Invariant B red test: an SF
# drainer must NOT treat this as a give-up deadline. Only SF exhaustion is
# terminal.
_INVARIANT_B_SHORT_BUDGET_MS = 3000
_INVARIANT_B_HOLD_BUDGETS = 3


def _wait_count(*, port: int, table: str, expected: int, timeout_s: float) -> None:
    """Block until a plain COUNT(*) on ``port`` reaches ``expected`` rows."""
    deadline = time.monotonic() + timeout_s
    last = -1
    while time.monotonic() < deadline:
        last = count_rows(port=port, table=table)
        if last >= expected:
            return
        time.sleep(_POLL_INTERVAL_S)
    raise AssertionError(
        f"row count on :{port} reached {last}, expected >= {expected} within {timeout_s}s"
    )


def _ingest_unaware(go_sidecar: GoSidecar, *, count: int, start_index: int) -> int:
    """Produce ``count`` rows into store-and-forward exactly as a real SF
    producer does: COMPLETELY UNAWARE of failover. It never inspects role,
    never waits for a "healthy" sender — it just appends (SEND) and
    publishes to on-disk SF (FLUSH). The only terminal condition is SF
    exhaustion; a hard failure on a transient failover window IS the
    regression this guards, so translate it into a descriptive assertion."""
    try:
        go_sidecar.send(_TABLE, count=count, start_index=start_index)
        return go_sidecar.flush()  # publishes into SF; returns the published fsn
    except GoSidecarError as e:
        raise AssertionError(
            f"store-and-forward producer hard-failed while ingesting rows "
            f"[{start_index}..{start_index + count}) — a transient replica-only "
            f"failover window must be survived by keeping rows in SF and retrying; "
            f"only SF exhaustion may be terminal. sidecar error: {e!r}"
        )


def _ingest_range_unaware(go_sidecar: GoSidecar, *, start_index: int, total: int) -> int:
    """Drive ``total`` rows through :func:`_ingest_unaware` in small batches,
    returning the highest published fsn. Batching + a brief pause keeps the
    producer genuinely mid-stream across the kill/promote events."""
    end = start_index + total
    idx = start_index
    last_fsn = -1
    while idx < end:
        n = min(_INGEST_BATCH, end - idx)
        last_fsn = _ingest_unaware(go_sidecar, count=n, start_index=idx)
        idx += n
        time.sleep(_INGEST_BATCH_INTERVAL_S)
    return last_fsn


def _await_all_replica_round(go_sidecar: GoSidecar, baseline, *, timeout_s: float) -> None:
    """Harness-side coverage guard (never touches the producer): block until
    the sender has COMPLETED at least one full reconnect round against the
    replica-only topology, so the all-replica role-reject path is provably
    exercised before we promote. Requiring reconnAttempts >= baseline+2
    proves the first round's walk completed (reached B, was role-rejected,
    looped, incremented again) rather than merely started."""
    target = baseline.reconn_attempts + 2
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if go_sidecar.stats().reconn_attempts >= target:
            return
        time.sleep(0.1)
    raise AssertionError(
        "sender did not complete a full replica-only reconnect round after the "
        f"primary was killed (needed reconnAttempts >= {target}); the all-replica "
        "role-reject path was never exercised before promotion"
    )


def test_durable_ack_sender_survives_replica_only_window(
    server_factory, go_sidecar: GoSidecar, scenario_dir
) -> None:
    """Kill the primary, keep ingesting through the replica-only window
    (unaware), promote the replica underneath the still-producing sender,
    and assert the full [0..total) sequence — including outage-window rows —
    durably acks on the promoted primary."""
    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    # Boot A (primary) + B (replica), shared object store, both min-http.
    a = server_factory("a", role="primary")
    b = server_factory("b", role="replica")
    a_ports = a.start(min_http=True)
    b_ports = b.start(min_http=True)
    assert b_ports.min_http is not None, (
        "node b (replica): min_http port not reported; needed to promote B later"
    )
    LOG.info("a_ports=%s b_ports=%s", a_ports, b_ports)

    # Durable-ack HA sender listing BOTH endpoints. A is primary -> binds A.
    connect_str = (
        f"ws::addr=127.0.0.1:{a_ports.http},127.0.0.1:{b_ports.http}"
        ";username=admin;password=quest"
        f";sf_dir={sf_dir}"
        ";request_durable_ack=on"
        ";reconnect_max_duration_millis=300000"
        ";reconnect_initial_backoff_millis=100"
        ";reconnect_max_backoff_millis=1000"
        ";close_flush_timeout_millis=5000;"
    )
    go_sidecar.connect(connect_str)

    # Ingest N rows, durably acked by A; B converges via replication.
    initial_fsn = _ingest_range_unaware(go_sidecar, start_index=0, total=_INITIAL_ROWS)
    assert go_sidecar.await_acked(initial_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"initial batch was not durably acked by A [publishedFsn={initial_fsn}]"
    )
    wait_for_dense_sequence(port=a_ports.pg, table=_TABLE,
                            expected_count=_INITIAL_ROWS, timeout_s=60.0)
    _wait_count(port=b_ports.pg, table=_TABLE, expected=_INITIAL_ROWS, timeout_s=120.0)
    LOG.info("A durably holds %d rows; B converged via replication", _INITIAL_ROWS)

    # Disturbance: kill the primary. Only B (a REPLICA) is now reachable.
    baseline = go_sidecar.stats()
    a.kill_9()
    LOG.info("killed primary A; entering replica-only failover window")

    # Keep ingesting straight through the window, UNAWARE. These rows cannot
    # be durably acked (B rejects durable ack as a REPLICA); they accumulate
    # in on-disk SF and must survive.
    _ingest_range_unaware(go_sidecar, start_index=_INITIAL_ROWS, total=_WINDOW_ROWS)
    LOG.info("ingested %d rows into SF during the replica-only window", _WINDOW_ROWS)

    # Coverage guard: a full all-replica reconnect ROUND completed before we
    # promote (counters only; never drives the producer).
    _await_all_replica_round(go_sidecar, baseline, timeout_s=30.0)

    # Promote B -> primary underneath the still-producing sender.
    LOG.info("promoting B REPLICA -> PRIMARY via /lifecycle/switch")
    lc.submit_switch(b_ports.min_http, "primary", wait=True, wait_timeout_s=_AWAIT_ROLE_TIMEOUT_S)

    # Keep ingesting after promotion, still unaware. The SAME sender is used
    # throughout — never a fresh CONNECT, which would mask a halted sender.
    final_fsn = _ingest_range_unaware(
        go_sidecar, start_index=_INITIAL_ROWS + _WINDOW_ROWS, total=_POST_ROWS
    )

    # Terminal drain: every row — including those produced during the outage
    # — must durably ack on the promoted primary.
    assert go_sidecar.await_acked(final_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"rows produced across the failover were lost: full published sequence not "
        f"durably acked by promoted B [publishedFsn={final_fsn}]."
    )

    total = _INITIAL_ROWS + _WINDOW_ROWS + _POST_ROWS
    wait_for_dense_sequence(port=b_ports.pg, table=_TABLE,
                            expected_count=total, timeout_s=120.0)
    LOG.info("recovered: durable-ack sender drained across the failover; B holds [0..%d)", total)


def test_durable_ack_drainer_never_gives_up_on_reconnect_budget(
    server_factory, go_sidecar: GoSidecar, scenario_dir
) -> None:
    """INVARIANT B: once rows are in on-disk SF, the drainer must NEVER
    terminate on a wall-clock reconnect budget. Kill the primary, hold a
    replica-only window (NO promotion) for several times the reconnect
    budget while the producer keeps appending — every FLUSH must keep
    succeeding — then promote and assert the whole sequence durably acks."""
    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    a = server_factory("a", role="primary")
    b = server_factory("b", role="replica")
    a_ports = a.start(min_http=True)
    b_ports = b.start(min_http=True)
    assert b_ports.min_http is not None, (
        "node b (replica): min_http port not reported; needed to promote B later"
    )
    LOG.info("a_ports=%s b_ports=%s", a_ports, b_ports)

    connect_str = (
        f"ws::addr=127.0.0.1:{a_ports.http},127.0.0.1:{b_ports.http}"
        ";username=admin;password=quest"
        f";sf_dir={sf_dir}"
        ";request_durable_ack=on"
        # Deliberately short — Invariant B forbids treating this as terminal
        # for an SF drainer. The window below outlives it several times over.
        f";reconnect_max_duration_millis={_INVARIANT_B_SHORT_BUDGET_MS}"
        ";reconnect_initial_backoff_millis=100"
        ";reconnect_max_backoff_millis=1000"
        ";close_flush_timeout_millis=5000;"
    )
    go_sidecar.connect(connect_str)

    # Establish a live, durably-acked sender against A; B converges.
    go_sidecar.send(_TABLE, count=_INITIAL_ROWS, start_index=0)
    initial_fsn = go_sidecar.flush()
    assert go_sidecar.await_acked(initial_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"initial batch was not durably acked by A [publishedFsn={initial_fsn}]"
    )
    _wait_count(port=b_ports.pg, table=_TABLE, expected=_INITIAL_ROWS, timeout_s=120.0)
    LOG.info("A durably holds %d rows; B converged via replication", _INITIAL_ROWS)

    # Kill the primary and DELIBERATELY do not promote. Hold the replica-only
    # window open for several reconnect budgets.
    a.kill_9()
    kill_at = time.monotonic()
    hold_s = (_INVARIANT_B_SHORT_BUDGET_MS / 1000.0) * _INVARIANT_B_HOLD_BUDGETS
    LOG.info("killed primary A; holding replica-only window for %.1fs (%dx the %dms budget)",
             hold_s, _INVARIANT_B_HOLD_BUDGETS, _INVARIANT_B_SHORT_BUDGET_MS)

    idx = _INITIAL_ROWS
    hold_deadline = kill_at + hold_s
    while time.monotonic() < hold_deadline:
        elapsed_ms = int((time.monotonic() - kill_at) * 1000)
        try:
            go_sidecar.send(_TABLE, count=1, start_index=idx)
            go_sidecar.flush()  # publishes into SF; must keep succeeding
        except GoSidecarError as e:
            raise AssertionError(
                f"durable-ack SF drainer gave up ~{elapsed_ms}ms into a replica-only window "
                f"with a {_INVARIANT_B_SHORT_BUDGET_MS}ms reconnect budget: a store-and-forward "
                f"drainer must NEVER terminate on a wall-clock reconnect budget — only SF "
                f"exhaustion is terminal. sidecar error: {e}"
            )
        idx += 1
        time.sleep(0.25)
    LOG.info("sender stayed alive across %.1fs of replica-only window (well past budget)", hold_s)

    # Positive confirmation the sender was genuinely alive: promote B and
    # assert every row durably acks and lands as a dense [0..idx).
    LOG.info("promoting B REPLICA -> PRIMARY via /lifecycle/switch")
    lc.submit_switch(b_ports.min_http, "primary", wait=True, wait_timeout_s=_AWAIT_ROLE_TIMEOUT_S)
    final_fsn = go_sidecar.flush()
    assert go_sidecar.await_acked(final_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"rows produced across the long replica-only window were lost: not durably acked by "
        f"promoted B [publishedFsn={final_fsn}]"
    )
    wait_for_dense_sequence(port=b_ports.pg, table=_TABLE, expected_count=idx, timeout_s=120.0)
    LOG.info("drainer never gave up; B holds [0..%d) after promotion", idx)


def test_orphan_drainer_durable_ack_survives_kill(
    server_factory, go_sidecar: GoSidecar, obj_store, scenario_dir
) -> None:
    """The BackgroundDrainer must honour ``request_durable_ack=on``.

    A "ghost" sender writes rows with durable-ack on and closes immediately
    (close_flush_timeout_millis=0), leaving its SF slot full of
    un-durably-acked frames. A "foreground" sender with ``drain_orphans=on``
    adopts the slot; its drainer must NOT trim on plain OK. Kill P1 before
    the WAL upload is durable, wipe disk + object store, and the drainer's
    SF must be the surviving copy that replays onto P2."""
    import shutil

    table = "go_trades_orphan_drain"
    sf_dir = scenario_dir / "sf"
    row_count = 50

    p1 = server_factory("p1")
    p1_ports = p1.start()

    ghost_cs = (
        f"ws::addr=127.0.0.1:{p1_ports.http};"
        "username=admin;password=quest;"
        f"sf_dir={sf_dir};"
        "sender_id=ghost;"
        "request_durable_ack=on;"
        # CLOSE returns immediately; the ghost's SF slot stays full of
        # un-durably-acked frames — the drainer is what should empty it.
        "close_flush_timeout_millis=0;"
    )
    go_sidecar.connect(ghost_cs)
    go_sidecar.send(table, count=row_count, start_index=0)
    go_sidecar.flush()
    go_sidecar.close()

    fg_cs = (
        f"ws::addr=127.0.0.1:{p1_ports.http};"
        "username=admin;password=quest;"
        f"sf_dir={sf_dir};"
        "sender_id=primary;"
        "drain_orphans=on;"
        "request_durable_ack=on;"
        "reconnect_max_duration_millis=60000;"
        "close_flush_timeout_millis=5000;"
    )
    go_sidecar.connect(fg_cs)

    # Window for the drainer to schedule, reconnect, push the orphan frames,
    # and receive OK from P1 (which must NOT trim under durable-ack).
    time.sleep(1.0)

    p1.kill_9()
    from lib.server import wait_port_free
    wait_port_free(p1_ports.http)
    wait_port_free(p1_ports.pg)
    if p1.db_root.exists():
        shutil.rmtree(p1.db_root)
    obj_store.wipe()

    p2 = server_factory("p2", db_root_name="p2-fresh")
    p2.start(http_port=p1_ports.http, pg_port=p1_ports.pg)

    # Dense oracle: every ghost row must land on P2, [0..row_count), no gaps,
    # no duplicates. A drainer that OK-trims loses the SF before the kill.
    wait_for_dense_sequence(port=p1_ports.pg, table=table,
                            expected_count=row_count, timeout_s=60.0)
