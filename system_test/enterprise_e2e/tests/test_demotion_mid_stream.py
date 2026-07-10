"""
In-place graceful demotion UNDERNEATH an actively-sending Go QWP sender.

Ports the Enterprise reference ``questdb-ent/e2e/tests/test_demotion_mid_stream.py``
to the Go ingress sidecar. Where test_durable_ack_failover.py kill-9s the
primary (an abnormal transport drop), this exercises the *graceful*
role-change path: a data frame lands on a connection whose peer has just
demoted PRIMARY->REPLICA, so the server must close the WebSocket with a
reconnect-eligible NORMAL_CLOSURE (not a SECURITY_ERROR NACK), and the Go
store-and-forward sender must absorb the window and replay after promotion.

Real test (durable-ack is implemented): the sender rides the demote, walks
the all-replica window (Invariant B: never gives up), and after B is
promoted every row lands exactly once on B.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pytest

from lib import lifecycle as lc
from lib.pg_query import count_rows, wait_for_dense_sequence

from go_sidecar import GoSidecar, GoSidecarError

LOG = logging.getLogger(__name__)

pytestmark = [pytest.mark.go_client]

_TABLE = "go_demotion_mid_stream"

_INITIAL_ROWS = 30
_WINDOW_ROWS = 40
_POST_ROWS = 20
_INGEST_BATCH = 10
_INGEST_BATCH_INTERVAL_S = 0.2
_DURABLE_ACK_AWAIT_TIMEOUT_MS = 60_000
_AWAIT_ROLE_TIMEOUT_S = 60.0
_POLL_INTERVAL_S = 0.25


def _wait_count(*, port: int, table: str, expected: int, timeout_s: float) -> None:
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
    """Produce rows exactly as a real SF producer does: UNAWARE of the role
    switch. It just appends (SEND) and publishes to on-disk SF (FLUSH). The
    only terminal condition is SF exhaustion; a hard failure across the demote
    IS the regression this guards (e.g. a demoted node NACKing SECURITY_ERROR
    instead of a role-change close)."""
    try:
        go_sidecar.send(_TABLE, count=count, start_index=start_index)
        return go_sidecar.flush()
    except GoSidecarError as e:
        raise AssertionError(
            f"store-and-forward producer hard-failed while ingesting rows "
            f"[{start_index}..{start_index + count}) across an in-place demote — "
            f"a graceful role change must surface as a reconnect-eligible close "
            f"(retry from SF), never a terminal. sidecar error: {e!r}"
        )


def _ingest_range_unaware(go_sidecar: GoSidecar, *, start_index: int, total: int) -> int:
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
    """Coverage guard: block until the sender has COMPLETED a full reconnect
    round against the all-replica topology (reconnAttempts >= baseline+2), so
    the role-reject walk the role-change close hands the client to is provably
    exercised before we promote."""
    target = baseline.reconn_attempts + 2
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if go_sidecar.stats().reconn_attempts >= target:
            return
        time.sleep(0.1)
    raise AssertionError(
        "sender did not complete a full all-replica reconnect round after the "
        f"in-place demote (needed reconnAttempts >= {target})"
    )


def _await_role_change_close_evidence(log_dir: Path, *, node: str, timeout_s: float) -> None:
    """Coverage witness: prove the demoted node's read-only gate fired on a
    mid-stream QWP frame — the precondition for the role-change close. The
    witness is the server's write-refusal log line ("replica access is
    read-only")."""
    needles = ("replica access is read-only",)
    files = (log_dir / f"{node}.stdout.log", log_dir / f"{node}.stderr.log")
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for f in files:
            try:
                text = f.read_text(encoding="utf-8", errors="replace")
            except FileNotFoundError:
                continue
            if any(n in text for n in needles):
                LOG.info("role-change close witnessed in %s", f.name)
                return
        time.sleep(_POLL_INTERVAL_S)
    raise AssertionError(
        f"no mid-stream read-only refusal in {node}'s logs within {timeout_s}s — "
        f"the ingest frames never hit the demoted node's read-only gate, so the "
        f"role-change close path was not exercised"
    )


def test_graceful_demotion_mid_stream_sender_survives(
    server_factory, go_sidecar: GoSidecar, scenario_dir, log_dir: Path
) -> None:
    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    a = server_factory("a", role="primary")
    b = server_factory("b", role="replica")
    a_ports = a.start(min_http=True)
    b_ports = b.start(min_http=True)
    assert a_ports.min_http is not None, "node a: min_http port not reported (needed to demote A)"
    assert b_ports.min_http is not None, "node b: min_http port not reported (needed to promote B)"
    LOG.info("a_ports=%s b_ports=%s", a_ports, b_ports)

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

    # Settled baseline: initial rows durably acked on A, B converged.
    initial_fsn = _ingest_range_unaware(go_sidecar, start_index=0, total=_INITIAL_ROWS)
    assert go_sidecar.await_acked(initial_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"initial batch was not durably acked by A [publishedFsn={initial_fsn}]"
    )
    wait_for_dense_sequence(port=a_ports.pg, table=_TABLE,
                            expected_count=_INITIAL_ROWS, timeout_s=60.0)
    _wait_count(port=b_ports.pg, table=_TABLE, expected=_INITIAL_ROWS, timeout_s=120.0)
    LOG.info("A durably holds %d rows; B converged via replication", _INITIAL_ROWS)

    baseline = go_sidecar.stats()
    assert baseline.server_errors == 0, (
        f"pre-demote baseline already carries server errors ({baseline.server_errors})"
    )

    # Disturbance: demote A IN PLACE, underneath the producing sender.
    # wait=False so frames are in flight WHILE the role flips.
    LOG.info("submitting A PRIMARY -> REPLICA demote underneath the producing sender")
    lc.submit_switch(a_ports.min_http, "replica", wait=False)

    _ingest_range_unaware(go_sidecar, start_index=_INITIAL_ROWS, total=_WINDOW_ROWS // 2)
    lc.await_role(a_ports.min_http, "replica", timeout_s=_AWAIT_ROLE_TIMEOUT_S)
    LOG.info("A settled as REPLICA; continuing ingest into the surviving wire")

    _ingest_range_unaware(go_sidecar,
                          start_index=_INITIAL_ROWS + _WINDOW_ROWS // 2,
                          total=_WINDOW_ROWS - _WINDOW_ROWS // 2)
    LOG.info("ingested %d rows across the demote + all-replica window", _WINDOW_ROWS)

    _await_all_replica_round(go_sidecar, baseline, timeout_s=30.0)
    _await_role_change_close_evidence(log_dir, node="a", timeout_s=15.0)

    # Wire-contract pin: the demote surfaced as a role-change close, not a NACK.
    stats = go_sidecar.stats()
    assert stats.server_errors == 0, (
        f"the in-place demote surfaced as {stats.server_errors} client-visible NACK(s); "
        f"a graceful role change must close the WebSocket with a reconnect-eligible "
        f"NORMAL_CLOSURE, never an error status the client classifies"
    )

    # Promote B underneath the still-producing sender.
    LOG.info("promoting B REPLICA -> PRIMARY via /lifecycle/switch")
    lc.submit_switch(b_ports.min_http, "primary", wait=True, wait_timeout_s=_AWAIT_ROLE_TIMEOUT_S)

    final_fsn = _ingest_range_unaware(
        go_sidecar, start_index=_INITIAL_ROWS + _WINDOW_ROWS, total=_POST_ROWS
    )
    assert go_sidecar.await_acked(final_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"rows produced across the in-place demote were lost: full published "
        f"sequence not durably acked by promoted B [publishedFsn={final_fsn}]. The "
        f"frame rejected by the role-change close must replay from SF (ackedFsn+1)."
    )

    total = _INITIAL_ROWS + _WINDOW_ROWS + _POST_ROWS
    # Dense = no loss AND no duplicates: the in-flight frame rejected at the
    # demote was replayed and must land exactly once.
    wait_for_dense_sequence(port=b_ports.pg, table=_TABLE,
                            expected_count=total, timeout_s=120.0)
    LOG.info("recovered: sender rode the mid-stream demote; B holds [0..%d) exactly once", total)
