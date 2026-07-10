"""
Production-faithful two-node graceful failover round-trip (Go client).

Ports the Enterprise reference ``questdb-ent/e2e/tests/test_failover_graceful.py``
to the Go ingress sidecar. Two forked JVMs share an ``fs::`` object store;
A boots primary, B boots replica. A symmetric demote-then-promote:

  A (primary) -> A (replica)   [demote A via /lifecycle/switch]
  B (replica) -> B (primary)   [promote B via /lifecycle/switch]

Then ingest into the newly-promoted B and assert A (now a replica) downloads
B's post-promote rows — onward-replication proof. Also asserts A refuses
writes after demotion (pg-wire + /exec), the pre-opened pg connection
survives the demotion with a clean rejection, the object store is never
wiped, and both nodes shut down cleanly.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import psycopg
import pytest

from lib import lifecycle as lc
from lib.pg_query import count_rows, wait_for_dense_sequence
from lib.shutdown import assert_clean_shutdown

from go_sidecar import GoSidecar

LOG = logging.getLogger(__name__)

pytestmark = [pytest.mark.go_client]

_TABLE = "go_graceful_failover"
_POLL_INTERVAL_S = 0.3
_AWAIT_ROLE_TIMEOUT_S = 60.0
_DURABLE_ACK_AWAIT_TIMEOUT_MS = 60_000
_INITIAL_ROWS = 30
_ONWARD_ROWS = 20


def _wait_replica_converges(*, pg_port: int, table: str, expected: int,
                            timeout_s: float = 90.0) -> None:
    """Block until the replica's pg-wire count reaches ``expected`` (the table
    is populated purely by WAL download, so a count match suffices)."""
    deadline = time.monotonic() + timeout_s
    last = -1
    while time.monotonic() < deadline:
        try:
            last = count_rows(port=pg_port, table=table, timeout_s=5.0)
        except TimeoutError:
            last = -1
        if last == expected:
            LOG.info("replica converged: count=%d == expected=%d", last, expected)
            return
        time.sleep(_POLL_INTERVAL_S)
    raise AssertionError(
        f"replica did not converge on {expected} rows in {table} within {timeout_s}s "
        f"(last observed: {last})"
    )


def test_graceful_failover_round_trip(
    server_factory, go_sidecar: GoSidecar, obj_store, scenario_dir: Path
) -> None:
    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    a = server_factory("a", role="primary")
    b = server_factory("b", role="replica")
    a_ports = a.start(min_http=True)
    b_ports = b.start(min_http=True)
    assert a_ports.min_http is not None, "node a: min_http port not reported"
    assert b_ports.min_http is not None, "node b: min_http port not reported"
    LOG.info("a_ports=%s b_ports=%s", a_ports, b_ports)

    # Ingest initial rows into A via the Go QWP sidecar (durable-ack).
    connect_str = (
        f"ws::addr=127.0.0.1:{a_ports.http}"
        ";username=admin;password=quest"
        f";sf_dir={sf_dir}"
        ";request_durable_ack=on"
        ";reconnect_max_duration_millis=60000"
        ";close_flush_timeout_millis=5000;"
    )
    go_sidecar.connect(connect_str)
    go_sidecar.send(_TABLE, count=_INITIAL_ROWS, start_index=0)
    initial_fsn = go_sidecar.flush()
    assert go_sidecar.await_acked(initial_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"initial batch was not durably acked by A before demote [publishedFsn={initial_fsn}]"
    )
    wait_for_dense_sequence(port=a_ports.pg, table=_TABLE,
                            expected_count=_INITIAL_ROWS, timeout_s=60.0)
    _wait_replica_converges(pg_port=b_ports.pg, table=_TABLE, expected=_INITIAL_ROWS, timeout_s=120.0)
    LOG.info("B caught up with A's initial %d rows", _INITIAL_ROWS)

    # Open a pg-wire connection to A BEFORE the demotion (connection-survival witness).
    survivor_conn = psycopg.connect(
        host="127.0.0.1", port=a_ports.pg, user="admin", password="quest",
        dbname="qdb", connect_timeout=10, autocommit=True,
    )
    try:
        LOG.info("demoting A PRIMARY -> REPLICA via /lifecycle/switch")
        lc.submit_switch(a_ports.min_http, "replica", wait=True, wait_timeout_s=_AWAIT_ROLE_TIMEOUT_S)
        snap_a = lc.lifecycle(a_ports.min_http)
        assert snap_a.get("currentRole") == "REPLICA", (
            f"A should be REPLICA after demotion, got {snap_a.get('currentRole')!r}"
        )

        _assert_write_rejected_pg(pg_port=a_ports.pg, table=_TABLE)
        _assert_existing_conn_rejects_write(survivor_conn, table=_TABLE)
        _assert_write_rejected_http(http_port=a_ports.http, table=_TABLE)

        LOG.info("promoting B REPLICA -> PRIMARY via /lifecycle/switch")
        lc.submit_switch(b_ports.min_http, "primary", wait=True, wait_timeout_s=_AWAIT_ROLE_TIMEOUT_S)
        snap_b = lc.lifecycle(b_ports.min_http)
        assert snap_b.get("currentRole") == "PRIMARY", (
            f"B should be PRIMARY after promotion, got {snap_b.get('currentRole')!r}"
        )

        # Ingest onward rows into B (now the primary): fresh sender/slot on B.
        go_sidecar.close()
        sf_dir_b = scenario_dir / "sf_b"
        sf_dir_b.mkdir(parents=True, exist_ok=True)
        connect_str_b = (
            f"ws::addr=127.0.0.1:{b_ports.http}"
            ";username=admin;password=quest"
            f";sf_dir={sf_dir_b}"
            ";request_durable_ack=on"
            ";reconnect_max_duration_millis=60000"
            ";close_flush_timeout_millis=5000;"
        )
        go_sidecar.connect(connect_str_b)
        go_sidecar.send(_TABLE, count=_ONWARD_ROWS, start_index=_INITIAL_ROWS)
        onward_fsn = go_sidecar.flush()
        assert go_sidecar.await_acked(onward_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
            f"onward batch was not durably acked by B [publishedFsn={onward_fsn}]"
        )

        total_expected = _INITIAL_ROWS + _ONWARD_ROWS
        wait_for_dense_sequence(port=b_ports.pg, table=_TABLE,
                                expected_count=total_expected, timeout_s=60.0)

        # A (now a replica) must download B's onward rows — onward replication.
        _wait_replica_converges(pg_port=a_ports.pg, table=_TABLE,
                                expected=total_expected, timeout_s=120.0)
        LOG.info("A (now replica) converged on B's %d total rows — onward replication confirmed",
                 total_expected)
    finally:
        survivor_conn.close()

    go_sidecar.close()
    b.stop()
    a.stop()
    assert_clean_shutdown(b)
    assert_clean_shutdown(a)


# ---- write-rejection helpers ----


def _assert_write_rejected_pg(*, pg_port: int, table: str) -> None:
    try:
        with psycopg.connect(
            host="127.0.0.1", port=pg_port, user="admin", password="quest",
            dbname="qdb", connect_timeout=5, autocommit=True,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO "{table}" ("timestamp", v) VALUES (now(), 99_999);')
        raise AssertionError(
            f"INSERT was accepted on demoted REPLICA (pg-wire :{pg_port}); "
            f"expected a clean 'replica access is read-only' rejection"
        )
    except psycopg.DatabaseError as exc:
        assert "read-only" in str(exc).lower(), (
            f"INSERT raised DatabaseError without 'read-only': {exc!r}"
        )
        LOG.info("write-rejection confirmed (pg-wire): %s", exc)


def _assert_existing_conn_rejects_write(conn: psycopg.Connection, *, table: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(f'INSERT INTO "{table}" ("timestamp", v) VALUES (now(), 88_888);')
        raise AssertionError(
            "pre-opened connection accepted INSERT on demoted REPLICA; expected rejection"
        )
    except psycopg.DatabaseError as exc:
        assert "read-only" in str(exc).lower(), (
            f"pre-opened connection raised DatabaseError without 'read-only': {exc!r}"
        )
        LOG.info("connection survival confirmed; write cleanly rejected: %s", exc)


def _assert_write_rejected_http(*, http_port: int, table: str) -> None:
    import http.client
    import urllib.parse

    sql = f'INSERT INTO "{table}" ("timestamp", v) VALUES (now(), 77_777)'
    path = "/exec?query=" + urllib.parse.quote_plus(sql)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", http_port, timeout=5)
        try:
            conn.request("GET", path, headers={"Authorization": "Basic YWRtaW46cXVlc3Q="})
            resp = conn.getresponse()
            status = resp.status
            body = resp.read().decode("utf-8", errors="replace")
        finally:
            conn.close()
    except Exception as exc:
        raise AssertionError(
            f"web-http /exec INSERT raised a connection error on demoted REPLICA: {exc!r}"
        ) from exc

    if 200 <= status < 300:
        raise AssertionError(
            f"web-http /exec INSERT accepted (HTTP {status}) on demoted REPLICA; "
            f"expected 403. Body: {body!r}"
        )
    assert status == 403, f"expected HTTP 403 on demoted REPLICA, got {status}. Body: {body!r}"
    assert "read-only" in body.lower(), f"HTTP 403 body missing 'read-only': {body!r}"
    LOG.info("write-rejection confirmed (web-http /exec): HTTP 403")
