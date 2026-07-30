"""
Transactional-atomicity kill tests for the Go client.

Ports the Enterprise reference
``questdb-ent/e2e/tests/test_txn_kill9_atomicity.py``. With
``transaction=on`` the client publishes auto-flushed (or explicitly
``FLUSH_DEFER``-ed) batches as deferred-commit frames; only an explicit
``FLUSH`` commits. A producer that dies mid-transaction leaves (a) an
uncommitted server-side tail that must roll back and never become visible,
and (b) an orphan deferred tail in its SF log that a successor must RETIRE,
not replay.

Go slot dir is ``<sf_dir>/<sender_id>`` (no ``-0`` level), unlike the Java
pooled facade.

STILL RED: durable-ack is implemented now, but deferred-commit
transactions are not — ``transaction=on`` is rejected by the config
parser, so every test here fails at the first ``connect`` with a
:class:`GoSidecarError` (were it to get further, the sidecar also rejects
``FLUSH_DEFER``). The module ``xfail`` (``raises=GoSidecarError``,
``strict=True``) pins that; the bodies run for real once transactions land.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pytest

from lib.pg_query import count_rows, execute_ddl, fetch_column_sorted, wait_for_count

from go_sidecar import GoSidecar, GoSidecarError

LOG = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.go_client,
    pytest.mark.xfail(
        reason="transaction=on (deferred-commit) is not yet implemented in the Go client "
        "(rejected at CONNECT); FLUSH_DEFER is likewise unsupported",
        raises=GoSidecarError,
        strict=True,
    ),
]

# How long we watch the row count after a mid-transaction kill to assert the
# uncommitted rows never surface. Rollback happens on disconnect.
_STABILITY_WINDOW_S = 3.0


def _txn_connect_string(http_port: int, sf_dir: Path, *, sender_id: str,
                        auto_flush_rows: int | None = None) -> str:
    parts = [
        f"ws::addr=127.0.0.1:{http_port}",
        "username=admin",
        "password=quest",
        f"sf_dir={sf_dir}",
        f"sender_id={sender_id}",
        # The subject under test: deferred-commit framing.
        "transaction=on",
        # OK from the server must not trim the SF — only STATUS_DURABLE_ACK
        # (gated on WAL upload, slower than the kill window) trims — so
        # committed frames stay in the log for the slow recovery path.
        "request_durable_ack=on",
        "reconnect_max_duration_millis=60000",
        "close_flush_timeout_millis=5000",
    ]
    if auto_flush_rows is not None:
        parts.append(f"auto_flush_rows={auto_flush_rows}")
    return ";".join(parts) + ";"


def _create_table(pg_port: int, table: str, *, dedup: bool) -> None:
    """DEDUP on (timestamp, v) is the escape valve for committed-frame replay
    after a sender crash; tests that never commit anything before the kill
    omit it to prove atomicity needs no dedup."""
    ddl = (
        f'CREATE TABLE "{table}" ('
        "v LONG, "
        "timestamp TIMESTAMP"
        ") TIMESTAMP(timestamp) PARTITION BY DAY WAL"
    )
    if dedup:
        ddl += " DEDUP UPSERT KEYS(timestamp, v)"
    execute_ddl(port=pg_port, ddl=ddl)


def _wait_frames_sent_above(go_sidecar: GoSidecar, baseline: int, *,
                            timeout_s: float = 15.0) -> int:
    """Block until the I/O loop has pushed at least one frame beyond
    ``baseline`` onto the wire, so the SERVER holds deferred frames at kill
    time (exercising server-side rollback, not just the never-sent case)."""
    deadline = time.monotonic() + timeout_s
    sent = -1
    while time.monotonic() < deadline:
        sent = go_sidecar.stats().sent
        if sent > baseline:
            return sent
        time.sleep(0.1)
    raise AssertionError(
        f"I/O loop did not advance past baseline={baseline} frames within "
        f"{timeout_s}s (last sent={sent}); deferred frames never reached the wire"
    )


def _assert_count_stable(pg_port: int, table: str, expected: int, *,
                         hold_s: float = _STABILITY_WINDOW_S) -> None:
    """Assert the visible row count equals ``expected`` and STAYS there for
    ``hold_s``. A transient appearance of uncommitted rows is a rollback bug
    even if they later vanish."""
    deadline = time.monotonic() + hold_s
    while time.monotonic() < deadline:
        observed = count_rows(port=pg_port, table=table)
        assert observed == expected, (
            f"atomicity violation: {table} shows {observed} rows, expected "
            f"{expected} (uncommitted transaction rows surfaced?)"
        )
        time.sleep(0.25)


def _assert_orphan_tail_on_disk(sf_dir: Path, sender_id: str) -> None:
    """Guard: the killed sender's slot dir must still hold .sfa segments, else
    the restart would not exercise the recovery/retirement path. Go slot dir
    is <sf_dir>/<sender_id> (no -0 level)."""
    slot_dir = sf_dir / sender_id
    sfa_files = list(slot_dir.glob("sf-*.sfa")) if slot_dir.exists() else []
    assert sfa_files, (
        f"expected .sfa segments in {slot_dir} after SIGKILL; the restart would "
        f"not exercise orphan-tail retirement. dir contents: "
        f"{list(slot_dir.iterdir()) if slot_dir.exists() else '<missing>'}"
    )
    LOG.info("recovery surface: %d .sfa file(s) in %s", len(sfa_files), slot_dir)


def test_kill9_mid_txn_uncommitted_rows_never_appear(
        server_factory, go_sidecar: GoSidecar, scenario_dir: Path, log_dir: Path) -> None:
    """Commit txn A, open txn B, SIGKILL between its deferred auto-flushes,
    restart on the same slot. The uncommitted txn-B rows must never appear;
    recovery replays committed frames below the tail (DEDUP collapses) and
    retires the orphan tail. Final oracle: exactly txn A + txn C."""
    table = "go_trades_txn_atomicity"
    sf_dir = scenario_dir / "sf"
    sender_id = "txn"

    p1 = server_factory("p1")
    p1_ports = p1.start()
    _create_table(p1_ports.pg, table, dedup=True)

    cs = _txn_connect_string(p1_ports.http, sf_dir, sender_id=sender_id,
                             auto_flush_rows=10)
    go_sidecar.connect(cs)

    # Txn A: committed.
    go_sidecar.send(table, count=40, start_index=0)
    committed_fsn = go_sidecar.flush()
    LOG.info("txn A committed up to fsn=%d", committed_fsn)
    wait_for_count(port=p1_ports.pg, table=table, expected=40, timeout_s=60.0)

    # Txn B: opened, never committed. auto_flush_rows=10 -> 2 deferred frames
    # published (20 rows) + 5 buffered rows that die with the process.
    sent_baseline = go_sidecar.stats().sent
    go_sidecar.send(table, count=25, start_index=100)
    sent_now = _wait_frames_sent_above(go_sidecar, sent_baseline)
    LOG.info("txn B deferred frames on the wire (sent %d -> %d); killing",
             sent_baseline, sent_now)

    go_sidecar.kill_9()
    assert go_sidecar.process is not None
    assert go_sidecar.process.poll() is not None, "sidecar must be dead"

    _assert_orphan_tail_on_disk(sf_dir, sender_id)

    # Server-side rollback: the count must hold at exactly txn A's 40 rows.
    _assert_count_stable(p1_ports.pg, table, 40)

    sidecar2 = GoSidecar(log_dir=log_dir, name="go-sidecar-txn-restart")
    sidecar2.start()
    try:
        sidecar2.connect(cs)
        # Txn C proves the recovered slot is fully writable post-retirement.
        sidecar2.send(table, count=30, start_index=200)
        sidecar2.flush()
        wait_for_count(port=p1_ports.pg, table=table, expected=70, timeout_s=60.0)
    finally:
        sidecar2.stop()

    values = fetch_column_sorted(port=p1_ports.pg, table=table, column="v")
    assert values == list(range(0, 40)) + list(range(200, 230)), (
        f"final table contents wrong; leaked txn-B rows: "
        f"{[v for v in values if 100 <= v < 125]}, full: {values}"
    )


def test_kill9_whole_log_deferred_fast_path_retirement(
        server_factory, go_sidecar: GoSidecar, scenario_dir: Path, log_dir: Path) -> None:
    """Nothing was ever committed before the kill: the entire SF log is one
    orphan deferred tail. Recovery must take the fast path — retire the whole
    log before any send. NO dedup on the table, so a replayed tail lands and
    fails loudly rather than being masked."""
    table = "go_trades_txn_fastpath"
    sf_dir = scenario_dir / "sf"
    sender_id = "txnfast"

    p1 = server_factory("p1")
    p1_ports = p1.start()
    _create_table(p1_ports.pg, table, dedup=False)

    cs = _txn_connect_string(p1_ports.http, sf_dir, sender_id=sender_id,
                             auto_flush_rows=10)
    go_sidecar.connect(cs)

    # 35 rows -> 3 deferred frames durable in SF, 5 buffered. No FLUSH, ever.
    go_sidecar.send(table, count=35, start_index=0)
    _wait_frames_sent_above(go_sidecar, 0)

    go_sidecar.kill_9()
    assert go_sidecar.process is not None
    assert go_sidecar.process.poll() is not None, "sidecar must be dead"

    _assert_orphan_tail_on_disk(sf_dir, sender_id)

    # The table must remain empty: uncommitted rows rolled back on disconnect.
    _assert_count_stable(p1_ports.pg, table, 0)

    sidecar2 = GoSidecar(log_dir=log_dir, name="go-sidecar-fast-restart")
    sidecar2.start()
    try:
        # Recovery: whole log deferred -> fast-path retirement before any send.
        sidecar2.connect(cs)
        sidecar2.send(table, count=20, start_index=500)
        sidecar2.flush()
        wait_for_count(port=p1_ports.pg, table=table, expected=20, timeout_s=60.0)
    finally:
        sidecar2.stop()

    values = fetch_column_sorted(port=p1_ports.pg, table=table, column="v")
    assert values == list(range(500, 520)), (
        f"fast-path retirement leaked killed-transaction rows: "
        f"{[v for v in values if v < 500]}, full: {values}"
    )


def test_kill9_between_deferred_flushes_deterministic(
        server_factory, go_sidecar: GoSidecar, scenario_dir: Path, log_dir: Path) -> None:
    """The literal 'SIGKILL between deferred flushes' scenario, made
    deterministic with the FLUSH_DEFER verb. Commit txn A; within open txn B
    run two explicit deferred publishes (asserting the published FSN advances
    across them), buffer a remainder, SIGKILL. Restart, commit txn C, assert
    txn B is gone in its entirety."""
    table = "go_trades_txn_deferred"
    sf_dir = scenario_dir / "sf"
    sender_id = "txndefer"

    p1 = server_factory("p1")
    p1_ports = p1.start()
    _create_table(p1_ports.pg, table, dedup=True)

    cs = _txn_connect_string(p1_ports.http, sf_dir, sender_id=sender_id)
    go_sidecar.connect(cs)

    # Txn A: committed.
    go_sidecar.send(table, count=20, start_index=0)
    go_sidecar.flush()
    wait_for_count(port=p1_ports.pg, table=table, expected=20, timeout_s=60.0)

    # Txn B: two explicit deferred publishes + a buffered remainder.
    go_sidecar.send(table, count=10, start_index=100)
    fsn1 = go_sidecar.flush_defer()
    assert fsn1 > 0, f"first FLUSH_DEFER published nothing (fsn={fsn1})"
    go_sidecar.send(table, count=10, start_index=110)
    fsn2 = go_sidecar.flush_defer()
    assert fsn2 > fsn1, (
        f"second FLUSH_DEFER did not advance the published fsn ({fsn1} -> {fsn2}); "
        "there are not really two deferred flushes to be 'between'"
    )
    go_sidecar.send(table, count=5, start_index=120)  # buffered remainder, never published

    go_sidecar.kill_9()
    assert go_sidecar.process is not None
    assert go_sidecar.process.poll() is not None, "sidecar must be dead"

    _assert_orphan_tail_on_disk(sf_dir, sender_id)
    _assert_count_stable(p1_ports.pg, table, 20)

    sidecar2 = GoSidecar(log_dir=log_dir, name="go-sidecar-defer-restart")
    sidecar2.start()
    try:
        sidecar2.connect(cs)
        sidecar2.send(table, count=20, start_index=300)
        sidecar2.flush()
        wait_for_count(port=p1_ports.pg, table=table, expected=40, timeout_s=60.0)
    finally:
        sidecar2.stop()

    values = fetch_column_sorted(port=p1_ports.pg, table=table, column="v")
    assert values == list(range(0, 20)) + list(range(300, 320)), (
        f"deferred txn-B rows leaked across recovery: "
        f"{[v for v in values if 100 <= v < 125]}, full: {values}"
    )


def test_kill9_at_commit_boundary_no_orphan(
        server_factory, go_sidecar: GoSidecar, scenario_dir: Path, log_dir: Path) -> None:
    """Inverse guard: the log ends WITH a commit, so nothing may be retired —
    every committed row must survive recovery. Over-eager tail retirement
    would fail this one."""
    table = "go_trades_txn_boundary"
    sf_dir = scenario_dir / "sf"
    sender_id = "txnboundary"

    p1 = server_factory("p1")
    p1_ports = p1.start()
    _create_table(p1_ports.pg, table, dedup=True)

    cs = _txn_connect_string(p1_ports.http, sf_dir, sender_id=sender_id)
    go_sidecar.connect(cs)

    # Txn A: committed, then SIGKILL with the log ending on a commit boundary.
    go_sidecar.send(table, count=40, start_index=0)
    committed_fsn = go_sidecar.flush()
    assert committed_fsn > 0
    go_sidecar.kill_9()
    assert go_sidecar.process is not None
    assert go_sidecar.process.poll() is not None, "sidecar must be dead"

    sidecar2 = GoSidecar(log_dir=log_dir, name="go-sidecar-boundary-restart")
    sidecar2.start()
    try:
        sidecar2.connect(cs)
        # Committed rows must survive recovery untouched.
        wait_for_count(port=p1_ports.pg, table=table, expected=40, timeout_s=60.0)
        sidecar2.send(table, count=10, start_index=200)
        sidecar2.flush()
        wait_for_count(port=p1_ports.pg, table=table, expected=50, timeout_s=60.0)
    finally:
        sidecar2.stop()

    values = fetch_column_sorted(port=p1_ports.pg, table=table, column="v")
    assert values == list(range(0, 40)) + list(range(200, 210)), (
        f"commit-boundary recovery altered committed rows: {values}"
    )
