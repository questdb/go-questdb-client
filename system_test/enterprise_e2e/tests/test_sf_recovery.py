"""
Sender-crash store-and-forward recovery e2e tests for the Go client.

Ports the sender-SIGKILL recovery cases from the Enterprise reference
``questdb-ent/e2e/tests/test_failover.py``. Where the failover tests kill
the *server*, these kill the *sender*: SIGKILL the sidecar mid-flight,
bring up a fresh sidecar on the same ``sf_dir``/``sender_id``, and require
it to recover the on-disk ``.sfa`` segments its dead predecessor left and
replay them to the still-alive primary.

Go slot layout differs from the Java pooled facade: Go writes segments
directly under ``<sf_dir>/<sender_id>/`` (that per-sender directory *is*
the slot — there is no ``-0`` slot level), so the recovery-surface globs
below target ``sf_dir / sender_id`` rather than ``sf_dir / f"{sender_id}-0"``.

Durable-ack is now implemented, so these are real tests: every test
connects with ``request_durable_ack=on`` (so server OKs don't trim the SF
before the sender is killed) and the restarted sender must recover the
on-disk ``.sfa`` segments and replay them.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pytest

from lib.pg_query import execute_ddl, wait_for_dense_sequence

from go_sidecar import GoSidecar

LOG = logging.getLogger(__name__)

pytestmark = [pytest.mark.go_client]

# Go slot dir for sender_id=primary: <sf_dir>/<sender_id> (no -0 level).
_SENDER_ID = "primary"


def _dedup_ddl(table: str) -> str:
    # DEDUP on (timestamp, v): the sidecar emits one row per v at a unique
    # timestamp, so replay of already-delivered frames collapses, while gaps
    # and corruption do not — the escape valve for same-server replay after a
    # sender crash (request_durable_ack prevents loss, not duplicates).
    return (
        f'CREATE TABLE "{table}" ('
        "v LONG, "
        "timestamp TIMESTAMP"
        ") TIMESTAMP(timestamp) PARTITION BY DAY WAL "
        "DEDUP UPSERT KEYS(timestamp, v)"
    )


def _connect_string(http_port: int, sf_dir: Path, *, extra: str = "") -> str:
    return (
        f"ws::addr=127.0.0.1:{http_port};"
        "username=admin;password=quest;"
        f"sf_dir={sf_dir};"
        f"sender_id={_SENDER_ID};"
        # Only STATUS_DURABLE_ACK (gated on WAL upload, slower than the kill
        # window) trims — plain OK must not. So the SF still holds every frame
        # at SIGKILL time and the restart exercises the recovery path.
        "request_durable_ack=on;"
        "reconnect_max_duration_millis=60000;"
        "close_flush_timeout_millis=5000;"
        f"{extra}"
    )


def test_sender_kill9_sf_recovery_replays(
    server_factory, go_sidecar: GoSidecar, scenario_dir: Path, log_dir: Path
) -> None:
    """SIGKILL the sender mid-flight, then bring up a fresh sender on the
    same slot. The new process must recover the on-disk SF segments and
    replay them to the still-alive primary; DEDUP collapses any frames the
    first sender already delivered."""
    table = "go_trades_sender_kill"
    row_count = 100
    sf_dir = scenario_dir / "sf"

    p1 = server_factory("p1")
    p1_ports = p1.start()
    execute_ddl(port=p1_ports.pg, ddl=_dedup_ddl(table))

    cs = _connect_string(p1_ports.http, sf_dir)
    go_sidecar.connect(cs)
    go_sidecar.send(table, count=row_count, start_index=0)
    flushed_fsn = go_sidecar.flush()
    LOG.info("first sender flushed up to fsn=%d before SIGKILL", flushed_fsn)
    # Small settle so some frames have left the wire and P1 has OK'd them —
    # the more interesting recovery path (warm cursor) than a cold replay.
    time.sleep(0.5)

    go_sidecar.kill_9()
    assert go_sidecar.process is not None
    assert go_sidecar.process.poll() is not None, "sidecar must be dead before recovery"

    # Sanity: the slot dir should still hold .sfa segments, else the test
    # isn't exercising recovery. Go slot dir is <sf_dir>/<sender_id>.
    slot_dir = sf_dir / _SENDER_ID
    sfa_files = list(slot_dir.glob("sf-*.sfa")) if slot_dir.exists() else []
    assert sfa_files, (
        f"expected un-trimmed .sfa segments in {slot_dir} after SIGKILL; "
        f"test is not exercising the recovery path. dir contents: "
        f"{list(slot_dir.iterdir()) if slot_dir.exists() else '<missing>'}"
    )
    LOG.info("recovery surface: %d .sfa file(s) survived SIGKILL in %s",
             len(sfa_files), slot_dir)

    sidecar2 = GoSidecar(log_dir=log_dir, name="go-sidecar-restart")
    sidecar2.start()
    try:
        sidecar2.connect(cs)
        # Recovery: the engine opens the slot, scans .sfa, rebuilds the
        # cursor, reconnects to P1, and replays every recovered frame.
        wait_for_dense_sequence(port=p1_ports.pg, table=table,
                                expected_count=row_count, timeout_s=60.0)
    finally:
        sidecar2.stop()


def test_sender_repeated_sigkill_no_state_corruption(
    server_factory, go_sidecar: GoSidecar, scenario_dir: Path, log_dir: Path
) -> None:
    """Multi-cycle SIGKILL torture. Kill-and-restart the sender N times
    against the same primary and slot; the on-disk slot state must stay
    consistent through every recovery. ``sf_max_bytes`` forces frequent
    rotation so each cycle leaves multiple sealed segments behind, and the
    final dense oracle checks the union [0..N) across all cycles."""
    table = "go_trades_multi_cycle"
    cycles = 6
    rows_per_cycle = 200
    total = cycles * rows_per_cycle
    sf_dir = scenario_dir / "sf"
    # Deliberate variety: pre-wire kill, mid-OK kill, post-OK-but-pre-durable
    # kill. Each timing exercises a different intra-cycle ack/trim state.
    settle_secs = [0.0, 0.1, 0.3, 0.0, 0.2, 0.5]
    assert len(settle_secs) == cycles

    p1 = server_factory("p1")
    p1_ports = p1.start()
    execute_ddl(port=p1_ports.pg, ddl=_dedup_ddl(table))

    cs = _connect_string(p1_ports.http, sf_dir, extra="sf_max_bytes=8192;")

    fixture_consumed = False
    for cycle in range(cycles):
        if not fixture_consumed:
            current = go_sidecar
            fixture_consumed = True
        else:
            current = GoSidecar(log_dir=log_dir, name=f"go-sidecar-cycle{cycle}")
            current.start()
        current.connect(cs)
        start = cycle * rows_per_cycle
        current.send(table, count=rows_per_cycle, start_index=start)
        current.flush()
        if settle_secs[cycle] > 0:
            time.sleep(settle_secs[cycle])
        current.kill_9()
        LOG.info("cycle %d/%d: killed after %dms settle",
                 cycle + 1, cycles, int(settle_secs[cycle] * 1000))

    slot_dir = sf_dir / _SENDER_ID
    sfa_files = list(slot_dir.glob("sf-*.sfa")) if slot_dir.exists() else []
    assert sfa_files, (
        f"expected un-trimmed .sfa segments in {slot_dir} after {cycles} SIGKILL "
        f"cycles; recovery path is not under test. dir contents: "
        f"{list(slot_dir.iterdir()) if slot_dir.exists() else '<missing>'}"
    )
    LOG.info("recovery surface after %d kills: %d .sfa file(s) in %s",
             cycles, len(sfa_files), slot_dir)

    final = GoSidecar(log_dir=log_dir, name="go-sidecar-final")
    final.start()
    try:
        final.connect(cs)
        # Dense oracle over the union of every cycle's range: catches rows
        # lost in any cycle, rows shifted by replay corruption, and duplicate
        # rows DEDUP failed to collapse.
        wait_for_dense_sequence(port=p1_ports.pg, table=table,
                                expected_count=total, timeout_s=180.0)
    finally:
        final.stop()


def test_partial_ack_sealed_segment_replay_dedup_collapses(
    server_factory, go_sidecar: GoSidecar, scenario_dir: Path, log_dir: Path
) -> None:
    """Recovery through a partially-acked surviving sealed segment: the
    persisted ``.ack-watermark`` carries the previous sender's durable-ack
    high-water mark across the SIGKILL boundary, so the new sender positions
    past the already-acked prefix instead of re-replaying it. Any small
    residual re-replay is absorbed by DEDUP."""
    table = "go_trades_partial_ack"
    batch_size = 200
    batches = 10
    row_count = batch_size * batches
    sf_dir = scenario_dir / "sf"

    p1 = server_factory("p1")
    p1_ports = p1.start()
    execute_ddl(port=p1_ports.pg, ddl=_dedup_ddl(table))

    cs = _connect_string(p1_ports.http, sf_dir, extra="sf_max_bytes=32768;")
    go_sidecar.connect(cs)
    # Many smaller flushes -> many frames -> exercise the
    # multiple-frames-per-segment path that makes partial-ack possible.
    for b in range(batches):
        go_sidecar.send(table, count=batch_size, start_index=b * batch_size)
        go_sidecar.flush()

    # Long enough for the WAL apply + durable-ack cadence to durably-ack and
    # trim the early sealed segments, but short enough that the most-recent
    # segments are still partially/fully un-durably-acked at kill time.
    time.sleep(4.0)

    slot_dir = sf_dir / _SENDER_ID
    pre_kill = sorted(slot_dir.glob("sf-*.sfa")) if slot_dir.exists() else []
    LOG.info("pre-kill SF surface: %d .sfa file(s) survived partial trim", len(pre_kill))
    assert pre_kill, (
        "expected at least one un-durably-acked .sfa segment at kill time; "
        "partial-ack recovery path is not under test."
    )

    go_sidecar.kill_9()

    sidecar2 = GoSidecar(log_dir=log_dir, name="go-sidecar-restart")
    sidecar2.start()
    try:
        sidecar2.connect(cs)
        # DEDUP collapses any re-replayed frames whose (timestamp, v) the
        # first sender already delivered; the watermark eliminates the bulk
        # of the re-replay surface.
        wait_for_dense_sequence(port=p1_ports.pg, table=table,
                                expected_count=row_count, timeout_s=120.0)
    finally:
        sidecar2.stop()
