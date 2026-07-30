"""
Deterministic failover tests for the Go QWiP client against
QuestDB Enterprise.

Each test mirrors the pattern in questdb-ent/e2e/tests/test_failover.py:
start a primary, send rows via the Go sidecar, kill -9 the primary,
start a successor, and verify no rows were lost.
"""

from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path

import pytest

from lib.obj_store import ObjStore
from lib.pg_query import wait_for_dense_sequence
from lib.server import wait_port_free

LOG = logging.getLogger(__name__)


def _connect_string(http_port: int, sf_dir: Path, *,
                    request_durable_ack: bool = True,
                    reconnect_max_ms: int = 60_000,
                    close_flush_timeout_ms: int = 5_000) -> str:
    parts = [
        f"ws::addr=127.0.0.1:{http_port}",
        "username=admin",
        "password=quest",
        f"sf_dir={sf_dir}",
        f"reconnect_max_duration_millis={reconnect_max_ms}",
        f"close_flush_timeout_millis={close_flush_timeout_ms}",
    ]
    if request_durable_ack:
        parts.append("request_durable_ack=on")
    return ";".join(parts) + ";"


@pytest.mark.go_client
def test_kill9_primary_failover_no_data_loss(server_factory, go_sidecar,
                                              obj_store: ObjStore, scenario_dir: Path) -> None:
    """Kill -9 P1 mid-flight, verify P2 has every row."""
    table = "go_trades_failover"
    row_count = 50
    sf_dir = scenario_dir / "sf"

    p1 = server_factory("p1")
    p1_ports = p1.start()

    go_sidecar.connect(_connect_string(p1_ports.http, sf_dir))
    go_sidecar.send(table, count=row_count, start_index=0)
    go_sidecar.flush()

    time.sleep(0.5)

    p1.kill_9()
    wait_port_free(p1_ports.http)
    wait_port_free(p1_ports.pg)

    if p1.db_root.exists():
        shutil.rmtree(p1.db_root)
    obj_store.wipe()

    p2 = server_factory("p2", db_root_name="p2-fresh")
    p2.start(http_port=p1_ports.http, pg_port=p1_ports.pg)

    wait_for_dense_sequence(port=p1_ports.pg, table=table,
                            expected_count=row_count, timeout_s=60.0)


@pytest.mark.go_client
def test_failover_during_active_send(server_factory, go_sidecar,
                                     obj_store: ObjStore, scenario_dir: Path) -> None:
    """Kill P1 while the sender is still pushing batches."""
    table = "go_trades_inflight"
    sf_dir = scenario_dir / "sf"
    batches = 5
    rows_per_batch = 20
    expected = batches * rows_per_batch

    p1 = server_factory("p1")
    p1_ports = p1.start()
    go_sidecar.connect(_connect_string(p1_ports.http, sf_dir))

    go_sidecar.send(table, count=rows_per_batch, start_index=0)
    go_sidecar.flush()
    for i in range(1, batches):
        go_sidecar.send(table, count=rows_per_batch, start_index=i * rows_per_batch)

    p1.kill_9()
    wait_port_free(p1_ports.http)
    wait_port_free(p1_ports.pg)

    if p1.db_root.exists():
        shutil.rmtree(p1.db_root)
    obj_store.wipe()

    p2 = server_factory("p2", db_root_name="p2-fresh")
    p2.start(http_port=p1_ports.http, pg_port=p1_ports.pg)

    go_sidecar.flush()

    wait_for_dense_sequence(port=p1_ports.pg, table=table,
                            expected_count=expected, timeout_s=60.0)


@pytest.mark.go_client
def test_two_failovers_in_one_scenario(server_factory, go_sidecar,
                                       obj_store: ObjStore, scenario_dir: Path) -> None:
    """Multiple failovers in a row — no row should be lost."""
    table = "go_trades_two_fail"
    sf_dir = scenario_dir / "sf"
    rows_per_phase = 25
    expected = rows_per_phase * 3

    # Phase 1.
    p1 = server_factory("p1")
    p1_ports = p1.start()
    go_sidecar.connect(_connect_string(p1_ports.http, sf_dir))
    go_sidecar.send(table, count=rows_per_phase, start_index=0)
    go_sidecar.flush()
    time.sleep(0.5)
    p1.kill_9()
    wait_port_free(p1_ports.http)
    wait_port_free(p1_ports.pg)
    if p1.db_root.exists():
        shutil.rmtree(p1.db_root)
    obj_store.wipe()

    # Phase 2.
    p2 = server_factory("p2", db_root_name="p2-fresh")
    p2.start(http_port=p1_ports.http, pg_port=p1_ports.pg)
    go_sidecar.send(table, count=rows_per_phase, start_index=rows_per_phase)
    go_sidecar.flush()
    time.sleep(0.5)
    p2.kill_9()
    wait_port_free(p1_ports.http)
    wait_port_free(p1_ports.pg)
    if p2.db_root.exists():
        shutil.rmtree(p2.db_root)
    obj_store.wipe()

    # Phase 3.
    p3 = server_factory("p3", db_root_name="p3-fresh")
    p3.start(http_port=p1_ports.http, pg_port=p1_ports.pg)
    go_sidecar.send(table, count=rows_per_phase, start_index=rows_per_phase * 2)
    go_sidecar.flush()

    wait_for_dense_sequence(port=p1_ports.pg, table=table,
                            expected_count=expected, timeout_s=90.0)


@pytest.mark.go_client
def test_ok_trim_loses_rows_without_durable_ack(server_factory, go_sidecar,
                                                 obj_store: ObjStore, scenario_dir: Path) -> None:
    """The negative control: with request_durable_ack=off the SF trims on
    OK, so killing P1 between OK and WAL upload and wiping everything
    should lose rows. This proves the failover tests above pass *because
    of* durable-ack, not by luck — and that the harness detects loss."""
    table = "go_trades_no_durable"
    sf_dir = scenario_dir / "sf"
    row_count = 50

    p1 = server_factory("p1")
    p1_ports = p1.start()

    go_sidecar.connect(_connect_string(p1_ports.http, sf_dir, request_durable_ack=False))
    go_sidecar.send(table, count=row_count, start_index=0)
    fsn = go_sidecar.flush()
    go_sidecar.await_acked(fsn, timeout_ms=30_000)

    p1.kill_9()
    wait_port_free(p1_ports.http)
    wait_port_free(p1_ports.pg)

    if p1.db_root.exists():
        shutil.rmtree(p1.db_root)
    obj_store.wipe()

    p2 = server_factory("p2", db_root_name="p2-fresh")
    p2_ports = p2.start(http_port=p1_ports.http, pg_port=p1_ports.pg)

    time.sleep(5)

    import psycopg
    try:
        conn = psycopg.connect(
            f"host=127.0.0.1 port={p2_ports.pg} user=admin password=quest dbname=qdb",
            autocommit=True,
        )
        cur = conn.execute(f"SELECT count() FROM '{table}'")
        actual = cur.fetchone()[0]
        conn.close()
    except Exception:
        actual = 0

    assert actual < row_count, (
        f"Expected data loss without durable-ack but got {actual}/{row_count} rows"
    )
