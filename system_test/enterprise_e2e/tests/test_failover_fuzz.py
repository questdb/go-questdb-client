"""
Randomized failover fuzz (Go client).

Ports the Enterprise reference ``questdb-ent/e2e/tests/test_failover_fuzz.py``
to the Go ingress sidecar. Same shape — "send batches, disturb primary,
verify content" — but rolls dice for row counts, batch counts, settle time,
and whether to do one or two disturbances. Disturbance type: kill-restart
only (kill -9 the primary, wipe the object store, start a fresh successor on
the same ports), exercising the durable-ack / reconnect-replay path.

Verification is a dense-sequence *content* oracle: each row's ``v`` equals
its global index, so the survivor must contain exactly ``v in [0, flushed)``
with no gaps, duplicates, or shifts.

Runs in a dedicated fuzz tier (``-m fuzz``); iteration count comes from
``QDB_E2E_FUZZ_ITERS`` (falling back to ``E2E_FUZZ_ITERS``, default 10) and
the seed from ``QDB_E2E_FUZZ_SEED``.
"""

from __future__ import annotations

import logging
import os
import random
import shutil
import time
from pathlib import Path

import pytest

from lib.obj_store import ObjStore
from lib.pg_query import wait_for_dense_sequence
from lib.server import wait_port_free

from go_sidecar import GoSidecar

LOG = logging.getLogger(__name__)

pytestmark = [pytest.mark.fuzz]

TABLES = ("go_fuzz_a", "go_fuzz_b")


def _fuzz_iters() -> int:
    return int(os.environ.get("QDB_E2E_FUZZ_ITERS")
               or os.environ.get("E2E_FUZZ_ITERS")
               or "10")


def _connect_string(http_port: int, sf_dir: Path) -> str:
    return (
        f"ws::addr=127.0.0.1:{http_port}"
        ";username=admin;password=quest"
        f";sf_dir={sf_dir}"
        ";request_durable_ack=on"
        ";reconnect_max_duration_millis=60000"
        ";close_flush_timeout_millis=5000;"
    )


def _seed_for_iteration(base: int, i: int) -> int:
    return (base * 0x9E3779B97F4A7C15 + i) & 0xFFFFFFFFFFFFFFFF


@pytest.mark.parametrize("iteration", range(_fuzz_iters()))
def test_random_failover(iteration: int, server_factory, go_sidecar: GoSidecar,
                         obj_store: ObjStore, scenario_dir: Path) -> None:
    base_seed = int(os.environ.get("QDB_E2E_FUZZ_SEED", str(time.time_ns() & 0xFFFFFFFF)))
    seed = _seed_for_iteration(base_seed, iteration)
    rng = random.Random(seed)
    LOG.info("fuzz iteration=%d seed=%d (base=%d)", iteration, seed, base_seed)

    sf_dir = scenario_dir / "sf"

    size_class = rng.choices(["small", "medium", "large"], weights=[0.70, 0.25, 0.05])[0]
    if size_class == "small":
        batches = rng.randint(1, 5)
        rows_per_batch = rng.randint(5, 40)
    elif size_class == "medium":
        batches = rng.randint(1, 5)
        rows_per_batch = rng.randint(200, 2_000)
    else:  # large
        batches = rng.randint(1, 2)
        rows_per_batch = rng.randint(10_000, 25_000)
    settle_ms = rng.randint(0, 800)
    do_second_failover = rng.choice([True, False])
    LOG.info("fuzz params: size_class=%s batches=%d rows_per_batch=%d settle_ms=%d "
             "do_second_failover=%s", size_class, batches, rows_per_batch, settle_ms,
             do_second_failover)

    sent: dict[str, int] = {t: 0 for t in TABLES}
    flushed: dict[str, int] = {t: 0 for t in TABLES}

    p1 = server_factory("p1")
    p1_ports = p1.start()
    go_sidecar.connect(_connect_string(p1_ports.http, sf_dir))

    for _ in range(batches):
        table = rng.choice(TABLES)
        go_sidecar.send(table, count=rows_per_batch, start_index=sent[table])
        sent[table] += rows_per_batch

    go_sidecar.flush()
    flushed = dict(sent)
    if settle_ms:
        time.sleep(settle_ms / 1000.0)

    _disturbance_kill_restart(p1, p1_ports, obj_store)
    p2 = server_factory("p2", db_root_name="p2-fresh")
    p2.start(http_port=p1_ports.http, pg_port=p1_ports.pg)

    if do_second_failover:
        for _ in range(rng.randint(1, 3)):
            table = rng.choice(TABLES)
            extra = rng.randint(5, 20)
            go_sidecar.send(table, count=extra, start_index=sent[table])
            sent[table] += extra
        go_sidecar.flush()
        flushed = dict(sent)
        time.sleep(rng.randint(0, 600) / 1000.0)
        _disturbance_kill_restart(p2, p1_ports, obj_store)
        p3 = server_factory("p3", db_root_name="p3-fresh")
        p3.start(http_port=p1_ports.http, pg_port=p1_ports.pg)

    timeout_s = 60.0 if size_class == "small" else (120.0 if size_class == "medium" else 240.0)
    for table, count in flushed.items():
        if count == 0:
            continue
        try:
            wait_for_dense_sequence(port=p1_ports.pg, table=table,
                                    expected_count=count, timeout_s=timeout_s)
        except AssertionError as exc:
            repro = (f"QDB_E2E_FUZZ_SEED={base_seed} pytest -m fuzz "
                     f"tests/test_failover_fuzz.py::test_random_failover[{iteration}]")
            raise AssertionError(
                f"fuzz iteration={iteration} base_seed={base_seed} derived_seed={seed} "
                f"size_class={size_class}: {exc}; params batches={batches} "
                f"rows_per_batch={rows_per_batch} settle_ms={settle_ms} "
                f"do_second_failover={do_second_failover}\nTo reproduce: {repro}"
            ) from exc


def _disturbance_kill_restart(server, ports, obj_store: ObjStore) -> None:
    """Kill-9 the server, wipe its DB root and the object store (kill-9
    simulates disk loss, so the client SF is the only surviving copy)."""
    server.kill_9()
    wait_port_free(ports.http)
    wait_port_free(ports.pg)
    if server.db_root.exists():
        shutil.rmtree(server.db_root)
    obj_store.wipe()
