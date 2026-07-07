"""
Graceful role-switch e2e suite for the Go client (ported from
questdb-ent/e2e/tests/test_switch.py).

A single primary with the min-http control plane is driven through a
P->R->P round-trip via ``submit_switch``. Under the Invariant B contract the
Go QWP/SF ingress path does not surface role state to the producer: an
in-place demote closes the ingress connection (no per-write SECURITY_ERROR)
and the store-and-forward sender absorbs the window (rows buffer to on-disk
SF and drain once a primary is reachable again). Write-gate evidence comes
from pg-wire probes; the QWP path is verified by containment (sends keep
succeeding), the frozen commit count on the settled REPLICA, and the
post-switch-back drain.

Read-probe latency bounds are reused verbatim from the Enterprise harness
(lib/probes.py, D-23 characterization, operator-approved 2026-05-27) — they
bound server-side read latency and are client-agnostic. The QWeP probe is
driven through the Go egress sidecar (QwpQueryClient).
"""

from __future__ import annotations

import logging
import statistics
import threading
import time
from pathlib import Path

import psycopg
import pytest

from lib.lifecycle import lifecycle, submit_switch
from lib.pg_query import count_rows, wait_for_dense_sequence
from lib.probes import (
    HTTP_PROBE_BOUND_MS,
    PG_PROBE_BOUND_MS,
    QWEP_PROBE_BOUND_MS,
    _probe_http,
    _probe_http_rows,
    _probe_pg,
    _probe_pg_rows,
    _probe_qwep,
)

from go_egress_sidecar import EgressGoSidecar
from go_sidecar import GoSidecar, GoSidecarError

LOG = logging.getLogger(__name__)

pytestmark = [pytest.mark.go_client]

TABLE = "go_switch_test"
_DURABLE_ACK_AWAIT_TIMEOUT_MS = 60_000
_PG_PROBE_SENTINEL = 999_999


def _connect_string(http_port: int, sf_dir: Path) -> str:
    return (
        f"ws::addr=127.0.0.1:{http_port}"
        ";username=admin;password=quest"
        f";sf_dir={sf_dir}"
        ";request_durable_ack=on"
        ";reconnect_max_duration_millis=60000"
        ";close_flush_timeout_millis=5000;"
    )


def _qwep_connect_string(http_port: int) -> str:
    return (
        f"ws::addr=127.0.0.1:{http_port}"
        ";username=admin;password=quest"
        ";failover_max_duration_ms=10000"
        ";auth_timeout_ms=5000;"
    )


def _pg_write_probe(pg_port: int) -> tuple[str, str]:
    """One pg-wire INSERT write probe. Returns (status, detail): "rejected"
    (read-only gate), "accepted" (committed), or "error" (transient)."""
    try:
        with psycopg.connect(
            host="127.0.0.1", port=pg_port, user="admin", password="quest",
            dbname="qdb", connect_timeout=5, autocommit=True,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'INSERT INTO "{TABLE}" ("timestamp", v) VALUES (now(), {_PG_PROBE_SENTINEL});'
                )
        return "accepted", ""
    except psycopg.DatabaseError as exc:
        msg = str(exc)
        return ("rejected", msg) if "read-only" in msg.lower() else ("error", msg)
    except Exception as exc:
        return "error", str(exc)


def _assert_write_rejected_pg(pg_port: int, *, label: str, attempts: int = 5) -> str:
    last_error = ""
    for _ in range(attempts):
        status, detail = _pg_write_probe(pg_port)
        if status == "rejected":
            LOG.info("%s: pg-wire write rejection confirmed: %s", label, detail)
            return detail
        assert status != "accepted", (
            f"{label}: pg-wire INSERT was ACCEPTED on the settled REPLICA — "
            f"expected a clean 'replica access is read-only' rejection"
        )
        last_error = detail
        time.sleep(0.5)
    raise AssertionError(
        f"{label}: pg-wire write probe kept failing transiently on the settled "
        f"REPLICA; could not confirm the write gate (last: {last_error!r})"
    )


def _stable_row_count(pg_port: int, table: str, *, stable_reads: int = 4,
                      interval_s: float = 0.4, timeout_s: float = 30.0, label: str = "") -> int:
    """Anchor a row count only after WAL apply has demonstrably settled
    (``stable_reads`` consecutive identical counts)."""
    deadline = time.monotonic() + timeout_s
    last = count_rows(port=pg_port, table=table)
    streak = 1
    while streak < stable_reads:
        if time.monotonic() > deadline:
            raise AssertionError(
                f"{label}: row count never converged within {timeout_s}s "
                f"(last={last}, streak={streak}/{stable_reads})"
            )
        time.sleep(interval_s)
        current = count_rows(port=pg_port, table=table)
        if current == last:
            streak += 1
        else:
            last, streak = current, 1
    return last


def _await_role_tolerant(min_http_port: int, role: str, *, timeout_s: float = 60.0,
                         poll_interval_s: float = 0.5, label: str = "") -> bool:
    """Poll lifecycle until ``role`` is settled, tolerating transient HTTP
    errors. Returns True on confirmation, False on timeout (no raise)."""
    target = role.upper()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            snap = lifecycle(min_http_port)
            if not snap.get("switchInFlight") and snap.get("currentRole") == target:
                return True
        except Exception as exc:
            LOG.debug("%s: await_role_tolerant transient error: %s", label, exc)
        time.sleep(min(poll_interval_s, max(0.0, deadline - time.monotonic())))
    LOG.warning("%s: await_role_tolerant(%s) did not settle within %.0fs", label, role, timeout_s)
    return False


def _submit_switch_tolerant(min_http_port: int, role: str, *, max_attempts: int = 5,
                            retry_sleep_s: float = 1.0, label: str = "") -> bool:
    for attempt in range(1, max_attempts + 1):
        try:
            submit_switch(min_http_port, role, wait=False)
            return True
        except Exception as exc:
            LOG.debug("%s: submit_switch(%s) attempt %d failed: %s", label, role, attempt, exc)
            if attempt < max_attempts:
                time.sleep(retry_sleep_s)
    return False


def test_write_path_across_switch(
    server_factory, go_sidecar: GoSidecar, scenario_dir: Path
) -> None:
    """During ingest, drive a P->R->P round-trip. QWP writes in the settled
    REPLICA window are ACCEPTED locally (SF absorbs them; a producer-visible
    role error is a containment regression) and do NOT commit on the replica;
    pg-wire rejects cleanly; after switch-back the SF drain durably-acks the
    window rows and the dense oracle holds [0..PRE+WINDOW) exactly once."""
    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    p1 = server_factory("p1")
    p1_ports = p1.start(min_http=True)
    assert p1_ports.min_http is not None, "min_http port not reported"
    go_sidecar.connect(_connect_string(p1_ports.http, sf_dir))

    PRE_SWITCH_ROWS = 100
    go_sidecar.send(TABLE, count=PRE_SWITCH_ROWS, start_index=0)
    seed_fsn = go_sidecar.flush()
    assert go_sidecar.await_acked(seed_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"pre-switch seed was not durably acked [fsn={seed_fsn}]"
    )

    ok = _submit_switch_tolerant(p1_ports.min_http, "replica", label="write_path")
    assert ok, "submit_switch(replica) failed after retries"
    assert _await_role_tolerant(p1_ports.min_http, "replica", timeout_s=60.0, label="write_path"), (
        "write_path: REPLICA role did not settle within 60s"
    )

    WINDOW_ROWS = 5
    write_index = PRE_SWITCH_ROWS
    window_fsn = -1
    for _ in range(WINDOW_ROWS):
        try:
            go_sidecar.send(TABLE, count=1, start_index=write_index)
            window_fsn = go_sidecar.flush()
            write_index += 1
        except GoSidecarError as exc:
            raise AssertionError(
                "INVARIANT B CONTAINMENT: the QWP producer saw a role error in the "
                f"settled REPLICA window: {exc}. The SF sender must absorb the window."
            ) from exc
        time.sleep(0.1)

    _assert_write_rejected_pg(p1_ports.pg, label="write_path")

    # Exact-count invariant: ZERO boundary rows may commit on the settled REPLICA.
    replica_count = count_rows(port=p1_ports.pg, table=TABLE)
    assert replica_count == PRE_SWITCH_ROWS, (
        f"BOUNDARY-RACE: {replica_count - PRE_SWITCH_ROWS} boundary row(s) committed on the "
        f"settled REPLICA (expected exactly {PRE_SWITCH_ROWS} pre-switch rows)."
    )

    ok = _submit_switch_tolerant(p1_ports.min_http, "primary", label="write_path")
    assert ok, "submit_switch(primary) failed after retries"
    _await_role_tolerant(p1_ports.min_http, "primary", timeout_s=60.0, label="write_path")

    total_rows = write_index
    assert go_sidecar.await_acked(window_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"window rows buffered in SF during the REPLICA window were not drained to "
        f"the re-promoted primary [publishedFsn={window_fsn}]"
    )
    wait_for_dense_sequence(port=p1_ports.pg, table=TABLE,
                            expected_count=total_rows, timeout_s=90.0)
    LOG.info("write_path: dense [0..%d) verified (pre-switch %d + window %d drained)",
             total_rows, PRE_SWITCH_ROWS, total_rows - PRE_SWITCH_ROWS)


def test_disturbance_honesty_guard(
    server_factory, go_sidecar: GoSidecar, scenario_dir: Path
) -> None:
    """Honesty guard: nothing passes on a no-op switch. Asserts lifecycle()
    actually flipped to REPLICA, the pg-wire write gate rejected, QWP rows
    sent on the settled REPLICA buffered (frozen commit count), and the
    buffered rows drained after switch-back."""
    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    p1 = server_factory("p1")
    p1_ports = p1.start(min_http=True)
    assert p1_ports.min_http is not None, "min_http port not reported"
    go_sidecar.connect(_connect_string(p1_ports.http, sf_dir))

    SEED_ROWS = 30
    go_sidecar.send(TABLE, count=SEED_ROWS, start_index=0)
    seed_fsn = go_sidecar.flush()
    assert go_sidecar.await_acked(seed_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"seed rows were not durably acked [fsn={seed_fsn}]"
    )

    initial_snap = lifecycle(p1_ports.min_http)
    assert initial_snap.get("currentRole") == "PRIMARY", (
        f"HONESTY GUARD: server did not start as PRIMARY (got {initial_snap.get('currentRole')!r})"
    )

    ok = _submit_switch_tolerant(p1_ports.min_http, "replica", label="honesty_guard")
    assert ok, "submit_switch(replica) failed after retries"

    replica_snap: dict | None = None
    qwp_containment_violations: list[str] = []
    window_fsn = -1
    write_idx = SEED_ROWS
    poll_deadline = time.monotonic() + 30.0
    while time.monotonic() < poll_deadline:
        try:
            go_sidecar.send(TABLE, count=1, start_index=write_idx)
            window_fsn = go_sidecar.flush()
            write_idx += 1
        except GoSidecarError as exc:
            qwp_containment_violations.append(str(exc))
        try:
            snap = lifecycle(p1_ports.min_http)
            if not snap.get("switchInFlight") and snap.get("currentRole") == "REPLICA":
                replica_snap = snap
                break
        except Exception:
            pass
        time.sleep(0.2)

    _await_role_tolerant(p1_ports.min_http, "replica", timeout_s=60.0, label="honesty_guard")
    if replica_snap is None:
        try:
            replica_snap = lifecycle(p1_ports.min_http)
        except Exception as exc:
            LOG.warning("honesty_guard: could not fetch lifecycle snapshot: %s", exc)

    pg_rejection_msg = _assert_write_rejected_pg(p1_ports.pg, label="honesty_guard")

    count_at_apex = _stable_row_count(p1_ports.pg, TABLE, label="honesty_guard")
    for _ in range(3):
        try:
            go_sidecar.send(TABLE, count=1, start_index=write_idx)
            window_fsn = go_sidecar.flush()
            write_idx += 1
        except GoSidecarError as exc:
            qwp_containment_violations.append(str(exc))
        time.sleep(0.3)
    count_after_writes = count_rows(port=p1_ports.pg, table=TABLE)
    qwp_window_writes = write_idx - SEED_ROWS

    ok = _submit_switch_tolerant(p1_ports.min_http, "primary", label="honesty_guard")
    assert ok, "submit_switch(primary) failed after retries"
    _await_role_tolerant(p1_ports.min_http, "primary", timeout_s=60.0, label="honesty_guard")

    assert replica_snap is not None, (
        "HONESTY GUARD: lifecycle() never confirmed REPLICA — the switch may not have happened"
    )
    assert replica_snap.get("currentRole") == "REPLICA", (
        f"HONESTY GUARD: apex snapshot currentRole={replica_snap.get('currentRole')!r}, want REPLICA"
    )
    assert not replica_snap.get("switchInFlight"), (
        f"HONESTY GUARD: apex snapshot switchInFlight=True: {replica_snap!r}"
    )
    assert not qwp_containment_violations, (
        "HONESTY GUARD / INVARIANT B: the QWP producer saw role error(s) during the "
        f"switch window: {qwp_containment_violations[:3]!r}"
    )
    assert qwp_window_writes > 0, "HONESTY GUARD: no QWP writes issued during the switch window"
    assert count_after_writes == count_at_apex, (
        f"HONESTY GUARD: commit count grew on the settled REPLICA "
        f"({count_at_apex} -> {count_after_writes}) — the REPLICA window is not gating QWP writes"
    )
    assert go_sidecar.await_acked(window_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"window rows buffered in SF were not drained after switch-back [publishedFsn={window_fsn}]"
    )
    LOG.info("honesty_guard: PASSED — REPLICA confirmed; pg gate rejected (%.60s); commit "
             "count frozen at %d across %d window writes; SF drained",
             pg_rejection_msg, count_at_apex, qwp_window_writes)


def test_reads_not_frozen(
    server_factory, go_sidecar: GoSidecar, go_egress_sidecar: EgressGoSidecar, scenario_dir: Path
) -> None:
    """Reads-not-frozen guard: 100% oblivious free-running probe threads fire
    fresh-connection batch reads over web-http and pg-wire on a fixed cadence
    across the P->R->P window. A frozen accept-loop tears a hole in the
    success timeline (MAX_SUCCESS_GAP_S) or starves the served floor
    (MIN_SERVED_PROBES); every served probe must return the full seeded batch
    within the D-23 latency bounds. The idle durably-acked QWP ingress
    connection must survive the round-trip (flat reconn_succ)."""
    SEED_ROWS = 200
    PROBE_PERIOD_S = 0.1
    BASELINE_S = 1.0
    TAIL_S = 2.0
    MAX_SUCCESS_GAP_S = 5.0
    MIN_SERVED_PROBES = 10
    MIN_ATTEMPTED_PROBES = 15

    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    p1 = server_factory("p1")
    p1_ports = p1.start(min_http=True)
    assert p1_ports.min_http is not None, "min_http port not reported"
    go_sidecar.connect(_connect_string(p1_ports.http, sf_dir))

    try:
        go_egress_sidecar.connect(_qwep_connect_string(p1_ports.http))
        qwep_available = True
    except Exception as exc:
        qwep_available = False
        LOG.warning("reads_not_frozen: QWeP egress sidecar failed to connect: %s", exc)

    go_sidecar.send(TABLE, count=SEED_ROWS, start_index=0)
    seed_fsn = go_sidecar.flush()
    assert go_sidecar.await_acked(seed_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"seed rows were not durably acked [fsn={seed_fsn}]"
    )

    visible_deadline = time.monotonic() + 30.0
    while time.monotonic() < visible_deadline:
        if (_probe_http_rows(p1_ports.http, TABLE, SEED_ROWS) is not None
                and _probe_pg_rows(p1_ports.pg, TABLE, SEED_ROWS) is not None):
            break
        time.sleep(0.2)
    else:
        pytest.fail(f"seeded batch of {SEED_ROWS} rows not readable on http+pg within 30s")

    reconn_succ_before = go_sidecar.stats().reconn_succ

    stop = threading.Event()
    http_samples: list[tuple[float, float | None]] = []
    pg_samples: list[tuple[float, float | None]] = []
    qwep_samples: list[tuple[float, float | None]] = []

    def _prober(probe_fn, samples):
        while not stop.is_set():
            t0 = time.monotonic()
            samples.append((t0, probe_fn()))
            stop.wait(PROBE_PERIOD_S)

    threads = [
        threading.Thread(target=_prober,
                         args=(lambda: _probe_http_rows(p1_ports.http, TABLE, SEED_ROWS), http_samples),
                         name="http-prober", daemon=True),
        threading.Thread(target=_prober,
                         args=(lambda: _probe_pg_rows(p1_ports.pg, TABLE, SEED_ROWS), pg_samples),
                         name="pg-prober", daemon=True),
    ]
    if qwep_available:
        threads.append(threading.Thread(
            target=_prober,
            args=(lambda: _probe_qwep(go_egress_sidecar,
                                      query=f"select * from {TABLE} limit {SEED_ROWS}",
                                      expect_rows=SEED_ROWS), qwep_samples),
            name="qwep-prober", daemon=True))

    window_start = time.monotonic()
    for th in threads:
        th.start()
    try:
        time.sleep(BASELINE_S)
        assert _submit_switch_tolerant(p1_ports.min_http, "replica", label="reads_not_frozen"), \
            "submit_switch(replica) failed after retries"
        assert _await_role_tolerant(p1_ports.min_http, "replica", timeout_s=60.0,
                                    label="reads_not_frozen"), "REPLICA did not settle within 60s"
        assert _submit_switch_tolerant(p1_ports.min_http, "primary", label="reads_not_frozen"), \
            "submit_switch(primary) failed after retries"
        assert _await_role_tolerant(p1_ports.min_http, "primary", timeout_s=60.0,
                                    label="reads_not_frozen"), "PRIMARY did not settle within 60s"
        time.sleep(TAIL_S)
    finally:
        window_end = time.monotonic()
        stop.set()
        for th in threads:
            th.join(timeout=30.0)

    reconn_succ_after = go_sidecar.stats().reconn_succ

    def _max_success_gap(samples):
        edges = [window_start] + [t for t, ms in samples if ms is not None] + [window_end]
        return max(b - a for a, b in zip(edges, edges[1:]))

    freeze_evidence: list[str] = []
    bound_violations: list[str] = []
    harness_evidence: list[str] = []
    for name, samples, bound_ms in (("HTTP", http_samples, HTTP_PROBE_BOUND_MS),
                                    ("PG", pg_samples, PG_PROBE_BOUND_MS)):
        served = [(t, ms) for t, ms in samples if ms is not None]
        gap = _max_success_gap(samples)
        if gap > MAX_SUCCESS_GAP_S:
            freeze_evidence.append(f"{name}: {gap:.1f}s hole in the success timeline "
                                   f"(bound {MAX_SUCCESS_GAP_S}s; served={len(served)}/{len(samples)})")
        if len(served) < MIN_SERVED_PROBES:
            freeze_evidence.append(f"{name}: only {len(served)}/{len(samples)} probe(s) served "
                                   f"(floor {MIN_SERVED_PROBES})")
        if len(samples) < MIN_ATTEMPTED_PROBES:
            harness_evidence.append(f"{name}: only {len(samples)} attempts (floor {MIN_ATTEMPTED_PROBES})")
        for t, ms in served:
            if ms > bound_ms:
                bound_violations.append(f"{name} probe at +{t - window_start:.1f}s took {ms:.1f}ms "
                                        f"> {bound_ms}ms (D-23 bound)")
    for t, ms in qwep_samples:
        if ms is not None and ms > QWEP_PROBE_BOUND_MS:
            bound_violations.append(f"QWeP probe at +{t - window_start:.1f}s took {ms:.1f}ms "
                                    f"> {QWEP_PROBE_BOUND_MS}ms (D-23 bound)")

    assert not freeze_evidence, "READS-NOT-FROZEN: " + "; ".join(freeze_evidence)
    assert not bound_violations, (
        f"READS-NOT-FROZEN: {len(bound_violations)} probe(s) exceeded latency bounds:\n  "
        + "\n  ".join(bound_violations))
    assert not harness_evidence, "probe-harness sanity: " + "; ".join(harness_evidence)
    assert reconn_succ_after == reconn_succ_before, (
        f"CONNECTION SURVIVAL: reconn_succ {reconn_succ_before} -> {reconn_succ_after}; the idle "
        f"durably-acked QWP ingress connection must survive the P->R->P round-trip"
    )


def test_characterize(
    server_factory, go_sidecar: GoSidecar, go_egress_sidecar: EgressGoSidecar, scenario_dir: Path
) -> None:
    """Observation pass: drive P->R->P non-blocking and log read-probe
    latencies (HTTP/QWeP/pg-wire) + write rejections + reconn_succ through the
    window. Minimal structural assertions (the switch happened; pg-wire gated;
    the QWP producer stayed error-free — Invariant B containment)."""
    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    p1 = server_factory("p1")
    p1_ports = p1.start(min_http=True)
    assert p1_ports.min_http is not None, "min_http port not reported"
    go_sidecar.connect(_connect_string(p1_ports.http, sf_dir))

    try:
        go_egress_sidecar.connect(_qwep_connect_string(p1_ports.http))
        qwep_available = True
    except Exception as exc:
        qwep_available = False
        LOG.warning("characterize: QWeP egress sidecar failed to connect: %s", exc)

    SEED_ROWS = 50
    go_sidecar.send(TABLE, count=SEED_ROWS, start_index=0)
    go_sidecar.flush()
    time.sleep(1.0)

    all_write_rej: list[str] = []
    qwp_containment_violations: list[str] = []
    write_idx = SEED_ROWS
    reconn_before = go_sidecar.stats().reconn_succ

    _submit_switch_tolerant(p1_ports.min_http, "replica", label="characterize")
    probe_deadline = time.monotonic() + 30.0
    replica_confirmed = False
    while time.monotonic() < probe_deadline:
        _probe_http(p1_ports.http)
        _probe_pg(p1_ports.pg)
        if qwep_available:
            _probe_qwep(go_egress_sidecar)
        try:
            go_sidecar.send(TABLE, count=1, start_index=write_idx)
            go_sidecar.flush()
            write_idx += 1
        except GoSidecarError as exc:
            qwp_containment_violations.append(str(exc))
        status, detail = _pg_write_probe(p1_ports.pg)
        if status == "rejected":
            all_write_rej.append(detail)
        try:
            snap = lifecycle(p1_ports.min_http)
            if not snap.get("switchInFlight") and snap.get("currentRole") == "REPLICA":
                replica_confirmed = True
                break
        except Exception:
            pass
        time.sleep(0.2)

    _await_role_tolerant(p1_ports.min_http, "replica", timeout_s=60.0, label="characterize")
    for _ in range(5):
        status, detail = _pg_write_probe(p1_ports.pg)
        if status == "rejected":
            all_write_rej.append(detail)
        time.sleep(0.2)

    _submit_switch_tolerant(p1_ports.min_http, "primary", label="characterize")
    primary_confirmed = _await_role_tolerant(p1_ports.min_http, "primary", timeout_s=60.0,
                                             label="characterize")
    reconn_after = go_sidecar.stats().reconn_succ
    LOG.info("characterize: reconn_succ %d -> %d (delta expected > 0 after a demote-close if "
             "writes were in flight)", reconn_before, reconn_after)

    switch_happened = replica_confirmed or primary_confirmed or len(all_write_rej) > 0
    assert switch_happened, (
        "Neither lifecycle confirmation NOR pg-wire write rejections observed — the switch may "
        f"not have happened (replica_confirmed={replica_confirmed}, primary_confirmed={primary_confirmed}, "
        f"pg_rejections={len(all_write_rej)})"
    )
    assert len(all_write_rej) > 0, (
        "No pg-wire write rejections observed through the switch window; expected 'replica access "
        "is read-only' on the settled REPLICA (QWP probes are contractually silent under Invariant B)"
    )
    assert not qwp_containment_violations, (
        f"INVARIANT B CONTAINMENT: the QWP producer saw role error(s): {qwp_containment_violations[:3]!r}"
    )
