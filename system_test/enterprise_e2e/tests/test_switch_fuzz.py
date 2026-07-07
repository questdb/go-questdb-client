"""
Switch-roundtrip fuzz for the Go client (ported from
questdb-ent/e2e/tests/test_switch_fuzz.py).

Owns the graceful switch-roundtrip fuzz; test_failover_fuzz.py owns kill-9 fuzz
(the one-disturbance-per-file split). Shape mirrors test_failover_fuzz.py for
seed isolation and repro strings:
  - Dedicated random.Random(seed) derived from QDB_E2E_FUZZ_SEED | time_ns;
    never module-level random.* (pytest-randomly reseeds stdlib random).
  - Per-iteration repro string on AssertionError.
  - @pytest.mark.fuzz (registered in pyproject.toml; --strict-markers enforced).

Chain: 1..6 P->R->P round-trips via lib.lifecycle.submit_switch/await_role.
Because this is the forked-JVM e2e tier, the Go QWP sidecar IS the primary
ingest vehicle (ilp-over-ws). The pg-wire + HTTP keep-alive connections are the
"client survival" witnesses.

TWO PLANES — the key reconciliation of the exact-count oracle with live ingest:

  LIVE PLANE (runs DURING each switch window):
    - client-survival fingerprint: psycopg conn survives SELECT 1 + HTTP
      keep-alive 200. The QWP ingress connection is NOT expected to survive: an
      in-place demote CLOSES it with a reconnect-eligible NORMAL_CLOSURE (not a
      terminal SECURITY_ERROR -- Invariant B); the SF sender buffers the window
      on disk and reconnects after promotion. A reconn_succ delta is therefore
      EXPECTED -- not asserted flat, but BOUNDED to chain_len + slack so a
      reconnect storm still fails; the post-chain ack barrier proves the
      buffered rows drained.
    - in-switch read prober: HTTP / pg-wire / QWeP each polled during the switch
      window; latency asserted < HTTP/PG/QWEP_PROBE_BOUND_MS (from lib.probes). A
      None return (timeout/error) counts as a freeze and fails the assertion.

  QUIESCED COUNT PLANE (runs at each REPLICA apex, NOT during the switch):
    - exact-count REPLICA oracle: at every REPLICA apex _quiesce_and_watermark()
      pauses ingest, flushes, awaits REPLICA settle, and polls count_rows until
      stable, capturing that as watermark_expected. Then asserts
      count_rows == watermark_expected (strict ==).
    - write-rejection oracle: a pg-wire INSERT and a web-http /exec INSERT on the
      settled REPLICA must be cleanly rejected ("replica access is read-only");
      the CSV import and line-UDP paths are covered by the engine-level getWriter
      gate.
    - NOTE: do NOT compare to a PRE captured before the chain under live ingest
      -- that value is stale by the first apex. The watermark is ALWAYS captured
      fresh at the quiescence point. Using >= is also wrong on the graceful path
      (it blinds to the boundary-race regression class).

CRITICAL -- do NOT wipe the object store on the graceful-switch path: a graceful
switch destroys nothing; wiping would mask real data loss. Only kill-9
(test_failover_fuzz.py) legitimately wipes.

PITR variant (test_switch_fuzz_pitr): boots a primary, ingests rows, then boots
a fresh PITR-recovered primary from the same object store, keeps a live QWP
client ingesting across the recover-boot + first switch, and asserts
count_rows == watermark_expected (no silent UDP drop, #092). ONE high-value
deterministic PITR e2e test, NOT a fuzz sub-dimension (the two-phase fixture
cost -- boot primary, upload, recover-boot -- exceeds the per-iteration budget).

REPRO: re-run with the same QDB_E2E_FUZZ_SEED env var to reproduce a failing
iteration; the AssertionError carries the exact command. The QWP sidecar and JVM
apply threads introduce interleaving not controlled by the seed, so a seed is a
probabilistic reproducer, not fully deterministic across runs.
"""

from __future__ import annotations

import http.client
import logging
import os
import random
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import psycopg
import pytest

from lib import lifecycle as lc
from lib.pg_query import count_rows
from lib.probes import (
    HTTP_PROBE_BOUND_MS,
    PG_PROBE_BOUND_MS,
    QWEP_PROBE_BOUND_MS,
    _probe_http,
    _probe_pg,
    _probe_qwep,
)

from go_egress_sidecar import EgressGoSidecar
from go_sidecar import GoSidecar

LOG = logging.getLogger(__name__)

pytestmark = [pytest.mark.fuzz]

# Timeout for each submit_switch call (ms). Matches test_switch.py convention.
_SWITCH_TIMEOUT_MS = 10_000

# Timeout for await_role (seconds).
_AWAIT_ROLE_TIMEOUT_S = 60.0

# Slack added to chain_len when bounding the SF-sender reconnect count across a
# chain. Under Invariant B each P->R leg demote-closes the QWP ingress connection
# and the sender reconnects ~once per leg, so the steady-state delta is chain_len.
# The slack absorbs benign timing jitter while still failing a reconnect storm.
_RECONN_SLACK = 4

# Timeout (ms) for the durable-ack await issued on the PRIMARY side of a switch
# boundary. The sidecar's durable watermark advances only when the client I/O
# loop reads an inbound STATUS_DURABLE_ACK frame; once the connection is
# demote-rejected that read can no longer happen, so the ack must be awaited
# WHILE the connection is still PRIMARY. Bounded so a dead ingest path cannot
# hang the iteration (the growth floor still fails it afterwards).
_DURABLE_ACK_AWAIT_TIMEOUT_MS = 10_000

# Table for baseline ingestion before the chain.
_TABLE = "switch_fuzz"

# How long (seconds) the count must stay unchanged before we accept it as the
# watermark in _quiesce_and_watermark.
_QUIESCE_STABLE_WINDOW_S = 1.0

# Poll interval (seconds) inside the quiesce stability loop.
_QUIESCE_POLL_INTERVAL_S = 0.2

# PITR recovery sentinel file name (Properties format with two required keys).
_RECOVER_SENTINEL = "_recover_point_in_time"

# ISO-8601 timestamp meaning "recover up to the latest available index entry".
# Raw numeric Long.MAX_VALUE is NOT accepted by MicrosTimestampDriver.parseAnyFormat;
# this far-future string is required (see smoke/fixtures/row_pitr_restore.py).
_RECOVERY_LATEST_TIMESTAMP = "9999-12-31T23:59:59.999999Z"


def _fuzz_iters() -> int:
    return int(os.environ.get("QDB_E2E_FUZZ_ITERS")
               or os.environ.get("E2E_FUZZ_ITERS")
               or "10")


def _seed_for_iteration(base: int, i: int) -> int:
    """Mix the global seed with the iteration index. Rerunning with the same
    base seed reproduces the entire sequence. Same multiplicative hash as
    test_failover_fuzz.py for consistency."""
    return (base * 0x9E3779B97F4A7C15 + i) & 0xFFFFFFFFFFFFFFFF


def _durable_acked_rows(stats) -> int:
    """Number of rows the sidecar has DURABLY acked, derived from the durable
    watermark the sidecar surfaces as ``stats().acked`` (the client's
    ``getAckedFsn()``).

    With ``request_durable_ack=on`` the server advances this watermark solely on
    ``STATUS_DURABLE_ACK`` frames -- after the row is persisted -- so every row
    counted here MUST be present in the table. It is NOT ``stats().acks``
    (getTotalAcks(), a transport-frame counter that also ticks on
    DROP_AND_CONTINUE rejections and is no persistence promise).

    The watermark is a 0-based frame sequence number (FSN). Every frame in these
    runs carries exactly one row (baseline + ingest loop both send single-row
    frames), so the number of durably acked rows is ``acked + 1`` once a frame
    has been acked, and ``0`` when the watermark is still ``-1``.
    """
    acked_fsn = stats.acked
    return acked_fsn + 1 if acked_fsn >= 0 else 0


# ---------------------------------------------------------------------------
# Quiescence protocol (the exact-count oracle reconciled with live ingest)
# ---------------------------------------------------------------------------


def _quiesce_and_watermark(
    p1_ports,
    go_sidecar: GoSidecar,
    pause_flag: threading.Event,
    ingest_index_ref: list,
    table: str,
    step: int,
) -> int:
    """Pause ingest, drain the sidecar, and capture a stable row count.

    Implements the QUIESCED COUNT PLANE. The returned value is
    watermark_expected: the exact row count at the quiescence point the
    exact-count oracle must equal.

    Protocol:
      1. Set pause_flag so the background ingest thread stops sending (but the
         thread stays alive so the caller can resume it).
      2. Flush the sidecar (drains any in-flight frames to the server).
      3. Confirm await_role(REPLICA) -- role must be settled before we count.
      4. Poll count_rows until unchanged for _QUIESCE_STABLE_WINDOW_S.
      5. Return that stable count as watermark_expected.

    After the apex assertion (caller's responsibility) the caller should clear
    pause_flag to resume ingest for the next chain leg.
    """
    LOG.info("quiesce step=%d: setting pause flag, pausing ingest", step)
    pause_flag.set()

    try:
        go_sidecar.flush()
        LOG.info("quiesce step=%d: sidecar flushed", step)
    except Exception as exc:
        LOG.warning("quiesce step=%d: sidecar flush raised %s (may be tolerable in REPLICA window)", step, exc)

    lc.await_role(p1_ports.min_http, "replica", timeout_s=_AWAIT_ROLE_TIMEOUT_S)
    LOG.info("quiesce step=%d: REPLICA settled, polling for stable row count", step)

    deadline = time.monotonic() + 30.0
    stable_since: Optional[float] = None
    last_count = -1
    watermark: int = -1
    # A row acked before the pause but not yet WAL-applied can land AFTER we
    # capture the watermark. Before accepting it, flush once more and require the
    # count to stay stable across a second window, so it reflects a fully-drained
    # table.
    reflushed = False
    while time.monotonic() < deadline:
        try:
            current = count_rows(port=p1_ports.pg, table=table, timeout_s=5.0)
        except (TimeoutError, psycopg.DatabaseError) as exc:
            LOG.debug("quiesce step=%d: count_rows raised %s; retrying", step, exc)
            time.sleep(_QUIESCE_POLL_INTERVAL_S)
            continue

        if current != last_count:
            last_count = current
            stable_since = time.monotonic()
            reflushed = False
            LOG.debug("quiesce step=%d: count changed to %d, resetting stable window", step, current)
        elif stable_since is not None and (time.monotonic() - stable_since) >= _QUIESCE_STABLE_WINDOW_S:
            if not reflushed:
                try:
                    go_sidecar.flush()
                except Exception as exc:
                    LOG.warning("quiesce step=%d: re-flush raised %s (tolerable in REPLICA window)", step, exc)
                reflushed = True
                stable_since = time.monotonic()
                time.sleep(_QUIESCE_POLL_INTERVAL_S)
                continue
            watermark = current
            LOG.info("quiesce step=%d: watermark_expected=%d (stable across re-flush)", step, watermark)
            break

        time.sleep(_QUIESCE_POLL_INTERVAL_S)

    if watermark == -1:
        # Never reached a stable count within the deadline. The exact-count
        # oracle downstream is a strict `==`, so proceeding with an unstable
        # last_count would either false-fail or mask a boundary-race excess.
        # Fail fast instead: an unstable watermark is a broken measurement, not
        # a conservative one.
        raise AssertionError(
            f"quiesce step={step}: table never quiesced to a stable row count "
            f"within the deadline (last_count={last_count}). Cannot capture a "
            f"trustworthy watermark for the exact-count oracle."
        )

    return watermark


# ---------------------------------------------------------------------------
# In-switch read prober (live availability plane)
# ---------------------------------------------------------------------------


def _run_in_switch_probes(
    p1_ports,
    egress,
    switch_deadline: float,
    target_role: str = "replica",
) -> list[str]:
    """Poll the three read probes while a switch is in progress.

    Called immediately after submit_switch (non-blocking) while the switch is
    still in flight. Returns a list of bound-violation strings; empty means no
    freeze detected. A None return from any probe counts as a freeze (timeout or
    connection refused during a gate-closed stall).

    Runs until switch_deadline (wall-clock) or the role settles to target_role,
    whichever comes first.
    """
    target_role_upper = target_role.upper()
    violations: list[str] = []
    while time.monotonic() < switch_deadline:
        ms = _probe_http(p1_ports.http)
        if ms is None:
            violations.append(
                f"HTTP probe returned None (freeze / connection error) during switch window "
                f"(bound={HTTP_PROBE_BOUND_MS}ms; a None means the server did not respond "
                f"within the probe timeout -- possible gate-closed freeze)"
            )
        elif ms > HTTP_PROBE_BOUND_MS:
            violations.append(
                f"HTTP probe {ms:.1f}ms > HTTP_PROBE_BOUND_MS={HTTP_PROBE_BOUND_MS}ms "
                f"(in-switch availability bound)"
            )

        ms = _probe_pg(p1_ports.pg)
        if ms is None:
            violations.append(
                f"PG probe returned None (freeze / connection error) during switch window "
                f"(bound={PG_PROBE_BOUND_MS}ms)"
            )
        elif ms > PG_PROBE_BOUND_MS:
            violations.append(
                f"PG probe {ms:.1f}ms > PG_PROBE_BOUND_MS={PG_PROBE_BOUND_MS}ms "
                f"(in-switch availability bound)"
            )

        if egress is not None:
            ms = _probe_qwep(egress)
            if ms is None:
                violations.append(
                    f"QWeP probe returned None (freeze / connection error) during switch window "
                    f"(bound={QWEP_PROBE_BOUND_MS}ms)"
                )
            elif ms > QWEP_PROBE_BOUND_MS:
                violations.append(
                    f"QWeP probe {ms:.1f}ms > QWEP_PROBE_BOUND_MS={QWEP_PROBE_BOUND_MS}ms "
                    f"(in-switch availability bound)"
                )

        try:
            snap = lc.lifecycle(p1_ports.min_http)
            if not snap.get("switchInFlight") and snap.get("currentRole") == target_role_upper:
                LOG.info("in-switch prober: %s confirmed, exiting probe loop", target_role_upper)
                break
        except Exception:
            pass

        time.sleep(0.1)

    return violations


# ---------------------------------------------------------------------------
# Write-rejection oracle helpers (quiesced plane)
# ---------------------------------------------------------------------------


def _assert_write_rejection(pg_port: int, table: str, step: int) -> None:
    """Assert a pg-wire INSERT on the settled REPLICA is cleanly rejected with
    'replica access is read-only'. A freeze (timeout/hang) or broken-pipe here
    is a bug -- the rejection must be clean and immediate."""
    LOG.info("write_rejection step=%d: attempting INSERT on settled REPLICA", step)
    try:
        with psycopg.connect(
            host="127.0.0.1",
            port=pg_port,
            user="admin",
            password="quest",
            dbname="qdb",
            connect_timeout=5,
            autocommit=True,
        ) as conn:
            with conn.cursor() as cur:
                # ("timestamp", v): the QWP-auto-created table is (v, timestamp),
                # so a (ts, v) list names a non-existent column and would fail with
                # "Invalid column" on a broken (writable) gate instead of committing
                # -- masking the real failure. ("timestamp", v) commits the sentinel
                # into v, so a broken gate is detected directly as accepted.
                cur.execute(
                    f"INSERT INTO \"{table}\" (\"timestamp\", v) VALUES (now(), 99_999);"
                )
        raise AssertionError(
            f"WRITE-REJECTION: INSERT was accepted on settled REPLICA at step={step}. "
            f"Expected a clean 'replica access is read-only' error. "
            f"A write committed on a REPLICA violates the boundary-race regression guard."
        )
    except (psycopg.OperationalError, psycopg.errors.ConnectionTimeout) as exc:
        # Must precede the broader DatabaseError clause: OperationalError /
        # ConnectionTimeout are subclasses of DatabaseError, so a DatabaseError-first
        # ordering would shadow this one and mis-report a freeze as a read-only refusal.
        raise AssertionError(
            f"WRITE-REJECTION: pg-wire INSERT raised OperationalError on settled REPLICA "
            f"at step={step}: {exc!r}. Expected a clean 'replica access is read-only' "
            f"DatabaseError -- a connection error here may indicate a freeze on the write path."
        ) from exc
    except psycopg.DatabaseError as exc:
        msg = str(exc).lower()
        assert "replica access is read-only" in msg or "read-only" in msg, (
            f"WRITE-REJECTION: INSERT raised DatabaseError but message did not contain "
            f"'replica access is read-only' at step={step}. Actual message: {exc!r}. "
            f"The rejection must be clean and role-specific (not a generic error)."
        )
        LOG.info("write_rejection step=%d: clean rejection confirmed: %s", step, exc)


def _assert_http_exec_write_rejection(http_port: int, table: str, step: int) -> None:
    """Assert a web-http /exec INSERT on the settled REPLICA is cleanly rejected
    (HTTP 403 'replica access is read-only'). Sibling to _assert_write_rejection:
    /exec uses a different server dispatch (JsonQueryProcessor) with its own
    read-only gate, so it is asserted independently."""
    sql = f'INSERT INTO "{table}" ("timestamp", v) VALUES (now(), 99_998)'
    # quote_plus matches _probe_http's "SELECT+1" -- the encoding the /exec
    # query-param decoder is proven to accept.
    path = "/exec?query=" + urllib.parse.quote_plus(sql)
    LOG.info("http_exec_write_rejection step=%d: attempting /exec INSERT on settled REPLICA", step)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", http_port, timeout=5)
        try:
            conn.request(
                "GET",
                path,
                headers={"Authorization": "Basic YWRtaW46cXVlc3Q="},
            )
            resp = conn.getresponse()
            status = resp.status
            body = resp.read().decode("utf-8", errors="replace")
        finally:
            conn.close()
    except Exception as exc:
        raise AssertionError(
            f"WRITE-REJECTION (/exec): web-http INSERT raised a connection error on settled "
            f"REPLICA at step={step}: {exc!r}. Expected a clean HTTP 403 'replica access is "
            f"read-only' -- a connection error here may indicate a freeze on the /exec write path."
        ) from exc

    if 200 <= status < 300:
        raise AssertionError(
            f"WRITE-REJECTION (/exec): INSERT was accepted (HTTP {status}) on settled REPLICA "
            f"at step={step}, body={body!r}. Expected HTTP 403 'replica access is read-only'. A "
            f"write committed on a REPLICA violates the boundary-race regression guard."
        )
    assert status == 403, (
        f"WRITE-REJECTION (/exec): expected HTTP 403 on settled REPLICA at step={step}, got "
        f"HTTP {status}, body={body!r}. The rejection must be role-specific (a generic error "
        f"status would mask a missing read-only gate)."
    )
    assert "replica access is read-only" in body.lower() or "read-only" in body.lower(), (
        f"WRITE-REJECTION (/exec): HTTP 403 body did not contain 'replica access is read-only' "
        f"at step={step}. Body: {body!r}. The rejection must be clean and role-specific."
    )
    LOG.info("http_exec_write_rejection step=%d: clean 403 rejection confirmed", step)


def _assert_copy_from_write_rejection(http_port: int, table: str, step: int) -> None:
    """Assert a COPY ... FROM import POST on a settled REPLICA is cleanly refused.

    The CSV import path (ParallelCsvFileImporter) acquires a TableWriter via
    engine.getWriter with a client lock reason, bypassing the per-statement
    read-only gate in JsonQueryProcessor. The engine-level gate in
    EntCairoEngine.getWriter closes that window by refusing client lock reasons.
    Exercises the /upload multipart endpoint and verifies a non-2xx (or a 2xx
    body carrying "replica access is read-only")."""
    LOG.info("copy_from_write_rejection step=%d: attempting CSV upload on settled REPLICA", step)
    csv_body = b"ts,v\n2020-01-01T00:00:00.000000Z,42\n"
    boundary = "----TestBoundary"
    body = (
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="data"; filename="test.csv"\r\n'
        f'Content-Type: text/plain\r\n'
        f'\r\n'
    ).encode() + csv_body + f'\r\n--{boundary}--\r\n'.encode()
    headers = {
        "Content-Type": f"multipart/form-data; boundary={boundary}",
        "Content-Length": str(len(body)),
        "Authorization": "Basic YWRtaW46cXVlc3Q=",
    }
    path = f"/upload?name={urllib.parse.quote(table)}&timestamp=ts&fmt=csv"
    try:
        conn = http.client.HTTPConnection("127.0.0.1", http_port, timeout=10)
        try:
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
            status = resp.status
            resp_body = resp.read().decode("utf-8", errors="replace")
        finally:
            conn.close()
    except Exception as exc:
        raise AssertionError(
            f"copy_from_write_rejection step={step}: upload raised a connection error on settled "
            f"REPLICA: {exc!r}. Expected a clean refusal with 'replica access is read-only'."
        ) from exc

    # The upload must be refused WITH EVIDENCE of the read-only gate: the
    # canonical "read-only" reason must appear in the body. Accepting any non-2xx
    # status on its own is too loose -- a 404 (path changed), 500 (unrelated
    # fault), or 401 would pass and mask a broken read-only gate. Requiring the
    # reason covers every acceptable case (a 2xx import-report carrying the error
    # or a non-2xx error body) and rejects both a silent 2xx accept and a bare
    # non-2xx with no read-only evidence. Mirrors the /exec check, which pins the
    # refusal reason rather than trusting the status alone.
    assert "read-only" in resp_body.lower(), (
        f"copy_from_write_rejection step={step}: CSV upload to settled REPLICA returned HTTP "
        f"{status} without a 'replica access is read-only' refusal reason in the body. "
        f"body={resp_body!r}. The engine-level getWriter gate must refuse the import path with "
        f"the read-only reason -- a bare non-2xx or a silent 2xx is not acceptable evidence."
    )
    LOG.info(
        "copy_from_write_rejection step=%d: REPLICA refused import with HTTP %d (OK)", step, status
    )


def _assert_ilp_udp_write_rejection_documented(step: int) -> None:
    """Documents that line-UDP write refusal is covered by the engine-level
    getWriter gate. The line-UDP ingest path acquires a TableWriter via
    engine.getWriter on every row batch; the EntCairoEngine.getWriter override
    refuses client lock reasons (including "ilpUdp"), so any UDP row to a
    read-only replica is dropped with the canonical "replica access is read-only"
    CairoException. The e2e UDP receiver is not enabled in this fs:// object-store
    environment, so a live UDP send cannot be driven here; the gate coverage is
    verified by ReadOnlyWriterAcquireRefusalTest (JVM-level)."""
    LOG.info(
        "ilp_udp_write_rejection step=%d: gate coverage verified by JVM-level unit test "
        "(line-UDP receiver not enabled in e2e fs:// environment)", step
    )


# ---------------------------------------------------------------------------
# Background ingest thread (QWP sidecar -- the ingest client)
# ---------------------------------------------------------------------------


def _ingest_loop(
    go_sidecar: GoSidecar,
    table: str,
    stop_flag: threading.Event,
    pause_flag: threading.Event,
    index_counter: list,  # [current_index] -- mutable single-element list
    error_holder: list,   # [exception | None]
    send_failures: list,  # [count, last_exc_repr] -- surfaced send-path failures
) -> None:
    """Background thread that continuously sends single-row frames via the QWP
    sidecar. Respects stop_flag (permanent exit) and pause_flag (temporary
    quiesce): when pause_flag is set the loop stops sending but keeps spinning,
    so a subsequent pause_flag.clear() resumes ingest on the SAME thread (a
    dead Python thread cannot be restarted). Only stop_flag exits the loop.
    Stores any thread-fatal exception in error_holder[0].

    SEND-path failures are NOT swallowed silently. A send can legitimately be
    refused while the node is mid-switch (under the demote-close contract an
    in-window send normally BUFFERS to SF without error, but a transient refusal
    racing the demote is legitimate), so a single failure is not fatal on its own
    -- but it IS recorded: send_failures[0] counts failures and send_failures[1]
    keeps the last repr. The main thread reconciles that evidence at the iteration
    oracle (a run where EVERY send failed shows zero post-baseline durable-ack
    growth and trips the growth floor)."""
    try:
        while not stop_flag.is_set():
            if pause_flag.is_set():
                # Quiesced for a watermark capture: hold off sending but keep
                # the thread alive so ingest can resume on pause_flag.clear().
                time.sleep(0.02)
                continue
            idx = index_counter[0]
            try:
                go_sidecar.send(table, count=1, start_index=idx)
                index_counter[0] = idx + 1
            except Exception as exc:
                send_failures[0] += 1
                send_failures[1] = repr(exc)
                LOG.warning(
                    "ingest_loop: send raised at index=%d (failure #%d): %s",
                    idx, send_failures[0], exc,
                )
            time.sleep(0.05)
    except Exception as exc:
        error_holder[0] = exc


# ---------------------------------------------------------------------------
# Main fuzz chain (chain / survival / read-prober / exact-count)
# ---------------------------------------------------------------------------


def _run_switch_fuzz_chain(
    seed: int,
    rng: random.Random,
    label: str,
    server_factory,
    go_sidecar: GoSidecar,
    go_egress_sidecar: EgressGoSidecar,
    obj_store,
    scenario_dir: Path,
) -> None:
    """Core multi-flip P->R->P chain shared by the random fuzz and pinned-seed
    tests. Drives chain_len round-trips with a live background ingest thread,
    per-apex quiescence + exact-count oracle, and write-rejection asserts.
    label is used in thread names and failure messages."""
    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    # ---- 1. Boot server with min-http control plane ----
    p1 = server_factory("p1")
    p1_ports = p1.start(min_http=True)
    assert p1_ports.min_http is not None, (
        "min_http port not reported; check ForkedEntServer READY line"
    )

    # ---- 2. Connect sidecar and start background ingest thread ----
    connect_str = (
        f"ws::addr=127.0.0.1:{p1_ports.http}"
        ";username=admin;password=quest"
        f";sf_dir={sf_dir}"
        ";request_durable_ack=on"
        ";reconnect_max_duration_millis=60000"
        ";close_flush_timeout_millis=5000;"
    )
    go_sidecar.connect(connect_str)

    # Send a small baseline as single-row frames (matching the ingest loop's
    # cadence) so every frame carries exactly one row for the WHOLE run -- that
    # keeps the durable watermark mappable to a durably-acked ROW count.
    baseline_rows = rng.randint(10, 50)
    for i in range(baseline_rows):
        go_sidecar.send(_TABLE, count=1, start_index=i)
    go_sidecar.flush()
    LOG.info("baseline rows sent and flushed: %d (single-row frames)", baseline_rows)

    stop_flag = threading.Event()
    pause_flag = threading.Event()  # quiesce/resume, distinct from stop_flag
    index_counter = [baseline_rows]  # next row index to send
    error_holder: list = [None]
    send_failures: list = [0, None]

    ingest_thread = threading.Thread(
        target=_ingest_loop,
        args=(go_sidecar, _TABLE, stop_flag, pause_flag, index_counter, error_holder, send_failures),
        name=f"ingest-{label}",
        daemon=True,
    )
    ingest_thread.start()
    LOG.info("background ingest thread started (live ingest across chain)")

    # ---- 3. Survival-plane witnesses + QWeP probe (opened INSIDE the try) ----
    _stats_before = go_sidecar.stats()
    reconn_before = _stats_before.reconn_succ
    reconn_attempts_before = _stats_before.reconn_attempts
    jdbc_conn = None
    http_conn = None
    qwep_for_probes = None

    # ---- 4. Drive the chain ----
    chain_len = rng.randint(1, 6)
    LOG.info("chain_len=%d", chain_len)

    all_probe_violations: list[str] = []

    try:
        # Open the witnesses inside the try so the finally always tears them down.
        jdbc_conn = psycopg.connect(
            host="127.0.0.1",
            port=p1_ports.pg,
            user="admin",
            password="quest",
            dbname="qdb",
            connect_timeout=10,
            autocommit=True,
        )
        http_conn = http.client.HTTPConnection("127.0.0.1", p1_ports.http, timeout=10)

        # The egress sidecar must be CONNECTED before it can serve as the QWeP
        # availability-plane witness (the fixture only starts the process).
        qwep_connect = (
            f"ws::addr=127.0.0.1:{p1_ports.http}"
            ";username=admin;password=quest"
            ";failover_max_duration_ms=10000"
            ";auth_timeout_ms=5000;"
        )
        try:
            go_egress_sidecar.connect(qwep_connect)
            qwep_for_probes = go_egress_sidecar
            LOG.info("egress QWeP sidecar connected (availability plane)")
        except Exception as exc:
            # The QWeP availability-plane witness is required: silently skipping
            # it would let a broken egress / QwpQueryClient path pass green. The
            # connect runs on a settled PRIMARY before the chain, so a failure
            # here is a real setup fault, not a transient switch-window blip.
            raise AssertionError(
                f"egress QWeP sidecar failed to connect on settled PRIMARY: {exc!r}. "
                f"The availability-plane probe cannot be skipped."
            ) from exc

        for step in range(chain_len):
            # ---- LIVE PLANE: submit P->R, probe during switch, quiesce at apex ----
            LOG.info("chain step=%d: P->R (live probe + quiescence at apex)", step)

            # Observe the durable ack WHILE STILL PRIMARY, before the demote is
            # submitted. The durable watermark advances only when the client I/O
            # loop reads an inbound STATUS_DURABLE_ACK frame; once demote-rejected
            # that read can no longer happen. flush() returns the highest published
            # FSN; await_acked drives the I/O loop until that FSN is durably acked
            # (or the bounded timeout elapses), so the acked-loss oracle and growth
            # floor have a real non-(-1) lower bound.
            published = go_sidecar.flush()
            if published >= 0:
                assert go_sidecar.await_acked(published, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
                    f"pre-switch durable-ack barrier step={step}: rows published up to "
                    f"FSN={published} were not durably acked within {_DURABLE_ACK_AWAIT_TIMEOUT_MS} ms "
                    f"while still PRIMARY. The durable watermark must advance before the demote so the "
                    f"acked-loss oracle has a real (non -1) lower bound."
                )

            lc.submit_switch(
                p1_ports.min_http,
                "replica",
                timeout_ms=_SWITCH_TIMEOUT_MS,
                wait=False,
            )

            probe_deadline = time.monotonic() + 25.0
            v = _run_in_switch_probes(p1_ports, qwep_for_probes, probe_deadline)
            all_probe_violations.extend(v)

            lc.await_role(p1_ports.min_http, "replica", timeout_s=_AWAIT_ROLE_TIMEOUT_S)

            # ---- QUIESCED COUNT PLANE: pause ingest, capture watermark, assert == ----
            watermark_expected = _quiesce_and_watermark(
                p1_ports, go_sidecar, pause_flag, index_counter, _TABLE, step
            )

            actual_count = count_rows(port=p1_ports.pg, table=_TABLE)
            assert actual_count == watermark_expected, (
                f"BOUNDARY-RACE REGRESSION at step={step}: "
                f"count_rows={actual_count} != watermark_expected={watermark_expected}. "
                f"A QWP connection or pg-wire client landed {actual_count - watermark_expected} "
                f"excess row(s) on the settled REPLICA. The QWP acceptOpen gate regressed OR a "
                f"pg-wire write bypassed the replica read-only guard. The watermark was captured "
                f"at the per-apex quiescence point (the == oracle is sound because ingest was "
                f"paused before the watermark was taken)."
            )
            LOG.info("exact-count apex step=%d: count_rows=%d == watermark_expected=%d (OK)",
                     step, actual_count, watermark_expected)

            # ---- ACKED-ROW-LOSS RECONCILIATION (loss direction) ----
            # The == oracle covers the EXCESS direction. The LOSS direction (a
            # durably-acked row gone MISSING) is reconciled against the DURABLE
            # WATERMARK (getAckedFsn(), stats().acked) -- NOT stats().acks. Every
            # frame carries one row, so the watermark maps to a durable ROW count.
            acked_rows = _durable_acked_rows(go_sidecar.stats())
            assert actual_count >= acked_rows, (
                f"ACKED-ROW LOSS at step={step}: count_rows={actual_count} < durable_acked_rows={acked_rows}. "
                f"The sidecar durably acked {acked_rows} rows (request_durable_ack=on, so each was "
                f"persisted before the durable watermark advanced), but only {actual_count} are present "
                f"on the settled REPLICA -- {acked_rows - actual_count} durably-acked row(s) went missing "
                f"across the switch. A durable ack is a persistence promise, so a missing durably-acked "
                f"row is data loss."
            )
            LOG.info("acked reconciliation step=%d: count_rows=%d >= durable_acked_rows=%d (no acked-row loss)",
                     step, actual_count, acked_rows)

            # Write-rejection oracle: INSERT on settled REPLICA must be cleanly
            # rejected on every client write path -- pg-wire and web-http /exec
            # (separate dispatch), plus the engine-level getWriter gate (CSV import
            # + line-UDP acquire a writer directly, bypassing the per-statement gate).
            _assert_write_rejection(p1_ports.pg, _TABLE, step)
            _assert_http_exec_write_rejection(p1_ports.http, _TABLE, step)
            _assert_copy_from_write_rejection(p1_ports.http, _TABLE, step)
            _assert_ilp_udp_write_rejection_documented(step)

            # Resume ingest for the next leg (R->P + next P->R). The ingest
            # thread is still alive (pause_flag, not stop_flag), so clearing
            # the pause resumes it.
            pause_flag.clear()
            LOG.info("chain step=%d: ingest resumed after REPLICA apex assertion", step)

            # ---- LIVE PLANE: submit R->P, probe during switch ----
            LOG.info("chain step=%d: R->P (live probe)", step)
            lc.submit_switch(
                p1_ports.min_http,
                "primary",
                timeout_ms=_SWITCH_TIMEOUT_MS,
                wait=False,
            )

            probe_deadline = time.monotonic() + 25.0
            v = _run_in_switch_probes(p1_ports, qwep_for_probes, probe_deadline, target_role="primary")
            all_probe_violations.extend(v)

            lc.await_role(p1_ports.min_http, "primary", timeout_s=_AWAIT_ROLE_TIMEOUT_S)
            snap_p = lc.lifecycle(p1_ports.min_http)
            LOG.info("chain step=%d: settled as PRIMARY, snapshot=%s", step, snap_p)

        # ---- 5. client-survival fingerprint after the full chain ----
        stop_flag.set()
        ingest_thread.join(timeout=10.0)

        if error_holder[0] is not None:
            raise RuntimeError(
                f"Background ingest thread raised an exception: {error_holder[0]!r}"
            ) from error_holder[0]

        # ---- DRAIN BARRIER (Invariant B): buffered window rows must ship ----
        # Every P->R leg demote-closed the QWP ingress connection while the ingest
        # thread was streaming, so the tail of the run's rows sits in on-disk SF
        # until the sender reconnects to the re-promoted primary. The chain ends
        # settled as PRIMARY: flush to publish anything pending and await the
        # durable ack for the highest published FSN.
        final_published = go_sidecar.flush()
        if final_published >= 0:
            assert go_sidecar.await_acked(final_published, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
                f"SF DRAIN FAILED after the chain (chain_len={chain_len}): rows published "
                f"up to FSN={final_published} were not durably acked within "
                f"{_DURABLE_ACK_AWAIT_TIMEOUT_MS} ms although the node settled as PRIMARY. "
                f"Invariant B promises rows buffered across the demote-close window ship "
                f"once a primary is reachable again."
            )
            LOG.info("drain barrier: durable ack reached FSN=%d after the chain", final_published)

        # ---- GROWTH FLOOR: kill the vacuous zero-ingest pass ----
        # Every oracle above is conditional on ingest having happened. Require real
        # progress on TWO planes: the send counter advanced past the baseline, and
        # the durable watermark is non-zero (at least one row durably persisted).
        # The floor anchors "ingest happened" on non-zero durable-watermark progress
        # (not on exceeding the baseline: with request_durable_ack=on the watermark
        # legitimately LAGS the table contents).
        final_acked_rows = _durable_acked_rows(go_sidecar.stats())
        final_count = count_rows(port=p1_ports.pg, table=_TABLE)
        sends_attempted = index_counter[0] - baseline_rows
        LOG.info(
            "growth floor: baseline_rows=%d sends_attempted=%d final_count=%d final_acked_rows=%d send_failures=%d",
            baseline_rows, sends_attempted, final_count, final_acked_rows, send_failures[0],
        )
        assert sends_attempted > 0, (
            f"ZERO-INGEST ITERATION (vacuous pass) for chain_len={chain_len}: the background "
            f"ingest thread attempted no rows past the baseline of {baseline_rows} "
            f"(index_counter={index_counter[0]}). Every count/acked oracle is vacuous when "
            f"the table never grew. send_failures={send_failures[0]} last_send_error={send_failures[1]!r}"
        )
        assert final_acked_rows > 0, (
            f"NO DURABLE INGEST (vacuous pass) for chain_len={chain_len}: the sidecar durably "
            f"acked ZERO rows ({sends_attempted} sends attempted, final_count={final_count}). "
            f"With request_durable_ack=on, an iteration that never received a single durable ack "
            f"has a dead ingest path. send_failures={send_failures[0]} last_send_error={send_failures[1]!r}"
        )
        if send_failures[0] > 0:
            LOG.warning(
                "ingest send path saw %d failure(s) across the chain (last=%s); the growth "
                "floor + acked reconciliation held, so a partial set of switch-window "
                "rejections is expected and tolerated",
                send_failures[0], send_failures[1],
            )

        # Plane 1: QWP ingress reconn behavior (BOUNDED under Invariant B). An
        # in-place demote CLOSES the ingress connection with a reconnect-eligible
        # NORMAL_CLOSURE and the SF sender reconnects once the node is PRIMARY
        # again, so one reconnect per leg is EXPECTED (typically delta==chain_len).
        # No lower bound (reconnect is timing dependent; delta==0 is reachable). The
        # upper bound (chain_len + slack) still catches a reconnect storm.
        stats_after = go_sidecar.stats()
        reconn_after = stats_after.reconn_succ
        reconn_delta = reconn_after - reconn_before
        reconn_attempts_delta = stats_after.reconn_attempts - reconn_attempts_before
        reconn_upper = chain_len + _RECONN_SLACK
        LOG.info(
            "reconn: succ before=%d after=%d delta=%d attempts_delta=%d "
            "(chain_len=%d; delta>0 EXPECTED under Invariant B; bounded by chain_len+%d=%d)",
            reconn_before, reconn_after, reconn_delta, reconn_attempts_delta,
            chain_len, _RECONN_SLACK, reconn_upper,
        )
        assert reconn_delta <= reconn_upper, (
            f"RECONNECT STORM: SF sender reconnected {reconn_delta} times across the "
            f"chain (reconn_succ before={reconn_before} after={reconn_after}), but a "
            f"chain_len={chain_len} P->R->P run should demote-close and reconnect about "
            f"once per leg -- bound is chain_len+{_RECONN_SLACK}={reconn_upper}. A delta "
            f"this high points at a flapping/looping reconnect on the SF ingress path."
        )
        assert reconn_attempts_delta <= reconn_upper, (
            f"RECONNECT-ATTEMPT STORM: SF sender made {reconn_attempts_delta} reconnect "
            f"attempts across the chain (chain_len={chain_len}, bound "
            f"chain_len+{_RECONN_SLACK}={reconn_upper}). Even if few succeeded, an "
            f"attempt count this high is a flapping reconnect loop on the SF ingress path."
        )

        # Plane 2: pre-opened psycopg Connection must still execute SELECT 1.
        with jdbc_conn.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
            assert row is not None, (
                "pre-opened psycopg connection returned None on SELECT 1 after chain "
                f"(chain_len={chain_len}); the pg-wire connection must survive a graceful switch"
            )
            LOG.info("pg-wire plane: SELECT 1 returned %s (connection survived chain)", row)

        # Plane 3: pre-opened HTTP keep-alive connection must return 200.
        http_conn.request(
            "GET",
            "/exec?query=SELECT+1&limit=1",
            headers={"Authorization": "Basic YWRtaW46cXVlc3Q="},
        )
        http_resp = http_conn.getresponse()
        http_resp.read()
        assert http_resp.status == 200, (
            f"HTTP keep-alive probe returned {http_resp.status} after chain "
            f"(chain_len={chain_len}); expected 200"
        )
        LOG.info("HTTP plane: keep-alive probe returned 200 (connection survived chain)")

        # ---- 6. in-switch-freeze assertion: no bound violations during any window ----
        assert not all_probe_violations, (
            f"IN-SWITCH FREEZE DETECTED: {len(all_probe_violations)} probe(s) exceeded "
            f"latency bounds or returned None (freeze) during the switch chain. "
            f"chain_len={chain_len}. Bounds from lib.probes (D-23 characterization). "
            f"A None return means the server did not respond within the probe timeout. "
            f"Violations:\n  " + "\n  ".join(all_probe_violations)
        )

    except AssertionError as exc:
        raise AssertionError(
            f"seed={seed} label={label} chain_len={chain_len}: {exc}"
        ) from exc
    finally:
        stop_flag.set()
        ingest_thread.join(timeout=5.0)
        if jdbc_conn is not None:
            try:
                jdbc_conn.close()
            except Exception:
                pass
        if http_conn is not None:
            try:
                http_conn.close()
            except Exception:
                pass
        try:
            go_egress_sidecar.close()
        except Exception:
            pass


@pytest.mark.parametrize("iteration", range(_fuzz_iters()))
def test_switch_fuzz(
    iteration: int,
    server_factory,
    go_sidecar: GoSidecar,
    go_egress_sidecar: EgressGoSidecar,
    obj_store,
    scenario_dir: Path,
) -> None:
    """Seed-isolated switch-roundtrip fuzz with the chain / survival / read-prober
    / exact-count oracles.

    For each iteration: boot p1 with min_http, connect the QWP sidecar + start a
    background ingest thread, open pg-wire + HTTP keep-alive survival witnesses,
    drive chain_len P->R->P round-trips (probing HTTP/PG/QWeP during each switch
    window; quiescing + asserting count_rows == watermark_expected + write-rejection
    at each REPLICA apex), then ack-barrier the SF drain (Invariant B), assert the
    client-survival fingerprint (reconn delta bounded, not flat), and assert no
    in-switch freeze."""
    base_seed = int(os.environ.get("QDB_E2E_FUZZ_SEED", str(time.time_ns() & 0xFFFFFFFF)))
    seed = _seed_for_iteration(base_seed, iteration)
    # DEDICATED Random -- never module-level random.* (pytest-randomly reseeds the
    # global stdlib random per test; using it here would break QDB_E2E_FUZZ_SEED repro).
    rng = random.Random(seed)

    LOG.info("fuzz iteration=%d seed=%d (base=%d)", iteration, seed, base_seed)

    try:
        _run_switch_fuzz_chain(
            seed=seed,
            rng=rng,
            label=str(iteration),
            server_factory=server_factory,
            go_sidecar=go_sidecar,
            go_egress_sidecar=go_egress_sidecar,
            obj_store=obj_store,
            scenario_dir=scenario_dir,
        )
    except AssertionError as exc:
        repro = (
            f"QDB_E2E_FUZZ_SEED={base_seed} "
            f"pytest -m fuzz "
            f"tests/test_switch_fuzz.py::test_switch_fuzz[{iteration}]"
        )
        raise AssertionError(
            f"fuzz iteration={iteration} base_seed={base_seed} "
            f"derived_seed={seed}: {exc}\n"
            f"To reproduce: {repro}"
        ) from exc


# ---------------------------------------------------------------------------
# PITR variant: live-client across recover-boot + first switch (#092)
# ---------------------------------------------------------------------------


def _write_recover_point_in_time(data_root: Path, source_obj_store_uri: str) -> Path:
    """Write the _recover_point_in_time Properties sentinel file.

    Source: PointInTimeRecoveryConfiguration.java -- Properties format with
    EXACTLY two keys (extra keys cause boot to fail). The timestamp is an
    ISO-8601 far-future string (Long.MAX_VALUE NOT accepted)."""
    sentinel = data_root / _RECOVER_SENTINEL
    content = (
        f"replication.object.store={source_obj_store_uri}\n"
        f"replication.recovery.timestamp={_RECOVERY_LATEST_TIMESTAMP}\n"
    )
    sentinel.write_text(content)
    return sentinel


def test_switch_fuzz_pitr(
    server_factory,
    go_sidecar: GoSidecar,
    go_egress_sidecar: EgressGoSidecar,
    obj_store,
    scenario_dir: Path,
) -> None:
    """PITR variant: live QWP client ingesting across PITR-recovered-boot + first switch.

    Boots a PITR-recovered primary by writing the _recover_point_in_time sentinel
    before starting the server; the ForkedEntServer picks it up on boot and
    recovers from the source object store. The source object store is the SAME
    filesystem obj_store used by the original primary (already populated with WAL
    data). The recovery target (the new primary's runtime upload location) must be
    a DIFFERENT empty object store (the Rust uploader raises ER001 if the PITR
    target is non-empty).

    Scope: ONE high-value deterministic PITR e2e test (not a fuzz sub-dimension).

    #092 guard: if count_rows < watermark_expected at the quiescence point after
    the PITR-recovered-boot + first switch, that is a product bug (UDP ingest
    silently dropped when acceptOpen=false during PITR-restore boot)."""
    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    # Phase 1: Boot original primary, ingest rows, let them replicate to obj_store.
    LOG.info("PITR variant: Phase 1 -- boot original primary and ingest")
    p1 = server_factory("p1-pitr-source")
    p1_ports = p1.start(min_http=True)
    assert p1_ports.min_http is not None, "min_http port not reported"

    connect_str = (
        f"ws::addr=127.0.0.1:{p1_ports.http}"
        ";username=admin;password=quest"
        f";sf_dir={sf_dir}"
        ";request_durable_ack=on"
        ";reconnect_max_duration_millis=60000"
        ";close_flush_timeout_millis=5000;"
    )
    go_sidecar.connect(connect_str)

    # Send rows to the source primary and flush. With request_durable_ack=on,
    # flush() + await_acked below block until the source primary has durably
    # acknowledged the upload to the object store -- by then the rows are remote.
    pre_pitr_rows = 50
    go_sidecar.send(_TABLE, count=pre_pitr_rows, start_index=0)
    published = go_sidecar.flush()
    if published >= 0:
        assert go_sidecar.await_acked(published, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
            f"PITR source-primary drain: rows published up to FSN={published} were not "
            f"durably acked within {_DURABLE_ACK_AWAIT_TIMEOUT_MS} ms. PITR recovery reads the "
            f"object store, so a row that never became durable would be missing after recovery "
            f"and silently weaken the #092 guard."
        )
    time.sleep(2.0)  # belt-and-braces slack for any async index/cleanup tail
    LOG.info("PITR variant: %d rows sent and durably acked to source primary", pre_pitr_rows)

    # Stop the source primary (graceful SIGTERM).
    p1.stop()
    LOG.info("PITR variant: source primary stopped")

    # Phase 2: Boot PITR-recovered primary from the source obj_store. The PITR
    # target MUST be a different (empty) location.
    pitr_target_dir = scenario_dir / "pitr_target"
    pitr_target_dir.mkdir(parents=True, exist_ok=True)
    pitr_target_obj = pitr_target_dir / "objstore" / "root"
    pitr_target_scratch = pitr_target_dir / "objstore" / "scratch"
    pitr_target_obj.mkdir(parents=True, exist_ok=True)
    pitr_target_scratch.mkdir(parents=True, exist_ok=True)
    pitr_target_uri = f"fs::root={pitr_target_obj};atomic_write_dir={pitr_target_scratch};"

    # The recovered primary needs its own data root (cannot reuse the source's).
    pitr_db_root = scenario_dir / "pitr_db"
    pitr_db_root.mkdir(parents=True, exist_ok=True)
    pitr_db_root_db = pitr_db_root / "db"
    pitr_db_root_db.mkdir(parents=True, exist_ok=True)

    # Write the _recover_point_in_time sentinel pointing at the SOURCE obj_store.
    sentinel = _write_recover_point_in_time(pitr_db_root_db, obj_store.uri)
    LOG.info("PITR variant: sentinel written at %s (source=%s)", sentinel, obj_store.uri)

    from lib.server import ForkedEntServer
    from lib.classpath import build_classpath
    pitr_server = ForkedEntServer(
        db_root=pitr_db_root_db,
        object_store_uri=pitr_target_uri,
        log_dir=scenario_dir / "logs" / "pitr-recovered",
        classpath=build_classpath(),
        name="p1-pitr-recovered",
    )
    # The recovered primary is a DIRECT ForkedEntServer (no server_factory
    # fixture teardown net), so the boot -> connect -> ingest-setup window must
    # run INSIDE the try/finally: a failure here (boot timeout, connect refusal)
    # must still stop the JVM instead of leaking it into later tests.
    pitr_stop_flag = threading.Event()
    pitr_pause_flag = threading.Event()
    pitr_ingest_thread = None
    qwep_for_probes = None
    try:
        LOG.info("PITR variant: Phase 2 -- booting PITR-recovered primary")
        pitr_ports = pitr_server.start(min_http=True, ready_timeout=180.0)
        assert pitr_ports.min_http is not None, "PITR-recovered server: min_http port not reported"
        LOG.info("PITR variant: recovered primary ready at %s", pitr_ports)

        # Connect a LIVE QWP client to the recovered primary.
        sf_dir_pitr = scenario_dir / "sf_pitr"
        sf_dir_pitr.mkdir(parents=True, exist_ok=True)
        pitr_connect_str = (
            f"ws::addr=127.0.0.1:{pitr_ports.http}"
            ";username=admin;password=quest"
            f";sf_dir={sf_dir_pitr}"
            ";request_durable_ack=on"
            ";reconnect_max_duration_millis=60000"
            ";close_flush_timeout_millis=5000;"
        )
        go_sidecar.connect(pitr_connect_str)

        # Start live ingest on the recovered primary (fresh index from 0).
        pitr_index_counter = [0]
        pitr_error_holder: list = [None]
        pitr_send_failures: list = [0, None]

        pitr_ingest_thread = threading.Thread(
            target=_ingest_loop,
            args=(go_sidecar, _TABLE, pitr_stop_flag, pitr_pause_flag, pitr_index_counter,
                  pitr_error_holder, pitr_send_failures),
            name="pitr-ingest",
            daemon=True,
        )
        pitr_ingest_thread.start()
        time.sleep(0.5)  # let a few rows arrive before the first switch
        LOG.info("PITR variant: live ingest started on recovered primary")

        qwep_connect = (
            f"ws::addr=127.0.0.1:{pitr_ports.http}"
            ";username=admin;password=quest"
            ";failover_max_duration_ms=10000"
            ";auth_timeout_ms=5000;"
        )
        try:
            go_egress_sidecar.connect(qwep_connect)
            qwep_for_probes = go_egress_sidecar
            LOG.info("PITR variant: egress QWeP sidecar connected")
        except Exception as exc:
            # Required witness: a silent skip would let a broken egress path pass
            # green. The recovered primary is settled here, so a connect failure
            # is a real setup fault.
            raise AssertionError(
                f"PITR variant: egress QWeP sidecar failed to connect on the recovered "
                f"primary: {exc!r}. The availability-plane probe cannot be skipped."
            ) from exc

        # First switch: P->R with live ingest.
        LOG.info("PITR variant: first switch P->R")

        # Observe the durable ack WHILE STILL PRIMARY (same reasoning as the main
        # chain): an await after the demote-reject would be too late to read the
        # inbound ack.
        pitr_published = go_sidecar.flush()
        if pitr_published >= 0:
            assert go_sidecar.await_acked(pitr_published, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
                f"PITR pre-switch durable-ack barrier: rows published up to FSN={pitr_published} "
                f"were not durably acked within {_DURABLE_ACK_AWAIT_TIMEOUT_MS} ms while still "
                f"PRIMARY, so the acked-loss oracle would have no real lower bound."
            )

        lc.submit_switch(
            pitr_ports.min_http,
            "replica",
            timeout_ms=_SWITCH_TIMEOUT_MS,
            wait=False,
        )

        probe_deadline = time.monotonic() + 25.0
        pitr_violations = _run_in_switch_probes(pitr_ports, qwep_for_probes, probe_deadline)

        lc.await_role(pitr_ports.min_http, "replica", timeout_s=_AWAIT_ROLE_TIMEOUT_S)
        LOG.info("PITR variant: REPLICA settled after first switch")

        # QUIESCED PLANE: pause ingest, capture watermark, assert count == watermark.
        # This is the #092 guard: if count < watermark_expected, UDP ingest was dropped.
        pitr_watermark = _quiesce_and_watermark(
            pitr_ports, go_sidecar, pitr_pause_flag, pitr_index_counter, _TABLE,
            step=0,
        )

        pitr_actual = count_rows(port=pitr_ports.pg, table=_TABLE)
        assert pitr_actual == pitr_watermark, (
            f"#092 PITR INGEST GUARD: count_rows={pitr_actual} != watermark_expected={pitr_watermark} "
            f"on PITR-recovered primary after first switch. QWP/UDP ingest may have been silently "
            f"dropped when acceptOpen=false during the PITR-restore boot (bug #092). Watermark "
            f"captured at quiescence point (consistent with the main fuzz protocol)."
        )
        LOG.info("PITR variant: #092 guard PASSED -- count_rows=%d == watermark_expected=%d",
                 pitr_actual, pitr_watermark)

        # ACKED-ROW-LOSS RECONCILIATION + GROWTH FLOOR: every durably-acked send
        # must be present on the recovered+settled REPLICA, and the recovered
        # primary must have ingested past its fresh baseline of 0.
        pitr_acked_rows = _durable_acked_rows(go_sidecar.stats())
        assert pitr_actual >= pitr_acked_rows, (
            f"PITR ACKED-ROW LOSS: count_rows={pitr_actual} < durable_acked_rows={pitr_acked_rows} on the "
            f"recovered+settled REPLICA -- {pitr_acked_rows - pitr_actual} durably-acked row(s) went "
            f"missing across the PITR-restore boot + first switch. A durable ack is a persistence "
            f"promise; a missing durably-acked row is data loss. send_failures={pitr_send_failures[0]} "
            f"last_send_error={pitr_send_failures[1]!r}"
        )
        assert pitr_acked_rows > 0, (
            f"PITR ZERO-INGEST (vacuous pass): the recovered primary durably acked no rows "
            f"(durable_acked_rows={pitr_acked_rows}, sends_attempted={pitr_index_counter[0]}), so the "
            f"#092 == guard proved nothing about ingest survival across the PITR-restore boot. "
            f"send_failures={pitr_send_failures[0]} last_send_error={pitr_send_failures[1]!r}"
        )
        LOG.info(
            "PITR variant: acked reconciliation + growth floor PASSED -- count_rows=%d >= durable_acked_rows=%d > 0",
            pitr_actual, pitr_acked_rows,
        )

        # Write-rejection on settled REPLICA (pg-wire + web-http /exec).
        _assert_write_rejection(pitr_ports.pg, _TABLE, step=0)
        _assert_http_exec_write_rejection(pitr_ports.http, _TABLE, step=0)

        # in-switch-freeze assertion: no freeze during the switch.
        assert not pitr_violations, (
            f"IN-SWITCH FREEZE on PITR-recovered primary: "
            f"{len(pitr_violations)} violation(s):\n  " + "\n  ".join(pitr_violations)
        )

        LOG.info("PITR variant: PASSED -- #092 guard green, no freeze, write-rejection clean")

    finally:
        pitr_stop_flag.set()
        if pitr_ingest_thread is not None:
            pitr_ingest_thread.join(timeout=5.0)
        try:
            go_egress_sidecar.close()
        except Exception:
            pass
        try:
            pitr_server.stop()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Pinned-seed regression case: the indexer post-upload sync_id skew
# ---------------------------------------------------------------------------


def test_switch_fuzz_pinned_seed_5171257088512701991(
    server_factory,
    go_sidecar: GoSidecar,
    go_egress_sidecar: EgressGoSidecar,
    obj_store,
    scenario_dir: Path,
) -> None:
    """Pinned regression: the multi-flip P->R->P chain that exposed a debug-build
    crash in the uploader's post-upload sync_id cross-check. Replays the exact
    seed deterministically so this scenario stays covered even when the random
    fuzz draws different seeds."""
    seed = 5171257088512701991
    rng = random.Random(seed)
    LOG.info("pinned-seed chain: seed=%d", seed)
    _run_switch_fuzz_chain(
        seed=seed,
        rng=rng,
        label="pinned-5171257088512701991",
        server_factory=server_factory,
        go_sidecar=go_sidecar,
        go_egress_sidecar=go_egress_sidecar,
        obj_store=obj_store,
        scenario_dir=scenario_dir,
    )


# ---------------------------------------------------------------------------
# Bounded live-downloading-replica + promote fuzz dimension
# ---------------------------------------------------------------------------


def test_switch_fuzz_live_replica_failover(
    go_sidecar: GoSidecar,
    classpath: str,
    log_dir: Path,
    scenario_dir: Path,
) -> None:
    """Bounded live-replica failover fuzz dimension for recurring onward-convergence.

    Drives a two-node demote->promote failover a bounded number of times:
    max(1, QDB_E2E_FUZZ_ITERS // 5) cycles. Each cycle boots fresh A (primary) + B
    (replica) against a fresh per-cycle object store (so the DataID does not
    collide across cycles), ingests rng-chosen initial rows into A, waits for B to
    catch up, demotes A, promotes B, ingests onward rows into B, and asserts A (now
    a replica) converges on B's total rows -- the headline onward-convergence
    invariant -- then asserts both nodes shut down cleanly."""
    from lib.obj_store import make_obj_store
    from lib.pg_query import wait_for_dense_sequence
    from lib.server import ForkedEntServer
    from lib.shutdown import assert_clean_shutdown

    iters = _fuzz_iters()
    num_cycles = max(1, iters // 5)
    base_seed = int(os.environ.get("QDB_E2E_FUZZ_SEED", str(time.time_ns() & 0xFFFFFFFF)))

    LOG.info(
        "live_replica_failover: iters=%d num_cycles=%d base_seed=%d",
        iters, num_cycles, base_seed,
    )

    _FAILOVER_TABLE = "switch_fuzz_failover"
    _REPLICA_CONVERGE_TIMEOUT_S = 120.0
    _REPLICA_POLL_INTERVAL_S = 0.3
    _LOCAL_AWAIT_ROLE_TIMEOUT_S = 60.0

    def _wait_replica_converges_local(*, pg_port: int, expected: int,
                                      timeout_s: float = _REPLICA_CONVERGE_TIMEOUT_S) -> None:
        """Block until the replica shows the expected row count (stable read)."""
        deadline = time.monotonic() + timeout_s
        last = -1
        while time.monotonic() < deadline:
            try:
                last = count_rows(port=pg_port, table=_FAILOVER_TABLE, timeout_s=5.0)
            except Exception:
                last = -1
            if last == expected:
                LOG.info("live_replica_failover: replica converged count=%d == expected=%d", last, expected)
                return
            time.sleep(_REPLICA_POLL_INTERVAL_S)
        raise AssertionError(
            f"live_replica_failover: replica did not converge on {expected} rows "
            f"in {_FAILOVER_TABLE} within {timeout_s}s (last observed: {last}). "
            f"This is the headline onward-convergence invariant: A (now replica) must "
            f"download B's post-promote rows. A failure here means onward replication "
            f"broke after the hot in-place promote."
        )

    for cycle in range(num_cycles):
        cycle_seed = _seed_for_iteration(base_seed, cycle)
        rng = random.Random(cycle_seed)

        initial_rows = rng.randint(10, 40)
        onward_rows = rng.randint(5, 20)

        LOG.info(
            "live_replica_failover: cycle=%d/%d seed=%d initial_rows=%d onward_rows=%d",
            cycle, num_cycles, cycle_seed, initial_rows, onward_rows,
        )

        cycle_start = time.monotonic()

        # Per-cycle object store: each cycle needs its own fresh store to avoid
        # ER007 DataID collision across cycles.
        cycle_dir = scenario_dir / f"cycle_{cycle}"
        cycle_dir.mkdir(parents=True, exist_ok=True)
        cycle_store = make_obj_store(cycle_dir)

        db_a = cycle_dir / "db_a"
        db_b = cycle_dir / "db_b"
        db_a.mkdir(parents=True, exist_ok=True)
        db_b.mkdir(parents=True, exist_ok=True)

        a = ForkedEntServer(
            db_root=db_a,
            object_store_uri=cycle_store.uri,
            log_dir=log_dir,
            extra_env={
                "QDB_REPLICATION_ROLE": "primary",
            },
            classpath=classpath,
            name=f"failover-a-c{cycle}",
        )
        b = ForkedEntServer(
            db_root=db_b,
            object_store_uri=cycle_store.uri,
            log_dir=log_dir,
            extra_env={
                "QDB_REPLICATION_ROLE": "replica",
                "QDB_REPLICATION_REPLICA_POLL_INTERVAL": "5",
            },
            classpath=classpath,
            name=f"failover-b-c{cycle}",
        )

        # a and b are DIRECT ForkedEntServer instances (no server_factory
        # teardown net), so their start() must run INSIDE the try whose except
        # stops them -- otherwise a boot failure or a min_http assert leaks the
        # JVM(s) into later cycles/tests.
        try:
            a_ports = a.start(min_http=True)
            b_ports = b.start(min_http=True)
            assert a_ports.min_http is not None, (
                f"cycle {cycle}: node A (primary) min_http port not reported"
            )
            assert b_ports.min_http is not None, (
                f"cycle {cycle}: node B (replica) min_http port not reported; "
                f"the replica+min_http combination must bind the min-http listener"
            )
            LOG.info("live_replica_failover: cycle=%d a_ports=%s b_ports=%s",
                     cycle, a_ports, b_ports)

            sf_cycle_dir = scenario_dir / "sf" / f"c{cycle}_a"
            sf_cycle_dir.mkdir(parents=True, exist_ok=True)

            connect_str_a = (
                f"ws::addr=127.0.0.1:{a_ports.http}"
                ";username=admin;password=quest"
                f";sf_dir={sf_cycle_dir}"
                ";request_durable_ack=on"
                ";reconnect_max_duration_millis=60000"
                ";close_flush_timeout_millis=5000;"
            )

            go_sidecar.connect(connect_str_a)
            go_sidecar.send(_FAILOVER_TABLE, count=initial_rows, start_index=0)
            go_sidecar.flush()
            LOG.info("live_replica_failover: cycle=%d sent %d initial rows to A", cycle, initial_rows)

            wait_for_dense_sequence(port=a_ports.pg, table=_FAILOVER_TABLE,
                                    expected_count=initial_rows, timeout_s=60.0)
            LOG.info("live_replica_failover: cycle=%d A confirmed %d rows", cycle, initial_rows)

            _wait_replica_converges_local(pg_port=b_ports.pg, expected=initial_rows)
            LOG.info("live_replica_failover: cycle=%d B caught up with %d initial rows",
                     cycle, initial_rows)

            # Demote A: primary -> replica.
            lc.submit_switch(a_ports.min_http, "replica", wait=True,
                             wait_timeout_s=_LOCAL_AWAIT_ROLE_TIMEOUT_S)
            snap_a = lc.lifecycle(a_ports.min_http)
            assert snap_a.get("currentRole") == "REPLICA", (
                f"cycle {cycle}: A should be REPLICA after demotion, "
                f"got {snap_a.get('currentRole')!r}"
            )
            LOG.info("live_replica_failover: cycle=%d A demoted to REPLICA", cycle)

            # Promote B: replica -> primary.
            lc.submit_switch(b_ports.min_http, "primary", wait=True,
                             wait_timeout_s=_LOCAL_AWAIT_ROLE_TIMEOUT_S)
            snap_b = lc.lifecycle(b_ports.min_http)
            assert snap_b.get("currentRole") == "PRIMARY", (
                f"cycle {cycle}: B should be PRIMARY after promotion, "
                f"got {snap_b.get('currentRole')!r}"
            )
            LOG.info("live_replica_failover: cycle=%d B promoted to PRIMARY", cycle)

            # Ingest onward rows into B (the new primary).
            go_sidecar.close()
            sf_b_dir = scenario_dir / "sf" / f"c{cycle}_b"
            sf_b_dir.mkdir(parents=True, exist_ok=True)
            connect_str_b = (
                f"ws::addr=127.0.0.1:{b_ports.http}"
                ";username=admin;password=quest"
                f";sf_dir={sf_b_dir}"
                ";request_durable_ack=on"
                ";reconnect_max_duration_millis=60000"
                ";close_flush_timeout_millis=5000;"
            )
            go_sidecar.connect(connect_str_b)
            go_sidecar.send(_FAILOVER_TABLE, count=onward_rows, start_index=initial_rows)
            go_sidecar.flush()
            LOG.info("live_replica_failover: cycle=%d sent %d onward rows to B (start_index=%d)",
                     cycle, onward_rows, initial_rows)

            total_expected = initial_rows + onward_rows

            wait_for_dense_sequence(port=b_ports.pg, table=_FAILOVER_TABLE,
                                    expected_count=total_expected, timeout_s=60.0)
            LOG.info("live_replica_failover: cycle=%d B has all %d rows", cycle, total_expected)

            # Assert A (now a replica) converges on B's total rows -- onward replication.
            _wait_replica_converges_local(pg_port=a_ports.pg, expected=total_expected)
            LOG.info(
                "live_replica_failover: cycle=%d A (now replica) converged on B's %d total rows "
                "-- onward replication confirmed",
                cycle, total_expected,
            )

            go_sidecar.close()
            b.stop()
            a.stop()
            assert_clean_shutdown(b)
            assert_clean_shutdown(a)

        except Exception:
            try:
                go_sidecar.close()
            except Exception:
                pass
            try:
                b.stop()
            except Exception:
                pass
            try:
                a.stop()
            except Exception:
                pass
            raise

        cycle_elapsed = time.monotonic() - cycle_start
        LOG.info(
            "live_replica_failover: cycle=%d PASSED in %.1fs "
            "(initial_rows=%d onward_rows=%d total=%d)",
            cycle, cycle_elapsed, initial_rows, onward_rows, total_expected,
        )

    LOG.info("live_replica_failover: all %d cycle(s) PASSED (base_seed=%d)", num_cycles, base_seed)
