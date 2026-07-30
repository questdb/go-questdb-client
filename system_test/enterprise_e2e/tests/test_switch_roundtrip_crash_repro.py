"""
Regression guard for the QWP switch-roundtrip crash + boundary race (Go
client; ported from questdb-ent/e2e/tests/test_switch_roundtrip_crash_repro.py).

Exercises the path test_write_path_across_switch AVOIDS: an aggressive write
storm during the P->R settlement window (boundary race), then a sidecar
close + reconnect + fresh QWP writes AFTER the R->P switch-back. Asserts the
fixed server invariants hold:

  1. No durably acknowledged row is lost across the P->R boundary.
  2. No server crash; the JVM stays alive through the whole round-trip.
  3. After R->P switch-back a reconnected QWP connection can write again.
"""

from __future__ import annotations

import http.client
import logging
import os
import time
import urllib.parse
from pathlib import Path

import pytest

from lib.lifecycle import lifecycle, submit_switch
from lib.pg_query import count_rows, fetch_column_sorted

from go_sidecar import GoSidecar, GoSidecarError

LOG = logging.getLogger(__name__)

pytestmark = [pytest.mark.go_client]

TABLE = "go_switch_roundtrip"
_BASIC_AUTH = "Basic YWRtaW46cXVlc3Q="  # admin:quest
PRE = 100
_DURABLE_ACK_AWAIT_TIMEOUT_MS = 60_000


def _durably_acked_rows(accepted_by_fsn: list[tuple[int, int]], acked_fsn: int) -> int:
    rows = 0
    for fsn, cumulative_rows in accepted_by_fsn:
        if fsn <= acked_fsn:
            rows = max(rows, cumulative_rows)
    return rows


def _connect_string(http_port: int, sf_dir: Path) -> str:
    return (
        f"ws::addr=127.0.0.1:{http_port}"
        ";username=admin;password=quest"
        f";sf_dir={sf_dir}"
        ";request_durable_ack=on"
        ";reconnect_max_duration_millis=60000"
        ";close_flush_timeout_millis=5000;"
    )


def _crash_artifact_dir() -> Path:
    d = Path(os.environ.get(
        "QDB_CRASH_ARTIFACT_DIR",
        str(Path(__file__).resolve().parent.parent / "crash-artifacts"),
    ))
    d.mkdir(parents=True, exist_ok=True)
    return d


def _lifecycle_brief(min_http_port: int) -> str:
    try:
        snap = lifecycle(min_http_port)
        comps = {c.get("name"): c.get("state") for c in snap.get("components", [])}
        return f"role={snap.get('currentRole')} switchInFlight={snap.get('switchInFlight')} components={comps}"
    except Exception as exc:
        return f"<lifecycle unavailable: {exc}>"


def _http_insert(http_port: int, table: str, ts_micros: int) -> tuple[int, str]:
    sql = f'INSERT INTO "{table}"(v, timestamp) VALUES({ts_micros}, {ts_micros})'
    q = urllib.parse.quote(sql, safe="")
    conn = http.client.HTTPConnection("127.0.0.1", http_port, timeout=10)
    try:
        conn.request("GET", f"/exec?query={q}", headers={"Authorization": _BASIC_AUTH})
        resp = conn.getresponse()
        return resp.status, resp.read().decode("utf-8", errors="replace")
    finally:
        conn.close()


def _await_role_tolerant(min_http_port: int, role: str, *, timeout_s: float = 60.0) -> bool:
    target = role.upper()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            snap = lifecycle(min_http_port)
            if not snap.get("switchInFlight") and snap.get("currentRole") == target:
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False


def _submit_switch_tolerant(min_http_port: int, role: str, *, max_attempts: int = 5) -> bool:
    for _ in range(max_attempts):
        try:
            submit_switch(min_http_port, role, wait=False)
            return True
        except Exception:
            time.sleep(1.0)
    return False


def _signal_name(returncode) -> str:
    if returncode is None:
        return "alive"
    if returncode < 0:
        try:
            import signal as _sig
            return f"signal {_sig.Signals(-returncode).name} ({-returncode})"
        except Exception:
            return f"signal {-returncode}"
    return f"exit code {returncode}"


def test_roundtrip_post_switch_write_no_crash_no_replica_writes(
    server_factory, go_sidecar: GoSidecar, scenario_dir: Path, log_dir: Path, monkeypatch
) -> None:
    crash_dir = _crash_artifact_dir()
    sf_dir = scenario_dir / "sf"
    sf_dir.mkdir(parents=True, exist_ok=True)

    # Preserve any native crash stack outside the auto-cleaned scenario tree.
    # Use monkeypatch so the process-wide env change is restored at teardown and
    # does not leak the crash-file redirect into every server forked by later
    # tests in the session.
    error_file = crash_dir / "hs_err_p1_%p.log"
    fork_opts = os.environ.get("JAVA_OPTS_FORK", "-Xmx512m")
    monkeypatch.setenv("JAVA_OPTS_FORK", f"{fork_opts} -XX:ErrorFile={error_file}")

    p1 = server_factory("p1")
    p1_ports = p1.start(min_http=True)
    assert p1_ports.min_http is not None
    go_sidecar.connect(_connect_string(p1_ports.http, sf_dir))

    accepted_by_fsn: list[tuple[int, int]] = []
    accepted_rows = 0

    # Phase 1: pre-switch seed.
    go_sidecar.send(TABLE, count=PRE, start_index=0)
    seed_fsn = go_sidecar.flush()
    accepted_rows = PRE
    accepted_by_fsn.append((seed_fsn, accepted_rows))
    assert go_sidecar.await_acked(seed_fsn, _DURABLE_ACK_AWAIT_TIMEOUT_MS), (
        f"seed frame was not durably acked [fsn={seed_fsn}]"
    )
    time.sleep(0.5)
    assert count_rows(port=p1_ports.pg, table=TABLE) == PRE

    # Phase 2: P->R with an aggressive boundary-race write storm during settlement.
    assert _submit_switch_tolerant(p1_ports.min_http, "replica")
    boundary_accepted = boundary_rejected = 0
    idx = PRE
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        if not p1.is_alive():
            break
        try:
            go_sidecar.send(TABLE, count=1, start_index=idx)
            fsn = go_sidecar.flush()
            idx += 1
            accepted_rows += 1
            accepted_by_fsn.append((fsn, accepted_rows))
            boundary_accepted += 1
        except GoSidecarError:
            boundary_rejected += 1
        except Exception as exc:
            LOG.warning("switch_roundtrip: boundary write error: %s", exc)
        try:
            snap = lifecycle(p1_ports.min_http)
            if not snap.get("switchInFlight") and snap.get("currentRole") == "REPLICA":
                break
        except Exception:
            pass
        time.sleep(0.05)
    assert _await_role_tolerant(p1_ports.min_http, "replica"), "node did not settle as REPLICA"
    LOG.info("switch_roundtrip: REPLICA settled — boundary accepted=%d rejected=%d",
             boundary_accepted, boundary_rejected)

    # INVARIANT 1: no acknowledged data loss across the boundary.
    replica_count = count_rows(port=p1_ports.pg, table=TABLE)
    acked_fsn = go_sidecar.stats().acked
    durably_acked_rows = _durably_acked_rows(accepted_by_fsn, acked_fsn)
    assert p1.is_alive(), "server crashed during P->R boundary-write storm"
    assert replica_count >= durably_acked_rows, (
        f"ACKED-ROW LOSS: count_rows={replica_count} < durably_acked_rows={durably_acked_rows} "
        f"(acked_fsn={acked_fsn})"
    )
    assert replica_count <= accepted_rows, (
        f"UNEXPLAINED ROW GROWTH: count_rows={replica_count} > accepted_rows={accepted_rows}"
    )
    observed_values = fetch_column_sorted(port=p1_ports.pg, table=TABLE, timeout_s=5.0)
    assert observed_values == list(range(replica_count)), (
        f"DENSE ROW INVARIANT FAILED: expected [0..{replica_count}), got "
        f"first={observed_values[:10]} last={observed_values[-10:]}"
    )

    # Phase 3: R->P switch-back.
    assert _submit_switch_tolerant(p1_ports.min_http, "primary")
    assert _await_role_tolerant(p1_ports.min_http, "primary"), "node did not settle back to PRIMARY"
    assert p1.is_alive(), "server crashed during R->P switch-back"

    # Phase 4: the path that used to abort the JVM — reconnect + write + HTTP probe.
    qwp_results: list[str] = []
    http_results: list[str] = []
    try:
        go_sidecar.close()
        go_sidecar.connect(_connect_string(p1_ports.http, sf_dir))
    except Exception as exc:
        LOG.warning("switch_roundtrip: reconnect raised: %s", exc)

    probe_deadline = time.monotonic() + 20.0
    attempt = 0
    crash_observed = False
    crash_detail = ""
    while time.monotonic() < probe_deadline and not crash_observed:
        attempt += 1
        if not p1.is_alive():
            crash_observed = True
            crash_detail = f"server died before QWP attempt {attempt}"
            break
        try:
            go_sidecar.send(TABLE, count=1, start_index=idx)
            go_sidecar.flush()
            idx += 1
            qwp_results.append(f"a{attempt}:OK")
        except GoSidecarError as exc:
            qwp_results.append(f"a{attempt}:REJ({exc})")
        except Exception as exc:
            qwp_results.append(f"a{attempt}:ERR({exc})")
        if attempt % 2 == 0 and p1.is_alive():
            try:
                st, body = _http_insert(p1_ports.http, TABLE, 1_700_000_000_000_000 + attempt)
                http_results.append(f"a{attempt}:{st}:{body[:120]}")
            except Exception as exc:
                http_results.append(f"a{attempt}:HTTP_ERR({exc})")
        time.sleep(0.5)

    time.sleep(3.0)
    if not p1.is_alive():
        crash_observed = True
        rc = p1.process.returncode if p1.process else None
        crash_detail = (crash_detail + "; " if crash_detail else "") + f"server exited: {_signal_name(rc)}"

    final_count = count_rows(port=p1_ports.pg, table=TABLE)
    hs_err_files = sorted(crash_dir.glob("hs_err_p1_*.log"))
    stderr_log = log_dir / "p1.stderr.log"
    tail = ""
    if stderr_log.exists():
        tail = "\n".join(stderr_log.read_text(encoding="utf-8", errors="replace").splitlines()[-50:])

    # INVARIANT 2: no crash.
    assert not crash_observed, (
        "CRASH/SHUTDOWN REGRESSION on the post-R->P reconnect-write path.\n"
        f"detail: {crash_detail}\nboundary_accepted={boundary_accepted} rejected={boundary_rejected}\n"
        f"qwp_results={qwp_results}\nhttp_results={http_results}\n"
        f"hs_err: {[str(f) for f in hs_err_files]}\n--- p1.stderr tail ---\n{tail}\n"
    )

    # INVARIANT 3: after switch-back the engine admits PRIMARY writes again.
    qwp_ok = any(r.endswith(":OK") for r in qwp_results)
    http_ok = any(":200:" in r for r in http_results)
    assert qwp_ok or http_ok, (
        "POST-SWITCH WRITE REGRESSION: no QWP or HTTP write succeeded after R->P switch-back.\n"
        f"qwp_results={qwp_results}\nhttp_results={http_results}\n"
    )
    assert final_count > PRE, (
        f"POST-SWITCH WRITE REGRESSION: final committed count {final_count} did not exceed seed {PRE}"
    )
    LOG.info("switch_roundtrip: PASS — no crash, PRIMARY admits writes; final_count=%d", final_count)
