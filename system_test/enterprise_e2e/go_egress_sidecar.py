"""
Go egress sidecar driver for the Enterprise e2e suite.

Drives the read-side ``QwpQueryClient`` (via ``egress_sidecar/main.go``,
compiled to ``go-e2e-egress-sidecar``) over the same line-based
stdin/stdout protocol as the Enterprise Java egress sidecar
(``lib/egress_sidecar.py``): ``CONNECT``, ``SHOW_ZONE``, ``SERVER_INFO``,
``QUERY``, ``CLOSE``, ``EXIT``.

Unlike the ingress ``go_sidecar`` (whose binary the CI pipeline builds in
a dedicated step), the egress binary is built on demand by
:func:`build_egress_sidecar` from a session-scoped fixture — mirroring the
c-questdb-client sidecar's ``build_qwp_sidecar`` — so the egress feature is
self-contained in this repo and needs no enterprise-pipeline change. Go is
already on ``PATH`` wherever these tests run.
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Thread
from typing import Optional

# Reuse the ingress sidecar's typed error and stream helpers so callers can
# catch one error type regardless of which sidecar threw.
from go_sidecar import GoSidecarError, _drain, _readline

LOG = logging.getLogger(__name__)

EGRESS_DIR = Path(__file__).resolve().parent / "egress_sidecar"
EGRESS_BIN = EGRESS_DIR / "go-e2e-egress-sidecar"


def build_egress_sidecar() -> Path:
    """Compile the egress sidecar binary (``go build`` is a fast no-op when
    the target is already current). Returns the binary path."""
    LOG.info("building Go egress sidecar in %s", EGRESS_DIR)
    subprocess.run(
        ["go", "build", "-o", str(EGRESS_BIN), "."],
        cwd=str(EGRESS_DIR),
        check=True,
    )
    return EGRESS_BIN


@dataclass
class EgressServerInfo:
    """Snapshot of the bound endpoint's SERVER_INFO as the egress client
    cached it. ``zone`` is ``None`` when the server didn't advertise one
    (the sidecar serialises that as ``<unset>``); ``role`` is the raw
    replication-role byte (0=STANDALONE, 1=PRIMARY, 2=REPLICA,
    3=PRIMARY_CATCHUP), or ``-1`` when no SERVER_INFO was cached."""

    zone: Optional[str]
    role: int


@dataclass
class EgressGoSidecar:
    log_dir: Path
    binary_path: Path
    name: str = "go-egress-sidecar"

    process: Optional[subprocess.Popen] = field(default=None, init=False, repr=False)
    _stderr_thread: Optional[Thread] = field(default=None, init=False, repr=False)

    def start(self, *, ready_timeout: float = 30.0) -> None:
        if self.process is not None:
            raise RuntimeError(f"egress sidecar {self.name!r} already started")
        if not self.binary_path.exists():
            raise FileNotFoundError(
                f"egress sidecar binary not found at {self.binary_path}; "
                f"run 'go build -o go-e2e-egress-sidecar .' in {EGRESS_DIR} first"
            )

        self.log_dir.mkdir(parents=True, exist_ok=True)
        stderr_log = open(self.log_dir / f"{self.name}.stderr.log", "w", encoding="utf-8")

        LOG.info("starting Go egress sidecar %s", self.name)
        self.process = subprocess.Popen(
            [str(self.binary_path)],
            env=os.environ.copy(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )

        self._stderr_thread = _drain(self.process.stderr, stderr_log, f"{self.name}-stderr")

        deadline = time.monotonic() + ready_timeout
        while True:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"egress sidecar {self.name!r} exited prematurely "
                    f"(code {self.process.returncode}); see "
                    f"{self.log_dir / f'{self.name}.stderr.log'}"
                )
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"egress sidecar {self.name!r} did not READY within {ready_timeout}s"
                )
            line = _readline(self.process.stdout, 0.5)
            if line is None:
                continue
            line = line.strip()
            if line == "READY":
                break
            LOG.warning("egress sidecar %s pre-READY: %r", self.name, line)

    def stop(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            try:
                self._send("EXIT")
            except (BrokenPipeError, OSError):
                pass
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                LOG.warning("egress sidecar %s did not exit after EXIT, escalating to SIGKILL",
                            self.name)
                self.process.kill()
                self.process.wait(timeout=5)
        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=5)
        for pipe in (self.process.stdin, self.process.stdout, self.process.stderr):
            if pipe is not None:
                pipe.close()

    # ---- protocol verbs ----

    def connect(self, connect_string: str) -> None:
        self._send(f"CONNECT {connect_string}")
        self._expect_ok()

    def show_zone(self) -> str:
        """SHOW PARAMETERS for replication.zone over the bound connection.
        Returns the value column; an unset zone surfaces as ``<unset>``."""
        self._send("SHOW_ZONE")
        reply = self._expect_ok()
        # Zone names may contain spaces (rare but valid) — consume greedily.
        return " ".join(reply) if reply else ""

    def query(self, sql: str) -> tuple[int, float]:
        """Execute a query via the egress client; returns (row_count,
        latency_ms). Raises :class:`GoSidecarError` on server error."""
        self._send(f"QUERY {sql}")
        reply = self._expect_ok()
        return (int(reply[0]), float(reply[1])) if len(reply) >= 2 else (0, 0.0)

    def server_info(self) -> EgressServerInfo:
        """Cached SERVER_INFO snapshot from the most recent bind (no wire
        round-trip, so it does not itself drive a reconnect)."""
        self._send("SERVER_INFO")
        reply = self._expect_ok()
        kv = dict(p.split("=", 1) for p in reply if "=" in p)
        raw_zone = kv.get("zone", "<unset>")
        zone = None if raw_zone == "<unset>" else raw_zone
        return EgressServerInfo(zone=zone, role=int(kv.get("role", "-1")))

    def close(self) -> None:
        self._send("CLOSE")
        self._expect_ok()

    # ---- internals ----

    def _send(self, line: str) -> None:
        if self.process is None or self.process.poll() is not None:
            raise RuntimeError(f"egress sidecar {self.name!r} is not running")
        assert self.process.stdin is not None
        self.process.stdin.write((line + "\n").encode("utf-8"))
        self.process.stdin.flush()

    def _expect_ok(self) -> list[str]:
        if self.process is None:
            raise RuntimeError("egress sidecar not running")
        # 60s is comfortably above the egress failover_max_duration budget
        # plus the last walk; a longer reply means we genuinely hung.
        line = _readline(self.process.stdout, 60.0)
        if line is None:
            raise RuntimeError("egress sidecar produced no reply (timeout or EOF)")
        line = line.strip()
        if line == "OK" or line.startswith("OK "):
            return line.split()[1:]
        if line.startswith("ERR"):
            raise GoSidecarError(line[len("ERR "):])
        raise RuntimeError(f"unexpected egress sidecar reply: {line!r}")
