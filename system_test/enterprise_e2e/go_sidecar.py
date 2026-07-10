"""
Go sidecar driver for the Enterprise e2e suite.

The Go client under test is exercised out-of-process: ``sidecar/main.go``
is compiled into the ``go-e2e-sidecar`` binary (see the CI pipeline and
the README) and driven over a line-based stdin/stdout protocol that
mirrors the Enterprise Java sidecar (``lib/sidecar.py``): ``CONNECT``,
``SEND``, ``FLUSH``, ``FLUSH_DEFER``, ``AWAIT_ACKED``, ``STATS``,
``CLOSE``, ``EXIT``.

This lives in its own module (rather than inline in ``conftest.py``) so
tests that need a *second* sender — the sender-crash recovery and
transaction-atomicity suites kill the sidecar and bring up a fresh one on
the same ``sf_dir``/``sender_id`` — can ``from go_sidecar import
GoSidecar`` and instantiate it directly, exactly as the Enterprise tests
instantiate a second ``Sidecar``.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Thread
from typing import IO, Optional

LOG = logging.getLogger(__name__)

SIDECAR_DIR = Path(__file__).resolve().parent / "sidecar"
SIDECAR_BIN = SIDECAR_DIR / "go-e2e-sidecar"


class GoSidecarError(RuntimeError):
    """Raised when the sidecar replies ``ERR <msg>`` to a command.

    The failover / recovery / transaction e2e tests pin their ``xfail``
    markers to this type (``raises=GoSidecarError``) so a red test proves
    the *client feature gap* (e.g. the sidecar rejecting
    ``request_durable_ack=on`` at ``CONNECT``) and not an unrelated
    harness bug — an unexpected exception type still surfaces as a real
    failure.
    """


@dataclass
class GoSidecarStats:
    acked: int
    sent: int
    acks: int
    reconn_attempts: int
    reconn_succ: int
    server_errors: int


@dataclass
class GoSidecar:
    log_dir: Path
    name: str = "go-sidecar"

    process: Optional[subprocess.Popen] = field(default=None, init=False, repr=False)
    _stderr_thread: Optional[Thread] = field(default=None, init=False, repr=False)

    def start(self, *, ready_timeout: float = 30.0) -> None:
        if self.process is not None:
            raise RuntimeError(f"sidecar {self.name!r} already started")

        binary = SIDECAR_BIN
        if not binary.exists():
            raise FileNotFoundError(
                f"sidecar binary not found at {binary}; "
                f"run 'go build -o go-e2e-sidecar .' in {SIDECAR_DIR} first"
            )

        cmd = [str(binary)]
        self.log_dir.mkdir(parents=True, exist_ok=True)
        stderr_log = open(self.log_dir / f"{self.name}.stderr.log", "w", encoding="utf-8")

        LOG.info("starting Go sidecar %s", self.name)
        self.process = subprocess.Popen(
            cmd,
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
                    f"sidecar {self.name!r} exited prematurely "
                    f"(code {self.process.returncode}); see "
                    f"{self.log_dir / f'{self.name}.stderr.log'}"
                )
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"sidecar {self.name!r} did not READY within {ready_timeout}s"
                )
            line = _readline(self.process.stdout, 0.5)
            if line is None:
                continue
            line = line.strip()
            if line == "READY":
                break
            LOG.warning("sidecar %s pre-READY: %r", self.name, line)

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
                LOG.warning("sidecar %s did not exit after EXIT, escalating to SIGKILL", self.name)
                self.process.kill()
                self.process.wait(timeout=5)
        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=5)
        for pipe in (self.process.stdin, self.process.stdout, self.process.stderr):
            if pipe is not None:
                pipe.close()

    def kill_9(self) -> None:
        if self.process is None or self.process.poll() is not None:
            return
        LOG.info("kill -9 sidecar %s pid=%d", self.name, self.process.pid)
        try:
            os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            LOG.error("sidecar %s did not exit after SIGKILL within 10s", self.name)

    # ---- protocol verbs ----

    def connect(self, connect_string: str) -> None:
        self._send(f"CONNECT {connect_string}")
        self._expect_ok()

    def send(self, table: str, count: int, start_index: int = 0) -> None:
        self._send(f"SEND {table} {count} {start_index}")
        self._expect_ok()

    def flush(self) -> int:
        self._send("FLUSH")
        reply = self._expect_ok()
        return int(reply[0]) if reply else -1

    def flush_defer(self) -> int:
        """Publish a deferred-commit (``transaction=on``) batch.

        Mirrors the Java sidecar's ``FLUSH_DEFER``. The Go client does not
        implement deferred-commit transactions yet, so the sidecar rejects
        this verb — the transaction-atomicity tests xfail on the resulting
        :class:`GoSidecarError` (in practice they fail earlier still, at
        ``CONNECT``, because ``transaction=on`` is itself rejected)."""
        self._send("FLUSH_DEFER")
        reply = self._expect_ok()
        return int(reply[0]) if reply else -1

    def await_acked(self, fsn: int, timeout_ms: int) -> bool:
        self._send(f"AWAIT_ACKED {fsn} {timeout_ms}")
        reply = self._expect_ok()
        return reply[0] == "true" if reply else False

    def stats(self) -> GoSidecarStats:
        self._send("STATS")
        reply = self._expect_ok()
        kv = dict(p.split("=", 1) for p in reply if "=" in p)
        return GoSidecarStats(
            acked=int(kv.get("acked", -1)),
            sent=int(kv.get("sent", 0)),
            acks=int(kv.get("acks", 0)),
            reconn_attempts=int(kv.get("reconnAttempts", 0)),
            reconn_succ=int(kv.get("reconnSucc", 0)),
            server_errors=int(kv.get("serverErrors", 0)),
        )

    def close(self) -> None:
        self._send("CLOSE")
        self._expect_ok()

    # ---- internals ----

    def _send(self, line: str) -> None:
        if self.process is None or self.process.poll() is not None:
            raise RuntimeError(f"sidecar {self.name!r} is not running")
        assert self.process.stdin is not None
        self.process.stdin.write((line + "\n").encode("utf-8"))
        self.process.stdin.flush()

    def _expect_ok(self) -> list[str]:
        if self.process is None:
            raise RuntimeError("sidecar not running")
        line = _readline(self.process.stdout, 60.0)
        if line is None:
            raise RuntimeError("sidecar produced no reply (timeout or EOF)")
        line = line.strip()
        if line == "OK" or line.startswith("OK "):
            return line.split()[1:]
        if line.startswith("ERR"):
            raise GoSidecarError(line[len("ERR "):])
        raise RuntimeError(f"unexpected sidecar reply: {line!r}")


def _readline(stream: IO[bytes], timeout: float) -> Optional[str]:
    import select
    readable, _, _ = select.select([stream], [], [], timeout)
    if not readable:
        return None
    line = stream.readline()
    if not line:
        return None
    return line.decode("utf-8", errors="replace")


def _drain(stream: IO[bytes], sink, label: str) -> Thread:
    def _run():
        for raw in stream:
            sink.write(raw.decode("utf-8", errors="replace"))
        sink.close()

    t = Thread(target=_run, name=label, daemon=True)
    t.start()
    return t
