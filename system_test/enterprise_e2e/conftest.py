"""
Pytest root config for go-questdb-client Enterprise e2e tests.

Registers the Enterprise shared_fixtures plugin (server_factory,
scenario_dir, obj_store, etc.) and adds a ``go_sidecar`` fixture
that launches the pre-built Go sidecar binary.

The ``GoSidecar`` class lives in ``go_sidecar.py`` next to this file so
tests that bring up a second sender (the sender-crash recovery and
transaction-atomicity suites) can import it directly.

The QUESTDB_ENTERPRISE_E2E_DIR environment variable must point at
the ``questdb-ent/e2e`` directory in the Enterprise checkout so the
plugin module is importable.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Iterator

import pytest

_ent_e2e = os.environ.get("QUESTDB_ENTERPRISE_E2E_DIR")
if _ent_e2e:
    sys.path.insert(0, _ent_e2e)

pytest_plugins = ("lib.shared_fixtures",)

# Imported after the sys.path insert above so a test can also do
# ``from go_sidecar import GoSidecar`` (go_sidecar.py sits next to this
# conftest, which pytest puts on sys.path).
from go_sidecar import GoSidecar  # noqa: E402
from go_egress_sidecar import EgressGoSidecar, build_egress_sidecar  # noqa: E402

LOG = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def go_sidecar(log_dir: Path) -> Iterator[GoSidecar]:
    s = GoSidecar(log_dir=log_dir, name="go-sidecar")
    s.start()
    try:
        yield s
    finally:
        s.stop()


@pytest.fixture(scope="session")
def go_egress_sidecar_binary() -> Path:
    """One ``go build`` per session (fast no-op when already current)."""
    return build_egress_sidecar()


@pytest.fixture(scope="function")
def go_egress_sidecar(
    go_egress_sidecar_binary: Path, log_dir: Path
) -> Iterator[EgressGoSidecar]:
    s = EgressGoSidecar(log_dir=log_dir, binary_path=go_egress_sidecar_binary,
                        name="go-egress-sidecar")
    s.start()
    try:
        yield s
    finally:
        s.stop()
