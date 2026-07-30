"""
Zone-based failover tests for the Go QWP egress client (QwpQueryClient).

Ports the Enterprise reference ``questdb-ent/e2e/tests/test_zone_failover.py``
to the Go egress sidecar. These exercise the zone half of the WalkTracker
priority lattice. The oracle for "which server am I bound to" is the egress
client itself: ``SHOW PARAMETERS WHERE property_path = 'replication.zone'``
over the bound connection returns the zone the bound server was started
with — proving the bind end-to-end.

``zone=`` is a *preference*, not a hard filter: state outranks zone, and a
host's zone tier is Unknown until observed, so the FIRST connect against an
all-Unknown tracker is decided by ``addr=`` order.

NOT xfail: the Go query client implements zone-aware failover, so these
exercise real behaviour and should pass against a live cluster.
"""

from __future__ import annotations

import logging
import socket
import time

import pytest

from lib.server import wait_port_free

from go_egress_sidecar import EgressGoSidecar

LOG = logging.getLogger(__name__)

pytestmark = [pytest.mark.go_client]

ZONE_A = "zone-A"
ZONE_B = "zone-B"


def _pick_unused_port() -> int:
    """Bind ephemeral on 127.0.0.1, immediately release, return the port —
    a syntactically valid ``addr=`` entry that resolves but refuses
    connections, exercising the WalkTracker's skip-transport-error path
    without starting a doomed JVM."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _connect_string(addrs: list[tuple[str, int]], *, zone: str | None,
                    target: str = "any",
                    failover_max_duration_ms: int = 15_000,
                    auth_timeout_ms: int = 5_000) -> str:
    addr_str = ",".join(f"{h}:{p}" for h, p in addrs)
    parts = [
        f"ws::addr={addr_str}",
        "username=admin",
        "password=quest",
        f"target={target}",
        f"failover_max_duration_ms={failover_max_duration_ms}",
        f"auth_timeout_ms={auth_timeout_ms}",
    ]
    if zone is not None:
        parts.append(f"zone={zone}")
    return ";".join(parts) + ";"


def _wait_for_zone(go_egress_sidecar: EgressGoSidecar, expected_zone: str,
                   *, timeout_s: float = 15.0) -> None:
    """Poll ``SHOW_ZONE`` until it matches ``expected_zone``. A mid-stream
    failover takes a few hundred ms (send fails, WalkTracker reconnects,
    re-issues the query); polling deflakes the transient values while the
    terminal state is asserted."""
    deadline = time.monotonic() + timeout_s
    observed = ""
    while time.monotonic() < deadline:
        observed = go_egress_sidecar.show_zone()
        if observed == expected_zone:
            return
        time.sleep(0.1)
    pytest.fail(
        f"SHOW_ZONE never settled on {expected_zone!r} within {timeout_s}s "
        f"(last observed: {observed!r}); the egress client did not bind to "
        f"the expected zone."
    )


def test_startup_reports_bound_zone(server_factory,
                                    go_egress_sidecar: EgressGoSidecar) -> None:
    """Happy path: with ``zone=A`` configured and a single zone-A server in
    ``addr=``, the client binds and SHOW PARAMETERS over that connection
    reports ``replication.zone=zone-A``."""
    p1 = server_factory("p1", zone=ZONE_A)
    p1_ports = p1.start()

    cs = _connect_string([("127.0.0.1", p1_ports.http)], zone=ZONE_A)
    go_egress_sidecar.connect(cs)
    assert go_egress_sidecar.show_zone() == ZONE_A


def test_falls_back_to_other_zone_when_no_same_zone_available(
        server_factory, go_egress_sidecar: EgressGoSidecar) -> None:
    """``zone=`` is a preference, not a hard constraint: when no same-zone
    endpoint is reachable the client must still bind a wrong-zone host
    rather than refusing the connect."""
    fake_a_port_1 = _pick_unused_port()
    fake_a_port_2 = _pick_unused_port()

    r2 = server_factory("r2", role="replica", zone=ZONE_B)
    r2_ports = r2.start()

    cs = _connect_string(
        [
            ("127.0.0.1", fake_a_port_1),
            ("127.0.0.1", fake_a_port_2),
            ("127.0.0.1", r2_ports.http),
        ],
        zone=ZONE_A,
    )
    go_egress_sidecar.connect(cs)
    assert go_egress_sidecar.show_zone() == ZONE_B


def test_failover_reports_new_zone(server_factory,
                                   go_egress_sidecar: EgressGoSidecar) -> None:
    """When the bound endpoint dies and the reconnect loop moves to a
    different-zone host, ``SHOW_ZONE`` must surface the *new* server's
    zone, not a cached value from the previous bind."""
    p1 = server_factory("p1", zone=ZONE_A)
    p1_ports = p1.start()
    r2 = server_factory("r2", role="replica", zone=ZONE_B)
    r2_ports = r2.start()

    cs = _connect_string(
        [("127.0.0.1", p1_ports.http), ("127.0.0.1", r2_ports.http)],
        zone=ZONE_A,
    )
    go_egress_sidecar.connect(cs)
    assert go_egress_sidecar.show_zone() == ZONE_A, \
        "addr=A,B with zone=A should bind A first (addr-order)"

    p1.kill_9()
    wait_port_free(p1_ports.http)

    # The next SHOW_ZONE triggers a fresh Execute; the dead socket surfaces
    # as a transport error, WalkTracker walks to r2 (the only remaining
    # endpoint). Poll because the first attempt may race the OS-level RST.
    _wait_for_zone(go_egress_sidecar, ZONE_B)


def test_zone_preference_breaks_state_ties(server_factory,
                                           go_egress_sidecar: EgressGoSidecar) -> None:
    """The scenario that actually exercises same-zone *preference*, not
    just addr-order. Drives the tracker until two hosts share priority
    ``TransportError`` and their zone tiers are the tie-breaker:

    1. addr=h_B(zone-B), h_A(zone-A), client zone=A. Initial bind -> h_B
       (addr-first, all Unknown). Tracker learns h_B tier=Other.
    2. Kill h_B -> reconnect binds h_A (Unknown beats h_B's TransportError).
       Tracker learns h_A tier=Same.
    3. Restart h_B on its port (still TransportError on the tracker).
    4. Kill h_A, restart it. Now both hosts are TransportError; only zone
       tier differs (h_A=Same, h_B=Other). With zone preference honoured,
       the next walk picks h_A (zone-A) over addr-first h_B.
    """
    h_a = server_factory("h_a", role="replica", zone=ZONE_A)
    h_a_ports = h_a.start()
    h_b = server_factory("h_b", role="replica", zone=ZONE_B)
    h_b_ports = h_b.start()

    cs = _connect_string(
        [("127.0.0.1", h_b_ports.http), ("127.0.0.1", h_a_ports.http)],
        zone=ZONE_A,
    )
    go_egress_sidecar.connect(cs)
    assert go_egress_sidecar.show_zone() == ZONE_B, \
        "addr-order first wins on initial connect; zone= does not yet bias"

    # Kill h_B -> failover reaches h_A (the only untried Unknown host left).
    h_b.kill_9()
    wait_port_free(h_b_ports.http)
    _wait_for_zone(go_egress_sidecar, ZONE_A)

    # Bring h_B back on its original port (tracker still remembers it as
    # TransportError + Other until the next walk).
    h_b_restart = server_factory("h_b_restart", role="replica", zone=ZONE_B,
                                 db_root_name="h_b_restart")
    h_b_restart.start(http_port=h_b_ports.http, pg_port=h_b_ports.pg)

    # Kill h_A and restart it. The mid-stream demote moves h_A to
    # TransportError + Same; after restart it is alive again but the tracker
    # still remembers the demotion. Both hosts TransportError; zone breaks
    # the tie.
    h_a.kill_9()
    wait_port_free(h_a_ports.http)
    h_a_restart = server_factory("h_a_restart", role="replica", zone=ZONE_A,
                                 db_root_name="h_a_restart")
    h_a_restart.start(http_port=h_a_ports.http, pg_port=h_a_ports.pg)

    # With zone preference honoured, h_A (Same) wins over h_B (Other) even
    # though h_B is at addr index 0.
    _wait_for_zone(go_egress_sidecar, ZONE_A)
