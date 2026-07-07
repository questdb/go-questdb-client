"""
Target-role failover tests for the Go QWP egress client (QwpQueryClient).

Ports the Enterprise reference ``questdb-ent/e2e/tests/test_target_filter.py``
to the Go egress sidecar. The client reads SERVER_INFO on each attempted
bind and applies the ``target=`` role filter locally:

* ``target=primary``  — accepts PRIMARY and STANDALONE, rejects REPLICA.
* ``target=replica``  — accepts REPLICA, rejects PRIMARY / STANDALONE.
* ``target=any``      — accepts everything (the default).

On a connect that exhausts every endpoint with no match,
``QwpQueryClientFromConf`` returns a ``*QwpRoleMismatchError`` (message
``no endpoint matches target=<x>``) — which the sidecar surfaces as
``ERR ...`` and the wrapper raises as :class:`GoSidecarError`.

Unlike the durable-ack / transaction ports, these are NOT xfail: the Go
query client implements target/zone failover, so they exercise real
behaviour and should pass against a live cluster. A failure is a genuine
signal (a Go egress gap or a port bug), which is exactly what we want.
"""

from __future__ import annotations

import logging
import time

import pytest

from lib.obj_store import ObjStore
from lib.server import wait_port_free

from go_egress_sidecar import EgressGoSidecar
from go_sidecar import GoSidecarError

LOG = logging.getLogger(__name__)

pytestmark = [pytest.mark.go_client]

# Wire role bytes from QwpServerInfo.Role (see qwp_constants.go).
ROLE_STANDALONE = 0
ROLE_PRIMARY = 1
ROLE_REPLICA = 2


def _connect_string(addrs: list[tuple[str, int]], *,
                    target: str,
                    zone: str | None = None,
                    failover_max_duration_ms: int = 15_000,
                    auth_timeout_ms: int = 5_000) -> str:
    """Egress connect string with explicit ``target=``. Egress
    (``QwpQueryClient``) uses ``username``/``password`` for HTTP Basic
    auth, distinct from the ingress ``Sender`` connect string."""
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


def test_target_replica_skips_primary_at_startup(server_factory,
                                                 go_egress_sidecar: EgressGoSidecar) -> None:
    """``target=replica`` MUST walk past a PRIMARY listed earlier in the
    address list and bind a REPLICA further down."""
    p1 = server_factory("p1", role="primary")
    p1_ports = p1.start()
    r1 = server_factory("r1", role="replica")
    r1_ports = r1.start()

    cs = _connect_string(
        [("127.0.0.1", p1_ports.http), ("127.0.0.1", r1_ports.http)],
        target="replica",
    )
    go_egress_sidecar.connect(cs)

    info = go_egress_sidecar.server_info()
    assert info.role == ROLE_REPLICA, \
        f"target=replica must bind a replica (role={ROLE_REPLICA}); got role={info.role}"


def test_target_replica_fails_when_only_primary_available(server_factory,
                                                          go_egress_sidecar: EgressGoSidecar) -> None:
    """When every endpoint is a primary and ``target=replica`` is set,
    the connect MUST fail loudly with a role-mismatch error rather than
    silently binding the primary."""
    p1 = server_factory("p1", role="primary")
    p1_ports = p1.start()

    cs = _connect_string(
        [("127.0.0.1", p1_ports.http)],
        target="replica",
    )
    with pytest.raises(GoSidecarError, match="target=replica"):
        go_egress_sidecar.connect(cs)


def test_target_replica_failover_skips_primary(server_factory,
                                               go_egress_sidecar: EgressGoSidecar) -> None:
    """During a mid-stream failover with ``target=replica``, the
    per-Execute reconnect loop MUST keep filtering out the primary.
    Topology: addr=r1, p1, r2, target=replica. Initial bind r1; kill r1;
    reconnect MUST walk past p1 and bind r2."""
    p1 = server_factory("p1", role="primary")
    p1_ports = p1.start()
    r1 = server_factory("r1", role="replica")
    r1_ports = r1.start()
    r2 = server_factory("r2", role="replica")
    r2_ports = r2.start()

    cs = _connect_string(
        [
            ("127.0.0.1", r1_ports.http),
            ("127.0.0.1", p1_ports.http),
            ("127.0.0.1", r2_ports.http),
        ],
        target="replica",
    )
    go_egress_sidecar.connect(cs)
    info = go_egress_sidecar.server_info()
    assert info.role == ROLE_REPLICA, "initial bind must be a replica"

    # Kill r1 and force a reconnect via the next Execute (SHOW_ZONE).
    r1.kill_9()
    wait_port_free(r1_ports.http)

    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        try:
            info = go_egress_sidecar.server_info()
        except GoSidecarError:
            info = None
        if info is not None and info.role == ROLE_REPLICA:
            try:
                # SHOW_ZONE drives a fresh Execute; success proves the bind
                # is live (both replicas left zone unset -> <unset> token).
                zone_after = go_egress_sidecar.show_zone()
                assert zone_after  # non-empty reply
                break
            except GoSidecarError:
                pass
        time.sleep(0.1)
    else:
        pytest.fail("egress client never recovered to a replica within 15s")

    info = go_egress_sidecar.server_info()
    assert info.role == ROLE_REPLICA, "post-failover bind must still be a replica"


def test_target_primary_failover_to_promoted_replica(server_factory,
                                                     go_egress_sidecar: EgressGoSidecar,
                                                     obj_store: ObjStore) -> None:
    """``target=primary`` is "follow the master across topology changes".
    Bind p1; kill both p1+r1 and wipe the object store; start a fresh
    primary on r1's address; the next Execute must re-bind to the promoted
    primary."""
    p1 = server_factory("p1", role="primary")
    p1_ports = p1.start()
    r1 = server_factory("r1", role="replica")
    r1_ports = r1.start()

    cs = _connect_string(
        [("127.0.0.1", p1_ports.http), ("127.0.0.1", r1_ports.http)],
        target="primary",
    )
    go_egress_sidecar.connect(cs)
    info = go_egress_sidecar.server_info()
    assert info.role == ROLE_PRIMARY, \
        f"initial bind must be PRIMARY (role={ROLE_PRIMARY}); got role={info.role}"

    # Kill both so the new primary can claim r1's port, and wipe the object
    # store so the new primary doesn't refuse on DataID mismatch.
    p1.kill_9()
    r1.kill_9()
    wait_port_free(p1_ports.http)
    wait_port_free(r1_ports.http)
    obj_store.wipe()

    promoted = server_factory("r1_promoted", role="primary",
                              db_root_name="r1-promoted-fresh")
    promoted.start(http_port=r1_ports.http, pg_port=r1_ports.pg)

    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        try:
            # SHOW_ZONE forces an Execute; the returned zone isn't
            # load-bearing (we assert on role via server_info next).
            go_egress_sidecar.show_zone()
            info = go_egress_sidecar.server_info()
            if info.role == ROLE_PRIMARY:
                break
        except GoSidecarError:
            pass
        time.sleep(0.2)
    else:
        pytest.fail(
            "egress client never re-bound to the promoted primary within 20s; "
            "target=primary did not follow the master."
        )

    info = go_egress_sidecar.server_info()
    assert info.role == ROLE_PRIMARY, \
        f"post-failover bind must be the new PRIMARY (role={ROLE_PRIMARY}); got role={info.role}"
