"""Strong symbol-dictionary oracle for the Go Enterprise e2e tests."""

from __future__ import annotations

import time
from typing import Optional

import psycopg


def wait_for_symbol_mapping(
    *,
    port: int,
    table: str,
    expected_count: int,
    symbol_cardinality: int,
    timeout_s: float = 90.0,
    host: str = "127.0.0.1",
    user: str = "admin",
    password: str = "quest",
) -> list[tuple[int, str | None]]:
    """Wait until every row has the symbol implied by its ``v`` value.

    The Go sidecar emits ``tag=test_<v % symbol_cardinality>`` in bounded
    dictionary mode. Comparing every row, rather than counts grouped by tag,
    catches NULL values and a dictionary shifted onto neighbouring IDs.
    """
    if symbol_cardinality <= 0:
        raise ValueError("symbol_cardinality must be positive")

    expected = [
        (value, f"test_{value % symbol_cardinality}")
        for value in range(expected_count)
    ]
    deadline = time.monotonic() + timeout_s
    observed: list[tuple[int, str | None]] = []
    last_exc: Optional[Exception] = None

    while time.monotonic() < deadline:
        try:
            with psycopg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname="qdb",
                connect_timeout=5,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT "v", "tag" FROM "{table}" ORDER BY "v";')
                    observed = [
                        (int(value), None if tag is None else str(tag))
                        for value, tag in cur.fetchall()
                    ]
            last_exc = None
            if observed == expected:
                return observed
        except (psycopg.OperationalError, psycopg.errors.ConnectionTimeout) as exc:
            last_exc = exc
        except psycopg.DatabaseError as exc:
            message = str(exc).lower()
            if "table does not exist" not in message and "does not exist" not in message:
                raise
            last_exc = exc
        time.sleep(0.25)

    if len(observed) != expected_count:
        raise AssertionError(
            f"{table}: expected {expected_count} rows for the symbol oracle, "
            f"observed {len(observed)} within {timeout_s}s; last error={last_exc!r}"
        )

    mismatches = [
        (index, expected_row, observed_row)
        for index, (expected_row, observed_row) in enumerate(zip(expected, observed))
        if observed_row != expected_row
    ]
    raise AssertionError(
        f"{table}: symbol dictionary mapping diverged; "
        f"first mismatches (row, expected, observed): {mismatches[:10]}"
    )
