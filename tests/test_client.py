import csv
import time
from pathlib import Path
from typing import Any

import pytest
from trino.exceptions import TrinoQueryError

from trino_mcp import Config
from trino_mcp.client import (
    QueryRejected,
    QueryTimeout,
    qualify,
    quote_ident,
)
from trino_mcp.export import OutputFormat

from .conftest import FakeCursor, client_over


def rows(count: int) -> list[list[int]]:
    return [[n] for n in range(count)]


class SlowCursor(FakeCursor):
    """A cursor whose execute outlives the deadline."""

    def execute(self, query: str) -> None:
        time.sleep(3)
        super().execute(query)


class NoResultCursor(FakeCursor):
    """A statement that ran but produced no result set."""

    def execute(self, query: str) -> None:
        super().execute(query)
        self.description = None


class DyingCursor(FakeCursor):
    """Drops the connection partway through the drain, once the file is open."""

    def __init__(self, columns: list[str], data: list[list[Any]]) -> None:
        super().__init__(columns, data)
        self.fetches = 0

    def fetchmany(self, size: int) -> list[list[Any]]:
        self.fetches += 1
        if self.fetches > 1:
            raise OSError("connection reset")
        return super().fetchmany(size)


class StallingCursor(FakeCursor):
    """Yields nothing and outlives the deadline, with the file already open."""

    def fetchmany(self, size: int) -> list[list[Any]]:
        time.sleep(3)
        return super().fetchmany(size)


class LateCursor(FakeCursor):
    """Outlives the deadline *before* the writer opens anything.

    Sleeps long enough to miss the deadline but short enough that the test does
    not spend a second waiting for the abandoned thread to reach its open().
    """

    def execute(self, query: str) -> None:
        time.sleep(0.8)
        super().execute(query)


class FailingCursor(FakeCursor):
    """Raises on the first execute, then behaves."""

    def __init__(
        self, columns: list[str], data: list[list[Any]], error: Exception
    ) -> None:
        super().__init__(columns, data)
        self.error = error
        self.attempts = 0

    def execute(self, query: str) -> None:
        self.attempts += 1
        if self.attempts == 1:
            raise self.error
        super().execute(query)


@pytest.mark.parametrize(
    ("available", "kept", "truncated"),
    [
        # The boundary is the point: a cap of N with exactly N rows is a
        # complete answer, which is why `_collect` fetches N + 1. A case below
        # the cap would exercise the same branch as the one at it.
        (10, 10, False),
        (25, 10, True),
    ],
)
def test_truncation_is_reported_only_when_rows_were_dropped(
    config: Config, available: int, kept: int, truncated: bool
) -> None:
    cursor = FakeCursor(["n"], rows(available))
    result = client_over(config.model_copy(update={"max_rows": 10}), cursor).execute(
        "SELECT n"
    )

    assert result.columns == ["n"]
    assert result.rows == rows(kept)
    assert result.truncated is truncated
    # Streaming is stopped rather than left running for pages nobody reads.
    assert cursor.canceled is truncated


def test_the_trailing_semicolon_is_stripped_before_execution(config: Config) -> None:
    cursor = FakeCursor(["n"], rows(1))
    client_over(config, cursor).execute("SELECT 1;")

    assert cursor.executed == ["SELECT 1"]


def test_writing_csv_ignores_the_cap_and_reports_the_count(
    config: Config, tmp_path: Path
) -> None:
    """A file export exists to move the *whole* result set, so it is not capped."""
    cursor = FakeCursor(["n"], rows(2500))
    # Misnamed on purpose: the writer follows fmt, never the suffix. Sniffing
    # the suffix is what silently turned an out.parquet export into JSON.
    destination = tmp_path / "big.json"

    written = client_over(
        config.model_copy(update={"max_rows": 10}), cursor
    ).execute_to_file("SELECT n", destination, "csv")

    assert written == 2500
    with destination.open(newline="") as handle:
        contents = list(csv.reader(handle))
    assert contents[0] == ["n"]
    assert len(contents) == 2501
    assert contents[-1] == ["2499"]


def test_an_export_survives_a_cursor_that_reports_no_columns(
    config: Config, tmp_path: Path
) -> None:
    """trino reports no columns for a query canceled before they arrived.

    Its annotation claims that cannot happen, so the guard reads as dead code and
    is easy to drop by accident — see `_description`. What each format then puts
    in the file is `test_export.py`'s business; this pins the wiring.
    """
    destination = tmp_path / "out.csv"

    written = client_over(config, NoResultCursor([], [])).execute_to_file(
        "SELECT n", destination, "csv"
    )

    assert written == 0
    assert destination.read_text() == ""


@pytest.mark.parametrize(
    ("failure", "fmt"),
    [
        ("drops", "json"),
        ("stalls", "json"),
        # Parquet stages its rows in a scratch directory, and that scratch is
        # bigger than the file it would have produced. It is cleaned up here
        # rather than by the writer, so this is the case that covers it.
        ("stalls", "parquet"),
    ],
)
def test_a_failed_export_leaves_nothing_behind(
    config: Config, tmp_path: Path, failure: str, fmt: OutputFormat
) -> None:
    """Half a result set under the reported name is worse than no file at all.

    The name is generated by the server and never appears in the error, so an
    orphan is undiscoverable — and a truncated JSON export does not even parse.
    Rows land in a temp file that is renamed only once the drain has finished,
    beside a scratch directory that goes the same way.
    """
    destination = tmp_path / f"out.{fmt}"
    cursor: FakeCursor
    if failure == "drops":
        cursor, expected, timeout = DyingCursor(["n"], rows(2500)), OSError, 5.0
    else:
        cursor, expected, timeout = StallingCursor(["n"], rows(1)), QueryTimeout, 0.01

    client = client_over(
        config.model_copy(update={"query_timeout_minutes": timeout}), cursor
    )

    with pytest.raises(expected):
        client.execute_to_file("SELECT n", destination, fmt)

    assert not destination.exists()
    # The temp file and the scratch are gone too, so a failure leaves the
    # directory exactly as it was.
    assert list(tmp_path.iterdir()) == []


def test_a_late_worker_cannot_resurrect_the_reported_path(
    config: Config, tmp_path: Path
) -> None:
    """The deadline can fire before the writer opens anything, so cleanup finds
    nothing to remove and the abandoned thread creates its file afterwards.

    That file must carry the temp name. Draining straight to the caller's path
    would let an export the caller was told had failed reappear as a result.
    """
    destination = tmp_path / "out.csv"
    client = client_over(
        config.model_copy(update={"query_timeout_minutes": 0.01}),
        LateCursor(["n"], rows(1)),
    )

    with pytest.raises(QueryTimeout):
        client.execute_to_file("SELECT n", destination, "csv")

    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and not any(tmp_path.iterdir()):
        time.sleep(0.02)

    assert not destination.exists()
    assert [p.name for p in tmp_path.iterdir()] == [f".{destination.name}.partial"]


def test_identifiers_cannot_escape_their_quotes(config: Config) -> None:
    """Catalog and schema names arrive as tool arguments, so they are untrusted.

    Quoting the helpers correctly is only half of it; the metadata methods have
    to actually route through them, which is where interpolating straight into
    an f-string would slip past.
    """
    assert quote_ident('ev"il') == '"ev""il"'
    assert qualify("cat", "sch", "tbl") == '"cat"."sch"."tbl"'
    assert qualify('a"; DROP TABLE x --') == '"a""; DROP TABLE x --"'

    cursor = FakeCursor(["Schema"], [["public"]])
    client = client_over(config, cursor)

    assert client.list_schemas('a"; DROP TABLE x --') == ["public"]
    assert cursor.executed == ['SHOW SCHEMAS FROM "a""; DROP TABLE x --"']


def test_an_overrunning_query_is_canceled_not_merely_abandoned(config: Config) -> None:
    """Giving up locally would leave the query burning cluster resources."""
    cursor = SlowCursor(["n"], rows(1))
    # 0.01 minutes = 0.6s, well inside the cursor's 3s sleep. Kept small because
    # this test spends its whole duration waiting for the deadline to expire.
    client = client_over(
        config.model_copy(update={"query_timeout_minutes": 0.01}), cursor
    )

    with pytest.raises(QueryTimeout) as raised:
        client.execute("SELECT n")

    assert cursor.canceled is True
    assert "TRINO_QUERY_TIMEOUT_MINUTES" in str(raised.value)


def test_a_transport_failure_is_retried_on_a_fresh_connection(config: Config) -> None:
    cursor = FailingCursor(["n"], rows(1), OSError("connection reset"))
    client = client_over(config, cursor)

    result = client.execute("SELECT n")

    assert result.rows == rows(1)
    assert cursor.attempts == 2
    assert client.connects == 2  # the stale connection was dropped and remade


@pytest.mark.parametrize(
    ("error", "query", "raised"),
    [
        # Trino understood it and said no; a retry would re-run a failure.
        (
            TrinoQueryError({"errorName": "SYNTAX_ERROR", "message": "nope"}, "q1"),
            "SELECT n",
            QueryRejected,
        ),
        # A dropped socket is not evidence the write did not run — the cluster
        # keeps executing after the client goes away, so a retry can land twice.
        (OSError("connection reset"), "INSERT INTO t VALUES (1)", OSError),
    ],
)
def test_statements_that_could_land_twice_are_not_retried(
    config: Config, error: Exception, query: str, raised: type[Exception]
) -> None:
    cursor = FailingCursor(["n"], rows(1), error)
    client = client_over(config, cursor)

    with pytest.raises(raised):
        client.execute(query)

    assert cursor.attempts == 1
