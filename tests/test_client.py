import csv
import json
import time
from pathlib import Path
from typing import Any

import pytest
from trino.exceptions import TrinoQueryError

from trino_mcp import Config
from trino_mcp.client import QueryTimeout, qualify, quote_ident

from .conftest import FakeCursor, client_over


def rows(count: int) -> list[list[int]]:
    return [[n] for n in range(count)]


class SlowCursor(FakeCursor):
    """A cursor whose execute outlives the deadline."""

    def execute(self, query: str) -> None:
        time.sleep(3)
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
    assert cursor.cancelled is truncated


def test_the_trailing_semicolon_is_stripped_before_execution(config: Config) -> None:
    cursor = FakeCursor(["n"], rows(1))
    client_over(config, cursor).execute("SELECT 1;")

    assert cursor.executed == ["SELECT 1"]


def test_writing_csv_ignores_the_cap_and_reports_the_count(
    config: Config, tmp_path: Path
) -> None:
    """A file export exists to move the *whole* result set, so it is not capped."""
    cursor = FakeCursor(["n"], rows(2500))
    destination = tmp_path / "out.csv"

    written = client_over(
        config.model_copy(update={"max_rows": 10}), cursor
    ).execute_to_file("SELECT n", destination)

    assert written == 2500
    with destination.open(newline="") as handle:
        contents = list(csv.reader(handle))
    assert contents[0] == ["n"]
    assert len(contents) == 2501
    assert contents[-1] == ["2499"]


def test_writing_json_produces_a_parseable_array(config: Config, tmp_path: Path) -> None:
    cursor = FakeCursor(["n"], rows(3))
    destination = tmp_path / "out.json"

    written = client_over(config, cursor).execute_to_file("SELECT n", destination)

    assert written == 3
    assert json.loads(destination.read_text()) == [{"n": 0}, {"n": 1}, {"n": 2}]


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


def test_an_overrunning_query_is_cancelled_not_merely_abandoned(config: Config) -> None:
    """Giving up locally would leave the query burning cluster resources."""
    cursor = SlowCursor(["n"], rows(1))
    # 0.01 minutes = 0.6s, well inside the cursor's 3s sleep. Kept small because
    # this test spends its whole duration waiting for the deadline to expire.
    client = client_over(
        config.model_copy(update={"query_timeout_minutes": 0.01}), cursor
    )

    with pytest.raises(QueryTimeout) as raised:
        client.execute("SELECT n")

    assert cursor.cancelled is True
    assert "TRINO_QUERY_TIMEOUT_MINUTES" in str(raised.value)


def test_a_transport_failure_is_retried_on_a_fresh_connection(config: Config) -> None:
    cursor = FailingCursor(["n"], rows(1), OSError("connection reset"))
    client = client_over(config, cursor)

    result = client.execute("SELECT n")

    assert result.rows == rows(1)
    assert cursor.attempts == 2
    assert client.connects == 2  # the stale connection was dropped and remade


@pytest.mark.parametrize(
    ("error", "query"),
    [
        # Trino understood it and said no; a retry would re-run a failure.
        (
            TrinoQueryError({"errorName": "SYNTAX_ERROR", "message": "nope"}, "q1"),
            "SELECT n",
        ),
        # A dropped socket is not evidence the write did not run — the cluster
        # keeps executing after the client goes away, so a retry can land twice.
        (OSError("connection reset"), "INSERT INTO t VALUES (1)"),
    ],
)
def test_statements_that_could_land_twice_are_not_retried(
    config: Config, error: Exception, query: str
) -> None:
    cursor = FailingCursor(["n"], rows(1), error)
    client = client_over(config, cursor)

    with pytest.raises(type(error)):
        client.execute(query)

    assert cursor.attempts == 1
