import csv
import time
from pathlib import Path
from typing import Any

import pytest
from trino.exceptions import TrinoQueryError

from trino_mcp import Config
from trino_mcp.client import QueryRejected, QueryTimeout
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


class ScriptedCursor(FakeCursor):
    """Answers each statement of a profile differently, keyed by a substring.

    `FakeCursor` carries one result set, which the first statement would drain.
    """

    def __init__(self, script: dict[str, Any]) -> None:
        super().__init__([], [])
        self.script = script

    def execute(self, query: str) -> None:
        self.executed.append(query)
        answer = next(
            (value for key, value in self.script.items() if key in query), ([], [])
        )
        if isinstance(answer, Exception):
            raise answer
        self._columns, self._rows, self._offset = *answer, 0
        self.description = [(name,) + (None,) * 6 for name in self._columns]


STATS_COLUMNS = [
    "column_name",
    "data_size",
    "distinct_values_count",
    "nulls_fraction",
    "row_count",
    "low_value",
    "high_value",
]


def profiling_script(connector: str = "iceberg") -> dict[str, Any]:
    return {
        "system.metadata.catalogs": (["connector_name"], [[connector]]),
        "SHOW STATS": (
            STATS_COLUMNS,
            [
                # Trino returns NULL for a statistic it does not have,
                # rather than refusing the statement.
                ["created_at", 1024.0, None, 0.0, None, "2026-01-01", "2026-07-01"],
                ["note", None, None, None, None, None, None],
                [None, None, None, None, 4200.0, None, None],
            ],
        ),
        "SHOW CREATE TABLE": (
            ["Create Table"],
            [
                [
                    (
                        "CREATE TABLE t (a int) COMMENT 'One row per order.' "
                        "WITH (partitioning = ARRAY['month(created_at)'])"
                    )
                ]
            ],
        ),
        "$snapshots": (["committed_at"], [["2026-08-01 10:00:00Z"]]),
        "$history": (["timestamp"], [["2026-08-01 10:00:00Z"]]),
    }


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
    assert destination.exists()


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


def test_untrusted_arguments_reach_sql_only_through_a_quoter(config: Config) -> None:
    """Names and patterns are both tool arguments, and both quote differently.

    The quoting helpers are simple enough to read; what needs pinning is that the
    metadata methods route through them at all, since interpolating straight into
    an f-string produces a statement that looks fine until someone sends a quote.
    """
    cursor = FakeCursor(["name"], [["public"]])
    client = client_over(config, cursor)

    client.list_schemas('a"; DROP TABLE x --')
    client.list_tables("cat", "sch", "%' OR '1'='1")
    client.search_columns("cat", "%id%", schema="public")

    assert cursor.executed == [
        'SHOW SCHEMAS FROM "a""; DROP TABLE x --"',
        """SHOW TABLES FROM "cat"."sch" LIKE '%'' OR ''1''=''1'""",
        (
            "SELECT table_schema, table_name, column_name, data_type "
            'FROM "cat".information_schema.columns '
            "WHERE column_name LIKE '%id%' AND table_schema = 'public' "
            "ORDER BY table_schema, table_name, column_name"
        ),
    ]


def test_each_listing_composes_its_own_like_clause(config: Config) -> None:
    """SHOW CATALOGS takes no FROM, so the three do not concatenate alike.

    The omitted pattern is the fourth case: `LIKE ''` is not the same as no LIKE,
    and would match only names that are empty.
    """
    cursor = FakeCursor(["name"], [["x"]])
    client = client_over(config, cursor)

    client.list_catalogs("%prod%")
    client.list_schemas("cat", "%prod%")
    client.list_tables("cat", "sch", "%prod%")
    client.list_tables("cat", "sch")

    assert cursor.executed == [
        "SHOW CATALOGS LIKE '%prod%'",
        "SHOW SCHEMAS FROM \"cat\" LIKE '%prod%'",
        'SHOW TABLES FROM "cat"."sch" LIKE \'%prod%\'',
        'SHOW TABLES FROM "cat"."sch"',
    ]


def test_a_capped_listing_reports_being_cut_short(config: Config) -> None:
    """Losing the flag here is how a table that exists gets reported as missing."""
    cursor = FakeCursor(["Table"], [[f"t{n}"] for n in range(25)])
    listed = client_over(config.model_copy(update={"max_rows": 10}), cursor).list_tables(
        "cat", "sch"
    )

    assert listed.names == [f"t{n}" for n in range(10)]
    assert listed.truncated is True


def test_a_profile_survives_a_section_that_fails(config: Config) -> None:
    """The parts that answered must still come back.

    Statistics can be refused by permissions, and raising would throw away the
    two good parts for the sake of the bad one.
    """
    cursor = ScriptedCursor(profiling_script() | {"SHOW STATS": OSError("denied")})
    profile = client_over(config, cursor).profile_table("cat", "sch", "orders")

    assert profile.partition_columns == ["created_at"]
    assert profile.comment == "One row per order."
    assert profile.last_written == "2026-08-01 10:00:00Z"
    assert profile.row_count is None
    assert [note.split(":")[0] for note in profile.notes] == ["SHOW STATS"]
    assert "denied" in profile.notes[0]


@pytest.mark.parametrize(
    ("connector", "history"),
    [
        (
            "iceberg",
            (
                'SELECT "committed_at" FROM "cat"."sch"."orders$snapshots" '
                'ORDER BY "committed_at" DESC LIMIT 1'
            ),
        ),
        (
            "delta_lake",
            (
                'SELECT "timestamp" FROM "cat"."sch"."orders$history" '
                'ORDER BY "timestamp" DESC LIMIT 1'
            ),
        ),
        # Anything else is left alone rather than probed and explained: a null
        # last_written is not a claim about the table.
        ("postgresql", None),
    ],
)
def test_freshness_comes_from_the_connectors_own_history_table(
    config: Config, connector: str, history: str | None
) -> None:
    """Each format names its history differently, and one name is reserved.

    The suffix goes inside the identifier's quotes — `"orders$snapshots"`, not
    `"orders"$snapshots` — which parses either way, so only the cluster objects.
    Delta's column is literally `timestamp`, which Trino reads as the start of a
    type literal unless quoted.
    """
    cursor = ScriptedCursor(profiling_script(connector))
    profile = client_over(config, cursor).profile_table("cat", "sch", "orders")

    if history is None:
        assert profile.last_written is None
        assert profile.notes == []
        assert not any("orders$" in query for query in cursor.executed)
    else:
        assert history in cursor.executed
        assert profile.last_written == "2026-08-01 10:00:00Z"


def test_an_uncollected_statistic_stays_unknown(config: Config) -> None:
    """Trino returns NULL rather than failing, and 0 is a different claim.

    Which statistics exist varies by connector — Iceberg carries some in its
    manifests — so NULL only ever means "unknown", where a 0 would say the
    column holds a single value. A column with nothing known at all is dropped instead — on a
    wide table those entries are the whole response and say nothing.
    """
    profile = client_over(config, ScriptedCursor(profiling_script())).profile_table(
        "cat", "sch", "orders"
    )

    assert profile.row_count == 4200
    assert [column.name for column in profile.columns] == ["created_at"]
    assert profile.columns[0].distinct_values is None
    assert profile.columns[0].nulls_fraction == 0.0
    assert profile.columns[0].low == "2026-01-01"


def test_a_profile_stops_after_the_first_timeout(config: Config) -> None:
    """The deadline is per statement, so each part could spend it in turn.

    The caller was promised one timeout, and a part that overran is evidence the
    next will too.
    """
    profile = client_over(
        config.model_copy(update={"query_timeout_minutes": 0.01}),
        SlowCursor(["n"], rows(1)),
    ).profile_table("cat", "sch", "orders")

    assert "TRINO_QUERY_TIMEOUT_MINUTES" in profile.notes[0]
    assert len(profile.notes) > 1
    assert all("skipped" in note for note in profile.notes[1:])


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
