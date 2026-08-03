"""What only a real cluster can settle.

Every fake in `tests/conftest.py` was written from the same reading of the docs
as the code it stands in for, so a misread survives the offline suite intact.
These drive a real `TrinoClient` against real Trino output instead.
"""

from pathlib import Path

import duckdb
import pytest

from trino_mcp import Config
from trino_mcp.client import TrinoClient

pytestmark = pytest.mark.e2e


@pytest.mark.parametrize(
    ("target", "connector", "comment", "row_count", "spec", "columns", "history"),
    [
        (
            ("iceberg", "sales", "orders"),
            "iceberg",
            "One row per order, from tpch",
            15000,
            ["month(orderdate)"],
            ["orderdate"],
            True,
        ),
        (("iceberg", "sales", "customer"), "iceberg", "", 1500, [], [], True),
        (
            ("delta", "sales", "shipments"),
            "delta_lake",
            "One row per shipment, from tpch",
            60175,
            ["linestatus"],
            ["linestatus"],
            True,
        ),
        (("pgsource", "public", "regions"), "postgresql", "", None, [], [], False),
    ],
    ids=["iceberg-partitioned", "iceberg-plain", "delta-partitioned", "postgresql"],
)
def test_a_profile_matches_the_cluster(
    live: TrinoClient,
    target: tuple[str, str, str],
    connector: str,
    comment: str,
    row_count: int | None,
    spec: list[str],
    columns: list[str],
    history: bool,
) -> None:
    """One call, and it pins every assumption the parsing rests on.

    A misnamed statistics column reads as "unknown", a changed `SHOW CREATE
    TABLE` rendering empties `partition_columns`, and either would send a model
    into a full scan without failing anything. The partitioned case proves the
    transform resolves to the column a WHERE clause has to name; the Delta case
    covers the other spelling of both branches — `partitioned_by` rather than
    `partitioning`, `$history` rather than `$snapshots`; and the relational case
    proves a connector with neither stays quiet rather than reporting a problem.
    """
    profile = live.profile_table(*target)

    assert profile.connector == connector
    assert profile.comment == comment
    assert profile.row_count == row_count
    assert profile.partition_spec == spec
    assert profile.partition_columns == columns
    assert (profile.last_written is not None) is history
    assert profile.notes == []

    if row_count is not None:
        assert profile.columns
        assert all(column.nulls_fraction is not None for column in profile.columns)


def test_a_capped_query_stops_a_real_paged_cursor(live_config: Config) -> None:
    """`_collect` fetches `cap + 1` in a loop, against pages Trino chooses.

    The offline fake hands back exactly what it is asked for, so the loop has
    only ever run against a cooperative cursor. Real paging is what decides
    whether the extra row — the whole basis for the truncation flag — actually
    turns up.
    """
    client = TrinoClient(live_config.model_copy(update={"max_rows": 10}))
    try:
        result = client.execute("SELECT * FROM iceberg.sales.orders")
    finally:
        client.close()

    assert len(result.rows) == 10
    assert result.truncated is True


def test_a_parquet_export_round_trips_real_trino_types(
    live: TrinoClient, tmp_path: Path
) -> None:
    """`export.py` maps Trino type names to DuckDB, from hand-written fixtures.

    This is the first time the real `cursor.description` type codes reach the
    writer, which is the only way to find out that the mapping matches what
    Trino actually reports.
    """
    destination = tmp_path / "orders.parquet"
    written = live.execute_to_file(
        "SELECT orderkey, orderdate, totalprice, comment FROM iceberg.sales.orders",
        destination,
        "parquet",
    )

    assert written == 15000
    described = duckdb.sql(
        f"DESCRIBE SELECT * FROM read_parquet('{destination}')"
    ).fetchall()
    types = {row[0]: row[1] for row in described}
    assert types == {
        "orderkey": "BIGINT",
        "orderdate": "DATE",
        "totalprice": "DOUBLE",
        "comment": "VARCHAR",
    }
