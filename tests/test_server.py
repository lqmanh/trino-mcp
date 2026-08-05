import logging
import re
from pathlib import Path

import pytest
from mcp import Client

from trino_mcp import Config
from trino_mcp.client import ColumnStats, Names, Profile, QueryResult

from .conftest import payload, serving, texts, with_manifest

pytestmark = pytest.mark.anyio

TOOLS = {
    "list_catalogs",
    "list_schemas",
    "list_tables",
    "search_columns",
    "describe_table",
    "profile_table",
    "execute_query",
    "explain_query",
}


async def test_the_expected_tools_are_exposed(connected: Client) -> None:
    listed = {tool.name for tool in (await connected.list_tools()).tools}
    assert listed == TOOLS


async def test_explain_table_appears_only_with_a_manifest(
    config: Config, tmp_path: Path
) -> None:
    """Its description ships in every session's system prompt, called or not.

    The equality assertion above is the other half: no manifest, no tool, no cost.
    """
    async with serving(with_manifest(config, tmp_path / "m.json")) as client:
        listed = {tool.name for tool in (await client.list_tools()).tools}

    assert listed == TOOLS | {"explain_table"}


async def test_an_empty_manifest_is_dropped_but_said_out_loud(
    config: Config, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Covering nothing is a broken generator run, not a deployment choice.

    Refusing to start would take the other tools down over an optional one, so
    the tool goes unregistered — and that is invisible to whoever configured a
    manifest unless the log says why.
    """
    empty = {"version": 1, "generated_at": "2026-08-05T00:00:00+00:00", "tables": {}}
    with caplog.at_level(logging.WARNING):
        async with serving(with_manifest(config, tmp_path / "m.json", empty)) as client:
            listed = {tool.name for tool in (await client.list_tools()).tools}
            instructions = client.instructions or ""

    assert listed == TOOLS
    assert "manifest covering" not in instructions
    assert "covers no tables" in caplog.text


async def test_explain_table_returns_the_facts_trino_cannot(
    config: Config, tmp_path: Path
) -> None:
    """Every field here answers a question the cluster has no way to answer."""
    async with serving(with_manifest(config, tmp_path / "m.json")) as client:
        result = await client.call_tool("explain_table", {"table": "cat.sch.orders"})

    assert result.structured_content == {
        "table": "cat.sch.orders",
        "known": True,
        "generated_at": "2026-08-05T00:00:00+00:00",
        "grain": ["id"],
        "unique_keys": [["reference"]],
        "replica_of": "pg_cat.public.orders",
        "upstream": [],
        "joins": [
            {"columns": ["user_id"], "target": "cat.sch.users", "target_columns": ["id"]}
        ],
        "columns": [{"name": "payload", "note": "JSON held as varchar", "sources": []}],
        "note": "",
    }


async def test_a_table_outside_the_manifest_says_so(
    config: Config, tmp_path: Path
) -> None:
    """Empty fields alone would read as "this table has no grain".

    Absence of coverage and absence of a grain are different answers, and only
    one of them is a fact about the table.
    """
    async with serving(with_manifest(config, tmp_path / "m.json")) as client:
        result = await client.call_tool("explain_table", {"table": "cat.sch.nowhere"})

    assert result.structured_content is not None
    assert result.structured_content["known"] is False
    assert result.structured_content["grain"] == []
    assert "missing coverage" in result.structured_content["note"]


@pytest.mark.parametrize("truncated", [False, True])
async def test_query_returns_the_columnar_shape(config: Config, truncated: bool) -> None:
    """Both flag values matter: a partial answer the model reads as complete is a bug."""
    result = QueryResult(columns=["a", "b"], rows=[[1, 2], [3, 4]], truncated=truncated)
    async with serving(config, execute=result) as client:
        body = payload(
            await client.call_tool("execute_query", {"sql": "SELECT a, b FROM t"})
        )

    assert body == {
        "columns": ["a", "b"],
        "rows": [[1, 2], [3, 4]],
        "row_count": 2,
        "truncated": truncated,
    }


@pytest.mark.parametrize("allow_writes", [False, True])
async def test_writes_are_gated_on_allow_writes(
    config: Config, allow_writes: bool
) -> None:
    gated = config.model_copy(update={"allow_writes": allow_writes})
    async with serving(gated, execute=QueryResult([], [], False)) as client:
        outcome = await client.call_tool("execute_query", {"sql": "DROP TABLE t"})

    if allow_writes:
        assert not outcome.is_error
    else:
        assert outcome.is_error
        assert "TRINO_ALLOW_WRITES" in texts(outcome)[0]


async def test_the_server_names_the_export_file(config: Config, tmp_path: Path) -> None:
    """The model supplies a format and nothing else.

    Asserting the shape rather than a fixed name keeps real randomness and a
    real clock in play; pinning either would need injection this suite has no
    other reason to carry.
    """
    exporting = config.model_copy(update={"output_dir": tmp_path})
    async with serving(exporting, execute_to_file=7) as client:
        outcome = await client.call_tool(
            "execute_query", {"sql": "SELECT 1", "output_file_format": "parquet"}
        )

    assert not outcome.is_error
    body = payload(outcome)
    assert body["rows_written"] == 7

    written = Path(body["path"])
    assert written.exists()
    assert written.parent == tmp_path.resolve()
    # coolname's three-slot pattern can include a joining token, so the word
    # count is not fixed: "rainbow-turtle-of-holiness".
    assert re.fullmatch(r"\d{8}T\d{6}(-[a-z]+)+\.parquet", written.name)


async def test_the_output_directory_is_created_on_demand(
    config: Config, tmp_path: Path
) -> None:
    """Building a server must not touch the filesystem, so creation waits."""
    target = tmp_path / "does" / "not" / "exist"
    exporting = config.model_copy(update={"output_dir": target})

    async with serving(exporting, execute_to_file=1) as client:
        assert not target.exists()
        outcome = await client.call_tool(
            "execute_query", {"sql": "SELECT 1", "output_file_format": "csv"}
        )

    assert not outcome.is_error
    assert target.is_dir()


async def test_describe_table_maps_columns(config: Config) -> None:
    described = QueryResult(
        columns=["Column", "Type", "Extra", "Comment"],
        rows=[["id", "bigint", "", "the key"], ["name", "varchar(25)", "", None]],
        truncated=False,
    )
    async with serving(config, describe_table=described) as client:
        result = await client.call_tool("describe_table", {"table": "cat.sch.people"})

    assert result.structured_content == {
        "table": "cat.sch.people",
        "columns": [
            {"name": "id", "type": "bigint", "extra": "", "comment": "the key"},
            {"name": "name", "type": "varchar(25)", "extra": "", "comment": ""},
        ],
    }


@pytest.mark.parametrize("truncated", [False, True])
async def test_a_listing_carries_its_truncation_flag(
    config: Config, truncated: bool
) -> None:
    """Every listing is capped, so every listing has to be able to admit it."""
    async with serving(config, list_tables=Names(["orders", "returns"], truncated)) as (
        client
    ):
        result = await client.call_tool(
            "list_tables", {"catalog": "cat", "schema": "sch"}
        )

    assert result.structured_content == {
        "tables": ["orders", "returns"],
        "truncated": truncated,
    }


@pytest.mark.parametrize("truncated", [False, True])
async def test_search_matches_carry_a_qualified_name(
    config: Config, truncated: bool
) -> None:
    """The catalog is an argument, not a result column, so the tool re-attaches it.

    Returning a bare table name would leave the model to guess the catalog it
    just supplied. The flag matters as much: a truncated search that reads as
    complete says the column exists nowhere else.
    """
    found = QueryResult(
        columns=["table_schema", "table_name", "column_name", "data_type"],
        rows=[["public", "orders", "customer_id", "varchar"]],
        truncated=truncated,
    )
    async with serving(config, execute=found) as client:
        result = await client.call_tool(
            "search_columns", {"catalog": "iceberg", "pattern": "%customer%"}
        )

    assert result.structured_content == {
        "matches": [
            {
                "table": "iceberg.public.orders",
                "column": "customer_id",
                "type": "varchar",
            }
        ],
        "truncated": truncated,
    }


PROFILE = Profile(
    connector="hive",
    comment="",
    row_count=None,
    partition_spec=["dt"],
    partition_columns=["dt"],
    last_written=None,
    columns=[ColumnStats("dt", None, None, None, None)],
    notes=["$snapshots: TrinoQueryError: TABLE_NOT_FOUND"],
)


async def test_a_partial_profile_reaches_the_model_intact(config: Config) -> None:
    """An unknown statistic has to arrive as null, and the reason alongside it.

    Coerce a stat on the way out, or drop `notes`, and an empty profile reads as
    a fact about the table rather than an absence of information.
    """
    async with serving(config, profile_table=PROFILE) as client:
        result = await client.call_tool("profile_table", {"table": "cat.sch.events"})

    assert result.structured_content == {
        "table": "cat.sch.events",
        "comment": "",
        "connector": "hive",
        "row_count": None,
        "partition_columns": ["dt"],
        "partition_spec": ["dt"],
        "last_written": None,
        "columns": [
            {
                "name": "dt",
                "nulls_fraction": None,
                "distinct_values": None,
                "low": None,
                "high": None,
            }
        ],
        "notes": ["$snapshots: TrinoQueryError: TABLE_NOT_FOUND"],
    }


async def test_a_failing_trino_call_becomes_a_tool_error(config: Config) -> None:
    async with serving(config, execute=RuntimeError("connection refused")) as client:
        outcome = await client.call_tool("execute_query", {"sql": "SELECT 1"})

    assert outcome.is_error
    assert "connection refused" in texts(outcome)[0]
