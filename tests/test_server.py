import re
from pathlib import Path

import pytest
from mcp import Client

from trino_mcp import Config
from trino_mcp.client import QueryResult

from .conftest import payload, serving, texts

pytestmark = pytest.mark.anyio

TOOLS = {
    "list_catalogs",
    "list_schemas",
    "list_tables",
    "describe_table",
    "execute_query",
    "explain_query",
}


async def test_the_expected_tools_are_exposed(connected: Client) -> None:
    listed = {tool.name for tool in (await connected.list_tools()).tools}
    assert listed == TOOLS


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


@pytest.mark.parametrize("fmt", ["csv", "json"])
async def test_the_server_names_the_export_file(
    config: Config, tmp_path: Path, fmt: str
) -> None:
    """The model supplies a format and nothing else.

    Asserting the shape rather than a fixed name keeps real randomness and a
    real clock in play; pinning either would need injection this suite has no
    other reason to carry.
    """
    exporting = config.model_copy(update={"output_dir": tmp_path})
    async with serving(exporting, execute_to_file=7) as client:
        outcome = await client.call_tool(
            "execute_query", {"sql": "SELECT 1", "output_file_format": fmt}
        )

    assert not outcome.is_error
    body = payload(outcome)
    assert body["rows_written"] == 7

    written = Path(body["path"])
    assert written.exists()
    assert written.parent == tmp_path.resolve()
    # coolname's three-slot pattern can include a joining token, so the word
    # count is not fixed: "rainbow-turtle-of-holiness".
    assert re.fullmatch(rf"\d{{8}}T\d{{6}}(-[a-z]+)+\.{fmt}", written.name)


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


async def test_a_failing_trino_call_becomes_a_tool_error(config: Config) -> None:
    async with serving(config, execute=RuntimeError("connection refused")) as client:
        outcome = await client.call_tool("execute_query", {"sql": "SELECT 1"})

    assert outcome.is_error
    assert "connection refused" in texts(outcome)[0]
