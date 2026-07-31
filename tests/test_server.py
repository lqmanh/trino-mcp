import contextlib
from collections.abc import AsyncIterator
from typing import Any

import pytest
from mcp import Client

from trino_mcp import Config, create_server
from trino_mcp.client import QueryResult

from .conftest import FakeTrino, payload, texts

pytestmark = pytest.mark.anyio

TOOLS = {
    "list_catalogs",
    "list_schemas",
    "list_tables",
    "describe_table",
    "execute_query",
    "explain_query",
}


@contextlib.asynccontextmanager
async def serving(config: Config, **results: Any) -> AsyncIterator[Client]:
    """An MCP client over a server whose Trino results the test supplies."""
    async with Client(create_server(FakeTrino(config, **results), config)) as client:
        yield client


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


async def test_output_file_cannot_escape_the_output_directory(
    config: Config, tmp_path: Any
) -> None:
    """The path comes from the model, and query results are untrusted input."""
    jailed = config.model_copy(update={"output_dir": tmp_path})
    async with serving(jailed, execute_to_file=7) as client:
        escape = await client.call_tool(
            "execute_query", {"sql": "SELECT 1", "output_file": "../../escaped.csv"}
        )
        allowed = await client.call_tool(
            "execute_query", {"sql": "SELECT 1", "output_file": "nested/fine.csv"}
        )

    assert escape.is_error
    assert "TRINO_OUTPUT_DIR" in texts(escape)[0]

    assert not allowed.is_error
    assert payload(allowed)["rows_written"] == 7


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
