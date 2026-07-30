"""Shared fixtures and fakes.

Nothing here talks to a Trino cluster; the suite runs offline.
"""

import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
from mcp import Client
from mcp.types import CallToolResult

from trino_mcp import Config, TrinoClient, create_server


@pytest.fixture
def anyio_backend() -> str:
    """Pin anyio to asyncio; trio is not a dependency."""
    return "asyncio"


@pytest.fixture
def config() -> Config:
    """A config that never reaches the network."""
    return Config(host="trino.invalid", user="tester", catalog="cat", db_schema="sch")


class FakeCursor:
    """The slice of the DB-API cursor protocol the client actually uses."""

    def __init__(self, columns: list[str], rows: list[list[Any]]) -> None:
        self._columns = columns
        self._rows = rows
        self._offset = 0
        self.description: list[tuple[Any, ...]] | None = None
        self.executed: list[str] = []
        self.cancelled = False
        self.query_id = "20260731_000000_00001_abcde"

    def execute(self, query: str) -> None:
        self.executed.append(query)
        self.description = [(name,) + (None,) * 6 for name in self._columns]

    def fetchmany(self, size: int) -> list[list[Any]]:
        batch = self._rows[self._offset : self._offset + size]
        self._offset += len(batch)
        return batch

    def fetchall(self) -> list[list[Any]]:
        batch = self._rows[self._offset :]
        self._offset = len(self._rows)
        return batch

    def cancel(self) -> None:
        self.cancelled = True


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


class StubbedClient(TrinoClient):
    """A TrinoClient that dials a fake connection instead of a cluster.

    Overriding `_connect` rather than assigning `_connection` means reconnection
    works too, so the retry path is exercisable.
    """

    def __init__(self, config: Config, cursor: FakeCursor) -> None:
        super().__init__(config)
        self.cursor = cursor
        self.connects = 0

    def _connect(self) -> Any:
        self.connects += 1
        return FakeConnection(self.cursor)


def client_over(config: Config, cursor: FakeCursor) -> StubbedClient:
    return StubbedClient(config, cursor)


class FakeTrino(TrinoClient):
    """A client whose results are set by the test rather than by a cluster."""

    def __init__(self, config: Config, **results: Any) -> None:
        super().__init__(config)
        self.results = results
        self.queries: list[str] = []

    def execute(self, query: str) -> Any:
        self.queries.append(query)
        if isinstance(self.results.get("execute"), Exception):
            raise self.results["execute"]
        return self.results["execute"]

    def execute_to_file(self, query: str, path: Any) -> int:
        self.queries.append(query)
        return self.results.get("execute_to_file", 0)

    def list_catalogs(self) -> list[str]:
        return self.results.get("list_catalogs", [])

    def list_schemas(self, catalog: str) -> list[str]:
        return self.results.get("list_schemas", [])

    def list_tables(self, catalog: str, schema: str) -> list[str]:
        return self.results.get("list_tables", [])

    def describe_table(self, catalog: str, schema: str, table: str) -> Any:
        return self.results["describe_table"]

    def explain(self, query: str, plan_format: str) -> str:
        self.queries.append(f"EXPLAIN ({plan_format}) {query}")
        return self.results.get("explain", "")


@pytest.fixture
async def connected() -> AsyncIterator[Client]:
    """An MCP client wired to a server with no useful data behind it.

    Enough for protocol-level assertions; tests needing real results build their
    own server with `serving`.
    """
    config = Config(host="trino.invalid", user="tester")
    async with Client(create_server(FakeTrino(config), config)) as client:
        yield client


def texts(result: CallToolResult) -> list[str]:
    """The text payloads of a tool result, in order."""
    return [block.text for block in result.content if block.type == "text"]


def payload(result: CallToolResult) -> Any:
    """The single JSON text block of a tool result, decoded."""
    return json.loads(texts(result)[0])
