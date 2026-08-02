"""Shared fixtures and fakes.

Nothing here talks to a Trino cluster; the suite runs offline.
"""

import contextlib
import json
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from mcp import Client
from mcp.types import CallToolResult

from trino_mcp import Config, TrinoClient, create_server
from trino_mcp.client import Names
from trino_mcp.export import OutputFormat


@pytest.fixture
def anyio_backend() -> str:
    """Pin anyio to asyncio; trio is not a dependency."""
    return "asyncio"


@pytest.fixture
def config() -> Config:
    """A config that never reaches the network."""
    return Config(host="trino.invalid", user="tester", catalog="cat", db_schema="sch")


class Rows:
    """An `export.Batches`, which is the whole contract the writers depend on.

    Nothing in `export` may need more than this; if a test here has to grow a
    `description`, that module has re-learned something belonging to the client.
    """

    def __init__(self, rows: list[list[Any]]) -> None:
        self._rows = rows
        self._offset = 0

    def fetchmany(self, size: int) -> list[list[Any]]:
        batch = self._rows[self._offset : self._offset + size]
        self._offset += len(batch)
        return batch


class FakeCursor(Rows):
    """`Rows` plus the slice of the DB-API cursor protocol the client uses.

    Column types are left unset: only the Parquet export reads them, and it is
    driven through `Rows` directly rather than through a cursor.
    """

    def __init__(self, columns: list[str], rows: list[list[Any]]) -> None:
        super().__init__(rows)
        self._columns = columns
        self.description: list[tuple[Any, ...]] | None = None
        self.executed: list[str] = []
        self.canceled = False
        self.query_id = "20260731_000000_00001_abcde"

    def execute(self, query: str) -> None:
        self.executed.append(query)
        self.description = [(name,) + (None,) * 6 for name in self._columns]

    def fetchall(self) -> list[list[Any]]:
        batch = self._rows[self._offset :]
        self._offset = len(self._rows)
        return batch

    def cancel(self) -> None:
        self.canceled = True


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

    def execute_to_file(self, query: str, path: Path, fmt: OutputFormat) -> int:
        self.queries.append(query)
        path.write_text("")
        return self.results.get("execute_to_file", 0)

    def _listing(self, key: str) -> Names:
        """Accept either bare names or a `Names`, so a test opts into truncation."""
        canned = self.results.get(key, [])
        return canned if isinstance(canned, Names) else Names(canned, False)

    def list_catalogs(self, pattern: str = "") -> Names:
        return self._listing("list_catalogs")

    def list_schemas(self, catalog: str, pattern: str = "") -> Names:
        return self._listing("list_schemas")

    def list_tables(self, catalog: str, schema: str, pattern: str = "") -> Names:
        return self._listing("list_tables")

    def describe_table(self, catalog: str, schema: str, table: str) -> Any:
        return self.results["describe_table"]

    def profile_table(self, catalog: str, schema: str, table: str) -> Any:
        return self.results["profile_table"]

    def explain(self, query: str, plan_format: str) -> str:
        self.queries.append(f"EXPLAIN ({plan_format}) {query}")
        return self.results.get("explain", "")


@contextlib.asynccontextmanager
async def serving(config: Config, **results: Any) -> AsyncGenerator[Client]:
    """An MCP client over a server whose Trino results the test supplies."""
    async with Client(create_server(FakeTrino(config, **results), config)) as client:
        yield client


@pytest.fixture
async def connected(config: Config) -> AsyncGenerator[Client]:
    """`serving` with nothing behind it — enough for protocol-level assertions."""
    async with serving(config) as client:
        yield client


def texts(result: CallToolResult) -> list[str]:
    """The text payloads of a tool result, in order."""
    return [block.text for block in result.content if block.type == "text"]


def payload(result: CallToolResult) -> Any:
    """The single JSON text block of a tool result, decoded."""
    return json.loads(texts(result)[0])
