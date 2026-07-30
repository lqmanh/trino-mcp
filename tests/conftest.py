from collections.abc import AsyncIterator

import pytest
from mcp import Client
from mcp.types import CallToolResult

from trino_mcp import server


@pytest.fixture
def anyio_backend() -> str:
    """Pin anyio to asyncio; trio is not a dependency."""
    return "asyncio"


@pytest.fixture
async def client() -> AsyncIterator[Client]:
    """An MCP client wired directly to the server, with no subprocess."""
    async with Client(server) as connected:
        yield connected


def texts(result: CallToolResult) -> list[str]:
    """The text payloads of a tool result, in order."""
    return [block.text for block in result.content if block.type == "text"]
