from importlib.metadata import version

import pytest
from mcp import Client

from .conftest import texts

pytestmark = pytest.mark.anyio


async def test_server_reports_version_from_package_metadata(client: Client) -> None:
    """The version reaches the wire from installed metadata, not a literal."""
    info = client.server_info
    assert info is not None
    assert info.name == "trino-mcp"
    assert info.version == version("trino-mcp")


async def test_ping_is_listed_and_callable(client: Client) -> None:
    listed = await client.list_tools()
    assert "ping" in [tool.name for tool in listed.tools]
    assert texts(await client.call_tool("ping", {})) == ["pong"]
