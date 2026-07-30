import shutil
import tempfile

import pytest
from mcp import Client, StdioServerParameters
from mcp.client.stdio import stdio_client

from .conftest import texts

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.skipif(
        shutil.which("trino-mcp") is None,
        reason="requires the trino-mcp console script on PATH",
    ),
]


async def test_stdio_session_keeps_stdout_clean() -> None:
    """Completing a session proves stdout carried nothing but JSON-RPC.

    Stdout is the protocol channel under stdio transport, so a stray print or a
    log handler defaulting to stdout desynchronises the framing and the session
    fails. Asserting the log line landed on stderr pins down both halves.
    """
    with tempfile.TemporaryFile("w+", encoding="utf-8") as errlog:
        transport = stdio_client(
            StdioServerParameters(command="trino-mcp"), errlog=errlog
        )
        async with Client(transport) as client:
            assert texts(await client.call_tool("ping", {})) == ["pong"]

        errlog.seek(0)
        captured = errlog.read()

    assert "ping" in captured, "tool logging should reach stderr"
