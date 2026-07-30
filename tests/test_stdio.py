import os
import shutil
import subprocess
import tempfile

import pytest
from mcp import Client, StdioServerParameters
from mcp.client.stdio import stdio_client

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.skipif(
        shutil.which("trino-mcp") is None,
        reason="requires the trino-mcp console script on PATH",
    ),
]


async def test_stdio_session_keeps_stdout_clean() -> None:
    """Completing a session proves stdout carried nothing but JSON-RPC.

    Stdout is the protocol channel under stdio transport, so a stray print, or a
    log handler defaulting to stdout, desynchronises the framing and the session
    fails outright. Asserting the startup banner landed on stderr pins the other
    half: the server does log, and it logs to the right stream.

    Deliberately no tool calls — the connection is opened lazily, so this runs
    with no Trino cluster anywhere.
    """
    with tempfile.TemporaryFile("w+", encoding="utf-8") as errlog:
        # TRINO_USER is required and stdio_client only forwards a fixed set of
        # variables, so it is passed explicitly; it merges over the defaults,
        # leaving PATH intact.
        transport = stdio_client(
            StdioServerParameters(command="trino-mcp", env={"TRINO_USER": "tester"}),
            errlog=errlog,
        )
        async with Client(transport) as client:
            listed = {tool.name for tool in (await client.list_tools()).tools}

        errlog.seek(0)
        captured = errlog.read()

    assert "execute_query" in listed
    assert "trino-mcp" in captured, "startup logging should reach stderr"


def test_startup_failure_never_prints_the_password() -> None:
    """A rejected config must not echo the credential it was handed.

    pydantic attaches the offending input to every error, and the input here is
    the environment dict, so rendering a ValidationError directly would put
    TRINO_PASSWORD on stderr. TRINO_USER is left unset to force the failure.
    """
    env = os.environ | {"TRINO_PASSWORD": "hunter2", "TRINO_HTTP_SCHEME": "http"}
    env.pop("TRINO_USER", None)

    result = subprocess.run(
        ["trino-mcp"],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,  # a non-zero exit is the point of the test
    )

    assert result.returncode == 2
    assert "hunter2" not in result.stderr + result.stdout
    assert "user" in result.stderr
