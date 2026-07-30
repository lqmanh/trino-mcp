"""Trino MCP server."""

import logging
import sys
from importlib.metadata import version

from mcp.server import MCPServer

log = logging.getLogger(__name__)

# The src layout means this module is importable only when the distribution is
# installed, so the metadata lookup cannot fail at runtime.
__version__ = version("trino-mcp")

server = MCPServer(
    name="trino-mcp",
    version=__version__,
    instructions="Query Trino clusters over the Model Context Protocol.",
)


@server.tool()
def ping() -> str:
    """Check that the server is reachable."""
    log.info("ping")
    return "pong"


def main() -> None:
    # Under stdio transport stdout carries the JSON-RPC framing, so logs must go
    # to stderr or the client drops the connection on a parse error.
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    server.run(transport="stdio")
