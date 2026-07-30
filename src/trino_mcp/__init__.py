"""Trino MCP server."""

import json
import logging
import sys
from collections.abc import Callable
from importlib.metadata import version
from pathlib import Path
from typing import Annotated, Any, Literal

import anyio.to_thread
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import TextContent
from pydantic import BaseModel, Field, ValidationError

from .client import QueryResult, QueryTimeout, TrinoClient
from .config import Config
from .sql import is_read_only

log = logging.getLogger(__name__)

# The src layout means this module is importable only when the distribution is
# installed, so the metadata lookup cannot fail at runtime.
__version__ = version("trino-mcp")

__all__ = ["Config", "TrinoClient", "__version__", "create_server", "main"]

PlanFormat = Literal["LOGICAL", "DISTRIBUTED", "IO", "VALIDATE"]


class Catalogs(BaseModel):
    catalogs: Annotated[list[str], Field(description="Catalog names.")]


class Schemas(BaseModel):
    schemas: Annotated[list[str], Field(description="Schema names in the catalog.")]


class Tables(BaseModel):
    tables: Annotated[
        list[str], Field(description="Table and view names in the schema.")
    ]


class Column(BaseModel):
    name: Annotated[str, Field(description="Column name.")]
    type: Annotated[str, Field(description="Trino type, e.g. varchar(25) or bigint.")]
    extra: Annotated[str, Field(description="Partition key marker, if any.")]
    comment: Annotated[str, Field(description="Column comment, empty when unset.")]


class TableDescription(BaseModel):
    table: Annotated[str, Field(description="Fully qualified table name.")]
    columns: Annotated[list[Column], Field(description="Columns in declaration order.")]


class Plan(BaseModel):
    plan: Annotated[str, Field(description="The query plan as reported by Trino.")]


class FileWritten(BaseModel):
    path: Annotated[str, Field(description="Where the results were written.")]
    rows_written: Annotated[int, Field(description="Number of data rows written.")]


def _instructions(config: Config) -> str:
    cap = "uncapped" if config.max_rows <= 0 else f"{config.max_rows} rows"
    timeout = (
        "no timeout"
        if config.query_timeout_minutes <= 0
        else f"{config.query_timeout_minutes:g} minutes"
    )
    writes = "enabled" if config.allow_writes else "disabled (read-only)"
    return (
        "Query a Trino cluster over the Model Context Protocol.\n"
        f"- Results are capped at {cap}; a truncated result sets truncated=true, "
        "which means you are seeing a partial answer. Add LIMIT or filters rather "
        "than assuming the rows shown are all of them.\n"
        f"- Queries are cancelled after {timeout}.\n"
        f"- Write statements are {writes}.\n"
        "- To pull a large result set out for downstream processing, pass "
        "output_file to `execute_query`: the rows go straight to disk uncapped and "
        "never "
        "enter this conversation."
    )


def _split_table(table: str, catalog: str, schema: str) -> tuple[str, str, str]:
    """Accept ``table``, ``schema.table``, or ``catalog.schema.table``.

    Explicit arguments win over parts parsed out of the name. Quoted identifiers
    containing dots are not handled; pass those as separate arguments.
    """
    parts = table.split(".")
    if len(parts) == 3:
        return catalog or parts[0], schema or parts[1], parts[2]
    if len(parts) == 2:
        return catalog, schema or parts[0], parts[1]
    return catalog, schema, table


def create_server(client: TrinoClient, config: Config) -> MCPServer:
    """Build the MCP server around an already-constructed client.

    Taking the client as an argument rather than reaching for module state is
    what lets the tests drive every tool against a fake.
    """
    server = MCPServer(
        name="trino-mcp",
        version=__version__,
        instructions=_instructions(config),
    )

    async def call[T](func: Callable[..., T], *args: Any) -> T:
        """Run a blocking client call off the event loop, as a tool-shaped error."""
        try:
            return await anyio.to_thread.run_sync(func, *args)
        except QueryTimeout as exc:
            raise ToolError(str(exc)) from exc
        except Exception as exc:
            log.exception("Trino call failed")
            raise ToolError(f"{type(exc).__name__}: {exc}") from exc

    def resolve_target(table: str, catalog: str, schema: str) -> tuple[str, str, str]:
        cat, sch, tbl = _split_table(table, catalog, schema)
        cat = cat or (config.catalog or "")
        sch = sch or (config.db_schema or "")
        if not cat or not sch:
            raise ToolError(
                "Both catalog and schema are required. Pass them as arguments, use a "
                "fully qualified 'catalog.schema.table' name, or set TRINO_CATALOG and "
                "TRINO_SCHEMA."
            )
        return cat, sch, tbl

    def resolve_output(output_file: str) -> Path:
        """Confine an output path to TRINO_OUTPUT_DIR.

        The path is chosen by the model, and query results are untrusted input,
        so text arriving from the warehouse must not be able to steer a write
        anywhere on the filesystem.
        """
        root = config.output_dir.expanduser().resolve()
        candidate = (root / Path(output_file).expanduser()).resolve()
        if candidate != root and root not in candidate.parents:
            raise ToolError(
                f"output_file must stay inside {root}. Set TRINO_OUTPUT_DIR to write elsewhere."
            )
        candidate.parent.mkdir(parents=True, exist_ok=True)
        return candidate

    @server.tool()
    async def list_catalogs() -> Catalogs:
        """List the catalogs available on the Trino cluster.

        Each catalog is a connector to a distinct data source. Start here when
        exploring an unfamiliar cluster.
        """
        return Catalogs(catalogs=await call(client.list_catalogs))

    @server.tool()
    async def list_schemas(
        catalog: Annotated[str, Field(description="Catalog to list schemas from.")],
    ) -> Schemas:
        """List the schemas in a catalog."""
        return Schemas(schemas=await call(client.list_schemas, catalog))

    @server.tool()
    async def list_tables(
        catalog: Annotated[str, Field(description="Catalog containing the schema.")],
        schema: Annotated[str, Field(description="Schema to list tables from.")],
    ) -> Tables:
        """List the tables and views in a schema."""
        return Tables(tables=await call(client.list_tables, catalog, schema))

    @server.tool()
    async def describe_table(
        table: Annotated[
            str,
            Field(
                description=(
                    "Table name. A qualified 'catalog.schema.table' is accepted, "
                    "but passing the parts separately is preferred."
                )
            ),
        ],
        catalog: Annotated[str, Field(description="Catalog, if not in `table`.")] = "",
        schema: Annotated[str, Field(description="Schema, if not in `table`.")] = "",
    ) -> TableDescription:
        """Show a table's columns, types, and comments.

        Call this before writing a query against an unfamiliar table.
        """
        cat, sch, tbl = resolve_target(table, catalog, schema)
        result: QueryResult = await call(client.describe_table, cat, sch, tbl)
        return TableDescription(
            table=f"{cat}.{sch}.{tbl}",
            columns=[
                Column(
                    name=str(row[0]),
                    type=str(row[1]),
                    extra=str(row[2] or ""),
                    comment=str(row[3] or ""),
                )
                for row in result.rows
            ],
        )

    # structured_output=False keeps the payload from being emitted twice (once as
    # structured content, once as pretty-printed text) and lets the JSON go out
    # with compact separators — this is the one tool whose output is unbounded.
    @server.tool(structured_output=False)
    async def execute_query(
        sql: Annotated[str, Field(description="The SQL statement to run.")],
        output_file: Annotated[
            str,
            Field(
                description=(
                    "Optional path, relative to the server's output directory. When "
                    "set, all rows are written there uncapped and only a count is "
                    "returned — use this for result sets too large to read."
                )
            ),
        ] = "",
    ) -> TextContent:
        """Run a SQL query and return the rows.

        Results come back columnar: {"columns": [...], "rows": [[...]], ...}.
        A "truncated": true field means the row cap cut the result short and you
        are seeing a partial answer.

        Read-only by default; write statements are rejected unless the server was
        started with TRINO_ALLOW_WRITES=true.
        """
        if not config.allow_writes and not is_read_only(sql):
            raise ToolError(
                "Only read-only statements are allowed (SELECT, SHOW, DESCRIBE, "
                "EXPLAIN, and CTEs over them). This was rejected as a write, a "
                "multi-statement string, or unparseable SQL. Set "
                "TRINO_ALLOW_WRITES=true to permit writes."
            )

        if output_file:
            destination = resolve_output(output_file)
            written = await call(client.execute_to_file, sql, destination)
            payload = FileWritten(
                path=str(destination), rows_written=written
            ).model_dump()
        else:
            result: QueryResult = await call(client.execute, sql)
            payload = {
                "columns": result.columns,
                "rows": result.rows,
                "row_count": result.row_count,
                "truncated": result.truncated,
            }

        return TextContent(
            type="text",
            text=json.dumps(payload, default=str, separators=(",", ":")),
        )

    @server.tool()
    async def explain_query(
        sql: Annotated[str, Field(description="The SQL statement to explain.")],
        plan_format: Annotated[
            PlanFormat, Field(description="Plan detail to return.")
        ] = "LOGICAL",
    ) -> Plan:
        """Show how Trino would execute a query, without running it.

        Use this to sanity-check an expensive query before paying for it.
        """
        return Plan(plan=await call(client.explain, sql, plan_format))

    return server


def main() -> None:
    # Under stdio transport stdout carries the JSON-RPC framing, so logs must go
    # to stderr or the client drops the connection on a parse error.
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        config = Config.from_env()
    except ValidationError as exc:
        # Not str(exc): pydantic renders the input alongside each error,
        # and here it is the environment dict, with TRINO_PASSWORD included.
        # include_input=False keeps the credential out of the log.
        detail = "\n".join(
            f"  {'.'.join(str(part) for part in error['loc']) or 'config'}: {error['msg']}"
            for error in exc.errors(include_input=False, include_url=False)
        )
        log.error("invalid configuration:\n%s", detail)
        raise SystemExit(2) from exc

    # Logged, not connected: the cluster is reached on the first tool call so a
    # brief outage does not stop the server from starting.
    log.info("trino-mcp %s | %s", __version__, config.summary)

    client = TrinoClient(config)
    try:
        create_server(client, config).run(transport="stdio")
    finally:
        client.close()
