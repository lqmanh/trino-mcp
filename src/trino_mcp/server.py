import json
import logging
import sys
from collections.abc import Callable
from datetime import datetime
from importlib.metadata import version
from typing import Annotated, Any, Literal

import anyio.to_thread
from coolname import generate_slug
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import TextContent
from pydantic import BaseModel, Field, ValidationError

from . import manifest as mf
from .client import Profile, QueryRejected, QueryResult, QueryTimeout, TrinoClient
from .config import Config
from .export import OutputFormat
from .sql import is_read_only

log = logging.getLogger(__name__)

# The src layout means this module is importable only when the distribution is installed,
# so the metadata lookup cannot fail at runtime.
__version__ = version("trino-mcp")

PlanFormat = Annotated[
    Literal["LOGICAL", "DISTRIBUTED", "IO", "VALIDATE"],
    Field(
        description=(
            "Which plan to return: LOGICAL for the shape of the query, "
            "DISTRIBUTED to see stage boundaries and data movement, IO "
            "for the tables and partitions it would read, VALIDATE to "
            "only check that the statement is valid."
        )
    ),
]

Pattern = Annotated[
    str,
    Field(
        description=(
            "SQL LIKE pattern to filter names by; % is the wildcard, so "
            "use %foo% for a substring match and foo for an exact one. "
            "Omit to list everything."
        )
    ),
]

Truncated = Annotated[
    bool,
    Field(
        description=(
            "True when the row cap cut this result short, so what you are "
            "looking for may be missing. Narrow the pattern rather than "
            "concluding it does not exist."
        )
    ),
]

QualifiedTable = Annotated[
    str, Field(description="Fully qualified catalog.schema.table.")
]

TableName = Annotated[
    str,
    Field(
        description=(
            "Table name. A qualified 'catalog.schema.table' is accepted, "
            "but passing the parts separately is preferred."
        )
    ),
]

CatalogPart = Annotated[str, Field(description="Catalog, if not in `table`.")]

SchemaPart = Annotated[str, Field(description="Schema, if not in `table`.")]


class Catalogs(BaseModel):
    catalogs: Annotated[list[str], Field(description="Catalog names.")]
    truncated: Truncated


class Schemas(BaseModel):
    schemas: Annotated[list[str], Field(description="Schema names in the catalog.")]
    truncated: Truncated


class Tables(BaseModel):
    tables: Annotated[list[str], Field(description="Table and view names in the schema.")]
    truncated: Truncated


class ColumnMatch(BaseModel):
    table: QualifiedTable
    column: Annotated[str, Field(description="Name of the matching column.")]
    type: Annotated[str, Field(description="Trino type of the matching column.")]


class ColumnMatches(BaseModel):
    matches: Annotated[
        list[ColumnMatch], Field(description="Matches, ordered by schema then table.")
    ]
    truncated: Truncated


class Column(BaseModel):
    name: Annotated[str, Field(description="Column name.")]
    type: Annotated[str, Field(description="Trino type, e.g. varchar(25) or bigint.")]
    extra: Annotated[str, Field(description="Partition key marker, if any.")]
    comment: Annotated[str, Field(description="Column comment, empty when unset.")]


class TableDescription(BaseModel):
    table: QualifiedTable
    columns: Annotated[list[Column], Field(description="Columns in declaration order.")]


class ColumnProfile(BaseModel):
    name: Annotated[str, Field(description="Column name.")]
    nulls_fraction: Annotated[
        float | None,
        Field(description="Share of rows that are NULL, 0 to 1. Null when uncollected."),
    ]
    distinct_values: Annotated[
        int | None,
        Field(
            description="Estimated distinct values. Null means never collected, not zero."
        ),
    ]
    low: Annotated[str | None, Field(description="Smallest value, if known.")]
    high: Annotated[str | None, Field(description="Largest value, if known.")]


class TableProfile(BaseModel):
    table: QualifiedTable
    comment: Annotated[
        str,
        Field(
            description=(
                "The table's own comment, empty when unset. `describe_table` returns "
                "comments on columns only."
            )
        ),
    ]
    connector: Annotated[
        str,
        Field(
            description=(
                "Connector backing the catalog — iceberg, hive, postgresql,... Says "
                "whether this is a lakehouse table or a live operational database."
            )
        ),
    ]
    row_count: Annotated[
        int | None, Field(description="Estimated rows. Null when never collected.")
    ]
    partition_columns: Annotated[
        list[str],
        Field(
            description=(
                "Columns to filter on to prune partitions — the cheapest win on a "
                "large table. Empty means unpartitioned, or the layout was unreadable."
            )
        ),
    ]
    partition_spec: Annotated[
        list[str],
        Field(
            description=(
                "The transforms behind those columns. Partitioning is often hidden: "
                "month(created_at) creates no month column, and filtering to one day "
                "still reads the whole month."
            )
        ),
    ]
    last_written: Annotated[
        str | None,
        Field(
            description=(
                "Last write time, from Iceberg or Delta Lake history. Null on other "
                "connectors, which says nothing about the table. History is pruned, "
                "so this is freshness, not an audit trail."
            )
        ),
    ]
    columns: Annotated[
        list[ColumnProfile],
        Field(
            description=(
                "Per-column statistics. Only columns with a known statistic appear; "
                "call `describe_table` for the full column list."
            )
        ),
    ]
    notes: Annotated[
        list[str],
        Field(
            description=(
                "What could not be gathered, and why. Read this before concluding "
                "something is absent rather than unavailable."
            )
        ),
    ]


class TableJoin(BaseModel):
    columns: Annotated[
        list[str], Field(description="Columns on this table, in key order.")
    ]
    target: QualifiedTable
    target_columns: Annotated[
        list[str], Field(description="Columns on the target, matched by position.")
    ]


class ColumnOrigin(BaseModel):
    name: Annotated[str, Field(description="Column name.")]
    note: Annotated[
        str,
        Field(
            description=(
                "What the Trino type does not convey, e.g. a varchar holding "
                "JSON. Empty when there is nothing to add."
            )
        ),
    ]
    sources: Annotated[
        list[str],
        Field(description="Upstream columns this one derives from, as table.column."),
    ]


class TableFacts(BaseModel):
    table: QualifiedTable
    known: Annotated[
        bool,
        Field(
            description=(
                "False when the manifest has no entry for this table. That is a "
                "gap in coverage, not a statement that the table has no grain."
            )
        ),
    ]
    generated_at: Annotated[
        str,
        Field(
            description=(
                "When the manifest was produced. These facts are static and can "
                "lag the cluster; Trino stays the authority on columns and types."
            )
        ),
    ]
    grain: Annotated[
        list[str],
        Field(
            description=(
                "Columns that together identify one row — what to GROUP BY to "
                "avoid double counting. Empty means unknown, not that rows are unique."
            )
        ),
    ]
    unique_keys: Annotated[
        list[list[str]],
        Field(description="Other column sets known to be unique, besides the grain."),
    ]
    replica_of: Annotated[
        str | None,
        Field(
            description=(
                "The source relation this table copies. Its presence means the "
                "data is a replica and lags the system of record. Null otherwise."
            )
        ),
    ]
    upstream: Annotated[
        list[str],
        Field(description="Tables this one is computed from. Empty for a replica."),
    ]
    joins: Annotated[
        list[TableJoin],
        Field(
            description=(
                "Which columns join this table to another. Inherited from the "
                "source database; nothing in the warehouse enforces them, and "
                "the list is not exhaustive."
            )
        ),
    ]
    columns: Annotated[
        list[ColumnOrigin],
        Field(
            description=(
                "Only columns with something to add; call `describe_table` for "
                "the full list."
            )
        ),
    ]
    note: Annotated[
        str, Field(description="Why an answer is missing, when it is. Empty otherwise.")
    ]


class Plan(BaseModel):
    plan: Annotated[str, Field(description="The query plan as reported by Trino.")]


class FileWritten(BaseModel):
    path: Annotated[str, Field(description="Where the results were written.")]
    rows_written: Annotated[int, Field(description="Number of data rows written.")]


def _instructions(config: Config, tables: int | None = None) -> str:
    cap = "uncapped" if config.max_rows <= 0 else f"{config.max_rows} rows"
    timeout = (
        "no timeout"
        if config.query_timeout_minutes <= 0
        else f"{config.query_timeout_minutes:g} minutes"
    )
    writes = "enabled" if config.allow_writes else "disabled (read-only)"
    catalog_note = (
        ""
        if not tables
        else (
            f"\n- A manifest covering {tables} tables is loaded. Call "
            "`explain_table` before joining or aggregating one: it gives the "
            "grain, the join keys, and where the data came from, none of which "
            "the cluster reports."
        )
    )
    return (
        "Query a Trino cluster over the Model Context Protocol.\n"
        f"- Results are capped at {cap}; a truncated result sets truncated=true, "
        "which means you are seeing a partial answer. Add LIMIT or filters rather "
        "than assuming the rows shown are all of them.\n"
        f"- Queries are canceled after {timeout}.\n"
        f"- Write statements are {writes}.\n"
        "- To pull a large result set out for downstream processing, pass "
        "output_file_format to `execute_query`: the rows go straight to disk "
        "uncapped and never enter this conversation. The server picks the "
        "filename and returns it." + catalog_note
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


def _generate_filename(fmt: OutputFormat) -> str:
    ts = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S")
    return f"{ts}-{generate_slug(3)}.{fmt}"


def create_server(client: TrinoClient, config: Config) -> MCPServer:
    """Build the MCP server around an already-constructed client."""
    store = mf.Store(config.manifest_path) if config.manifest_path else None
    if store is not None and not len(store):
        log.warning(
            "manifest %s covers no tables; explain_table not registered", store.path
        )
        store = None

    server = MCPServer(
        name="trino-mcp",
        version=__version__,
        instructions=_instructions(config, len(store) if store else None),
    )

    async def call[T](func: Callable[..., T], *args: Any) -> T:
        try:
            return await anyio.to_thread.run_sync(func, *args)
        except (QueryTimeout, QueryRejected) as exc:
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

    @server.tool()
    async def list_catalogs(pattern: Pattern = "") -> Catalogs:
        """List the catalogs in the Trino cluster.

        Each catalog is a connector to a distinct data source. Start here when
        exploring an unfamiliar cluster.
        """
        listed = await call(client.list_catalogs, pattern)
        return Catalogs(catalogs=listed.names, truncated=listed.truncated)

    @server.tool()
    async def list_schemas(
        catalog: Annotated[str, Field(description="Exact catalog name.")],
        pattern: Pattern = "",
    ) -> Schemas:
        """List the schemas in a catalog."""
        listed = await call(client.list_schemas, catalog, pattern)
        return Schemas(schemas=listed.names, truncated=listed.truncated)

    @server.tool()
    async def list_tables(
        catalog: Annotated[str, Field(description="Exact catalog name.")],
        schema: Annotated[str, Field(description="Exact schema name.")],
        pattern: Pattern = "",
    ) -> Tables:
        """List the tables and views in a schema."""
        listed = await call(client.list_tables, catalog, schema, pattern)
        return Tables(tables=listed.names, truncated=listed.truncated)

    @server.tool()
    async def search_columns(
        catalog: Annotated[str, Field(description="Exact catalog name.")],
        pattern: Annotated[
            str,
            Field(
                description=(
                    "SQL LIKE pattern matched against column names; % is the wildcard, "
                    "so use %foo% for a substring match and foo for an exact one."
                )
            ),
        ],
        schema: Annotated[
            str,
            Field(
                description=(
                    "Exact schema name, optional. Passing it restricts the search "
                    "and is much faster on a large catalog."
                )
            ),
        ] = "",
    ) -> ColumnMatches:
        """Find which tables contain a column matching a pattern.

        Use this when you know roughly what a column is called but not which table holds it.
        """
        result: QueryResult = await call(client.search_columns, catalog, pattern, schema)
        return ColumnMatches(
            matches=[
                ColumnMatch(
                    table=f"{catalog}.{row[0]}.{row[1]}",
                    column=str(row[2]),
                    type=str(row[3]),
                )
                for row in result.rows
            ],
            truncated=result.truncated,
        )

    @server.tool()
    async def describe_table(
        table: TableName, catalog: CatalogPart = "", schema: SchemaPart = ""
    ) -> TableDescription:
        """Show a table's columns, types, and comments.

        Call this before writing a query against an unfamiliar table. It returns
        the column list and nothing else; for row counts, null fractions,
        freshness, and which column to filter on, call `profile_table`.
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

    @server.tool()
    async def profile_table(
        table: TableName, catalog: CatalogPart = "", schema: SchemaPart = ""
    ) -> TableProfile:
        """Report how big a table is, how it is laid out, and how fresh it is.

        Reach for this before querying an unfamiliar table, or any query whose
        cost you cannot predict: `partition_columns` is what keeps a large table
        cheap, and a table can look unpartitioned until you ask.

        Slower than `describe_table`. Whatever a connector will not answer is
        left out and explained in `notes` rather than failing the call.
        """
        cat, sch, tbl = resolve_target(table, catalog, schema)
        profile: Profile = await call(client.profile_table, cat, sch, tbl)
        return TableProfile(
            table=f"{cat}.{sch}.{tbl}",
            comment=profile.comment,
            connector=profile.connector,
            row_count=profile.row_count,
            partition_columns=profile.partition_columns,
            partition_spec=profile.partition_spec,
            last_written=profile.last_written,
            columns=[
                ColumnProfile(
                    name=stats.name,
                    nulls_fraction=stats.nulls_fraction,
                    distinct_values=stats.distinct_values,
                    low=stats.low,
                    high=stats.high,
                )
                for stats in profile.columns
            ],
            notes=profile.notes,
        )

    # structured_output=False keeps the payload from being emitted twice (once as
    # structured content, once as pretty-printed text) and lets the JSON go out
    # with compact separators — this is the one tool whose output is unbounded.
    @server.tool(structured_output=False)
    async def execute_query(
        sql: Annotated[str, Field(description="The SQL statement to run.")],
        output_file_format: Annotated[
            OutputFormat | None,
            Field(
                description=(
                    "Write the whole result set to a file instead of returning "
                    "rows; only the path and a row count come back. Use it for "
                    "results too large to read. Prefer parquet — smallest, keeps "
                    "each column's Trino type, loads straight into DuckDB, pandas, "
                    "or polars. Use csv only when something downstream cannot read "
                    "parquet, and json when row, array, or map columns need a text "
                    "format. The server names the file."
                )
            ),
        ] = None,
    ) -> TextContent:
        """Run a SQL query and return the rows.

        Results are columnar: {"columns": [...], "rows": [[...]], ...}. A
        "truncated": true field means the row cap cut the result short and you
        are seeing a partial answer.

        With output_file_format the reply is {"path": ..., "rows_written": N}
        instead, and the rows never enter this conversation.

        Read-only unless the server was started with TRINO_ALLOW_WRITES=true.
        """
        if not config.allow_writes and not is_read_only(sql):
            raise ToolError(
                "Only read-only statements are allowed (SELECT, SHOW, DESCRIBE, "
                "EXPLAIN, and CTEs over them). This was rejected as a write, a "
                "multi-statement string, or unparseable SQL. Set "
                "TRINO_ALLOW_WRITES=true to permit writes."
            )

        if output_file_format is not None:
            await call(lambda: config.output_dir.mkdir(parents=True, exist_ok=True))
            destination = config.output_dir / _generate_filename(output_file_format)
            written = await call(
                client.execute_to_file, sql, destination, output_file_format
            )
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

    # Registered only when a manifest is configured and not empty
    if store:

        @server.tool()
        async def explain_table(
            table: TableName, catalog: CatalogPart = "", schema: SchemaPart = ""
        ) -> TableFacts:
            """Explain a table: its grain, how it joins, and where it came from.

            Call this before writing a join or an aggregate. `grain` is the
            column set that makes one row, `joins` gives the columns to join on,
            and `replica_of`, `upstream` and `columns[].sources` answer where a
            value originally came from. Column names alone will not tell you any
            of it, and neither will the cluster.

            Static generated metadata, so it can lag; `describe_table` and
            `profile_table` remain the live truth.
            """
            cat, sch, tbl = resolve_target(table, catalog, schema)
            name = f"{cat}.{sch}.{tbl}"
            facts, loaded = await call(store.lookup, name)
            if facts is None:
                return TableFacts(
                    table=name,
                    known=False,
                    generated_at=loaded.generated_at,
                    grain=[],
                    unique_keys=[],
                    replica_of=None,
                    upstream=[],
                    joins=[],
                    columns=[],
                    note=(
                        f"No entry for {name}. The manifest covers "
                        f"{len(loaded.tables)} other tables, so this is missing "
                        "coverage rather than a table without a grain."
                    ),
                )
            return TableFacts(
                table=name,
                known=True,
                generated_at=loaded.generated_at,
                grain=facts.grain,
                unique_keys=facts.unique_keys,
                replica_of=facts.replica_of,
                upstream=facts.upstream,
                joins=[
                    TableJoin(
                        columns=join.columns,
                        target=join.target,
                        target_columns=join.target_columns,
                    )
                    for join in facts.joins
                ],
                columns=[
                    ColumnOrigin(
                        name=column.name,
                        note=column.note or "",
                        sources=column.sources,
                    )
                    for column in facts.columns
                ],
                note="",
            )

    @server.tool()
    async def explain_query(
        sql: Annotated[str, Field(description="The SQL statement to explain.")],
        plan_format: PlanFormat = "LOGICAL",
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
    log.info("trino-mcp %s | %s", __version__, config)

    client = TrinoClient(config)
    try:
        create_server(client, config).run(transport="stdio")
    finally:
        client.close()
