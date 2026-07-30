# Prior Art and Comparison

Three Trino MCP servers already existed when this one was started. The design here owes a
lot to what they got right and to the problems they had already run into. Links below are
pinned to the commit that was read, so they keep resolving to the code described.

- **[tuannvm/mcp-trino](https://github.com/tuannvm/mcp-trino)** (Go) — the most mature and
  actively maintained.
- **[alaturqua/mcp-trino-python](https://github.com/alaturqua/mcp-trino-python)** (Python)
  — the widest tool surface, oriented at Iceberg table maintenance.
- **[weijie-tan3/trino-mcp](https://github.com/weijie-tan3/trino-mcp)** (Python) — closest
  in spirit to this one.

## At a glance

|                    | mcp-trino                                   | mcp-trino-python | trino-mcp                   | **this**               |
| ------------------ | ------------------------------------------- | ---------------- | --------------------------- | ---------------------- |
| Language           | Go                                          | Python           | Python                      | Python                 |
| Tools              | 6                                           | 22               | 8                           | 6                      |
| Read-only guard    | regex + keywords                            | none             | sqlglot AST                 | sqlglot AST            |
| Row limit          | yes, + truncated flag                       | no               | no                          | yes, + truncated flag  |
| Query timeout      | context deadline                            | no               | yes, cancels                | yes, cancels           |
| Identifier quoting | no                                          | no               | no                          | **yes**                |
| Result shape       | row dicts                                   | row dicts        | row dicts                   | **columnar**           |
| File export        | no                                          | no               | yes                         | yes, uncapped + jailed |
| Auth               | password, OAuth2, JWT, Azure, impersonation | password         | password, OAuth2, Azure SPN | password               |
| Transport          | stdio, HTTP, SSE                            | stdio, HTTP, SSE | stdio                       | stdio                  |

`mcp-trino` is solving a materially larger problem than this project does — multi-tenant,
deployed, SSO — and ships a CLI, a REPL, and Helm charts alongside the server. Most of
what is "missing" here is on purpose, and would be a regression in its context.

## Ideas that shaped this design

### Row cap with truncation signal

`mcp-trino`
[stops iterating at a configured limit](https://github.com/tuannvm/mcp-trino/blob/3e0be007e203057318a098c5b846ba881e8f427c/internal/trino/client.go#L379-L437),
closes the rows to end server-side streaming, and reports `Truncated: true` alongside the
results. Neither Python implementation caps at all — both
[`fetchall()`](https://github.com/alaturqua/mcp-trino-python/blob/ea1300e228f8a95eea38c9804487614bbf7e78a4/src/trino_client.py#L77-L84)
unbounded.

### sqlglot AST read-only detection

`trino-mcp`
[checks statements with sqlglot](https://github.com/weijie-tan3/trino-mcp/blob/f32b6f564242b66b46813a91321c770e08c8fef5/src/trino_mcp/utils.py#L42-L89)
and inspects the syntax tree. It is the right call: keyword matching has to special-case
every read-only statement containing a write keyword (`SHOW CREATE TABLE`).

### Query timeout with server-side cancellation

`trino-mcp`'s
[timeout handling](https://github.com/weijie-tan3/trino-mcp/blob/f32b6f564242b66b46813a91321c770e08c8fef5/src/trino_mcp/client.py#L117-L214)
covers a subtlety: `cursor.cancel()` does nothing while `_next_uri` is unset — the state
of a query still in its opening request, which is exactly when a timeout fires. A direct
`DELETE /v1/query/{id}` is the reliable path.

### Output file

`trino-mcp` offers
[an output path on its query tool](https://github.com/weijie-tan3/trino-mcp/blob/f32b6f564242b66b46813a91321c770e08c8fef5/src/trino_mcp/server.py#L325-L367),
returning a row count only, keeping large result sets out of the context.

### `explain_query` tool

Only `mcp-trino` has one. Cheap to provide, and it lets a model check an expensive query
before running it.

## Edge cases these implementations surfaced

### Multi-statement bypass in `parse_one`

`trino-mcp`'s guard calls
[`sqlglot.parse_one()`](https://github.com/weijie-tan3/trino-mcp/blob/f32b6f564242b66b46813a91321c770e08c8fef5/src/trino_mcp/utils.py#L57-L63),
which returns the first statement and discards the rest, so only the first is inspected:

```sql
SELECT * FROM t; DROP TABLE t   -- reads as read-only
```

Found while building a case matrix for this project's own guard. Here `sqlglot.parse()` is
used and anything that is not exactly one statement is rejected
([`src/trino_mcp/sql.py`](../src/trino_mcp/sql.py)), with a regression test.

`mcp-trino` sidesteps the class by
[refusing any query containing `;`](https://github.com/tuannvm/mcp-trino/blob/3e0be007e203057318a098c5b846ba881e8f427c/internal/trino/client.go#L206-L210),
at the cost of rejecting a harmless trailing semicolon. This project does both: normalise
the trailing `;` away, then reject genuine multi-statement input.

### No read-only enforcement

`mcp-trino-python` does not attempt it. Its
[`execute_query`](https://github.com/alaturqua/mcp-trino-python/blob/ea1300e228f8a95eea38c9804487614bbf7e78a4/src/trino_client.py#L67-L84)
runs whatever it is given, and `optimize` and `expire_snapshots` are exposed
unconditionally — consistent with a table-maintenance server rather than an analytics one.
Worth knowing before pointing it at production.

### Unquoted identifiers

All three interpolate tool arguments straight into metadata queries, e.g.
[`f"SHOW SCHEMAS FROM {catalog}"`](https://github.com/alaturqua/mcp-trino-python/blob/ea1300e228f8a95eea38c9804487614bbf7e78a4/src/trino_client.py#L109-L127).
Catalog, schema, and table names come from the model, so a metadata tool can run arbitrary
SQL, bypassing whatever guard sits on the query tool.

This project quotes every identifier and doubles embedded quotes
([`src/trino_mcp/client.py`](../src/trino_mcp/client.py)), with a test asserting
`a"; DROP TABLE x --` cannot escape.

### Retry scope on non-idempotent statements

`trino-mcp`
[catches bare `Exception`](https://github.com/weijie-tan3/trino-mcp/blob/f32b6f564242b66b46813a91321c770e08c8fef5/src/trino_mcp/client.py#L100-L110)
around `cursor.execute()` and retries once on a fresh connection, treating a
`TrinoQueryError` — the cluster understood the statement and rejected it — the same as a
dropped socket. A failing write can run twice.

Here the retry covers transport failures only; `TrinoQueryError` propagates immediately.

### `Field()` as a parameter default

`trino-mcp` writes
[`catalog: str = Field(description=...)`](https://github.com/weijie-tan3/trino-mcp/blob/f32b6f564242b66b46813a91321c770e08c8fef5/src/trino_mcp/server.py#L291-L300).
FastMCP's introspection handles it, but the `FieldInfo` object is the runtime default, so
calling the function directly hands it an object where a string is expected.
`Annotated[str, Field(...)]` is used here instead.

### Top-level module namespace

`mcp-trino-python` ships
[`packages = ["src"]` with `mcp-trino = "server:main"`](https://github.com/alaturqua/mcp-trino-python/blob/ea1300e228f8a95eea38c9804487614bbf7e78a4/pyproject.toml),
installing `server`, `config`, and `trino_client` as top-level modules, where another
distribution claiming those names would collide. Its modules also import each other flatly
(`from config import TrinoConfig`), so it is not importable as a library.

## Scope left out on purpose

Capability these servers carry that this one does not need:

- **Iceberg maintenance tooling** (`optimize`, `optimize_manifests`, `expire_snapshots`,
  `show_manifests`, `show_partitions`, `show_files`, `show_entries`, `show_refs`,
  `show_metadata_log_entries`) — 15 of `mcp-trino-python`'s 22 tools.
- **OAuth2 / JWT / Azure SPN / impersonation / allowlists** — for shared deployments;
  unnecessary for a single user with password auth.
- **HTTP and SSE transports, Helm charts, the CLI/REPL** — stdio only here.
- **`show_catalog_tree`** — issues 1 + N + N×M queries to build one string.
- **A CLI argument layer** — `trino-mcp` mirrors every environment variable as an argparse
  flag. Harnesses pass environment variables, and a password on a command line is visible
  in `ps`.
- **Query watermark comments** — `trino-mcp`
  [prepends a JSON comment to every statement](https://github.com/weijie-tan3/trino-mcp/blob/f32b6f564242b66b46813a91321c770e08c8fef5/src/trino_mcp/client.py#L54-L70)
  for attribution. The `source` connection parameter already lands in
  `system.runtime.queries.source`.
- **Two query tools** — `trino-mcp` exposes both `execute_query` and
  `execute_query_read_only`. One tool here, gated by `TRINO_ALLOW_WRITES`.

## Where this differs by choice

Trade-offs, not corrections.

- **Columnar results.** All three return row dicts, repeating column names on every row.
  This returns `{"columns": [...], "rows": [[...]]}` — cheaper on wide results, less
  self-describing. (`mcp-trino`'s docs show columnar output; its code returns row dicts.)
- **Lazy connection.** The cluster is contacted on first tool use rather than at startup,
  so a brief outage does not stop the server starting. Connection errors surface per call
  instead of once, up front.
- **No module-global mutable state.** `trino-mcp`
  [initialises `config` and `client` to `None` at import](https://github.com/weijie-tan3/trino-mcp/blob/f32b6f564242b66b46813a91321c770e08c8fef5/src/trino_mcp/server.py#L24-L34)
  and populates them in `main()`; this project passes the client into `create_server()`,
  so tests hand in a fake rather than patching module state.
- **Output paths are confined.** `trino-mcp` writes wherever the tool call asks. Here the
  path resolves under `TRINO_OUTPUT_DIR` and anything escaping it is refused — query
  results are untrusted input.
- **File export ignores the row cap.** Exporting exists to move a whole result set; rows
  stream in batches instead.
