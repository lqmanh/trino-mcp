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

|                    | mcp-trino                                   | mcp-trino-python | trino-mcp                   | **this**         |
| ------------------ | ------------------------------------------- | ---------------- | --------------------------- | ---------------- |
| Language           | Go                                          | Python           | Python                      | Python           |
| Tools              | 6                                           | 22               | 8                           | 8                |
| Read-only guard    | regex + keywords                            | no               | sqlglot AST                 | sqlglot AST      |
| Row cap            | yes                                         | no               | no                          | yes              |
| Query timeout      | yes, cancels                                | no               | yes, cancels                | yes, cancels     |
| Identifier quoting | no                                          | no               | no                          | **yes**          |
| Result shape       | row dicts                                   | row dicts        | row dicts                   | **columnar**     |
| File export        | no                                          | no               | yes                         | yes              |
| Auth               | password, OAuth2, JWT, Azure, impersonation | password         | password, OAuth2, Azure SPN | password         |
| Transport          | stdio, HTTP, SSE                            | stdio, HTTP, SSE | stdio                       | stdio            |
| Runs as            | shared service, multi-user                  | local subprocess | local subprocess            | local subprocess |
| Extras             | CLI, REPL, Helm chart                       | —                | —                           | —                |

## Scope left out on purpose

Capability these servers carry that this one does not need:

- **Table maintenance tools** — For write operations (`optimize`, `optimize_manifests`,
  `expire_snapshots`), `execute_query` already runs them when `TRINO_ALLOW_WRITES=true`,
  so a tool apiece would only shorten the name. It would also spread the write gate: today
  `execute_query` is the single place that checks the flag, so each write tool added is
  another one that has to remember. For read-only inspectors (`show_manifests`,
  `show_partitions`, `show_files`, `show_entries`, `show_refs`,
  `show_metadata_log_entries`), `profile_table` covers what they are usually reached for.
- **OAuth2 / JWT / Azure SPN / impersonation / allowlists**
- **HTTP and SSE transports, Helm charts, the CLI/REPL**
- **`show_catalog_tree`** — This issues 1 + N + N×M queries to build one string.
- **A CLI argument layer** — `trino-mcp` mirrors every environment variable as an argparse
  flag. Harnesses pass environment variables, and a password on a command line is visible
  in `ps`.
- **Query watermark comments** — `trino-mcp`
  [prepends a JSON comment to every statement](https://github.com/weijie-tan3/trino-mcp/blob/f32b6f564242b66b46813a91321c770e08c8fef5/src/trino_mcp/client.py#L54-L70)
  for attribution. The `source` connection parameter already lands in
  `system.runtime.queries.source`.
- **Two separate tools for read and write** — `trino-mcp` exposes both `execute_query` and
  `execute_query_read_only`. One tool here, gated by `TRINO_ALLOW_WRITES`.

## Where this differs by choice

Trade-offs, not corrections.

- **One profile instead of a shelf of inspectors** — `mcp-trino-python` exposes each
  metadata table as its own tool. `profile_table` reads what the connector supports in one
  call and answers the question they exist to serve — which column prunes partitions, how
  big, how fresh — rather than handing back `$manifests` for the model to interpret. A
  connector with none of them still returns statistics plus a note saying what was
  skipped. The cost is that raw metadata tables are only reachable through
  `execute_query`.
- **Columnar results** — All three return row dicts, repeating column names on every row.
  This returns `{"columns": [...], "rows": [[...]]}` — cheaper on wide results, less
  self-describing. (`mcp-trino`'s docs show columnar output; its code returns row dicts.)
- **Lazy connection** — The cluster is contacted on first tool use rather than at startup,
  so a brief outage does not stop the server starting. Connection errors surface per call
  instead of once, up front.
- **No module-global mutable state** — `trino-mcp`
  [initializes `config` and `client` to `None` at import](https://github.com/weijie-tan3/trino-mcp/blob/f32b6f564242b66b46813a91321c770e08c8fef5/src/trino_mcp/server.py#L24-L34)
  and populates them in `main()`; this project passes the client into `create_server()`,
  so tests hand in a fake rather than patching module state.
