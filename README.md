# trino-mcp

An MCP server for querying [Trino](https://trino.io) from an AI agent harness.

## Tools

| Tool             | Description                                                   |
| ---------------- | ------------------------------------------------------------- |
| `list_catalogs`  | List the catalogs in the cluster                              |
| `list_schemas`   | List the schemas in a catalog, optionally filtered by pattern |
| `list_tables`    | List the tables and views in a schema, likewise filterable    |
| `search_columns` | Find which tables contain a column matching a pattern         |
| `describe_table` | Show a table's columns, types, and comments                   |
| `profile_table`  | Report a table's size, partitioning, freshness, and comment   |
| `execute_query`  | Run a SQL query, returning rows or writing them to a file     |
| `explain_query`  | Show how Trino would execute a query, without running it      |

## Configuration

All settings come from the environment and are validated at startup.

| Variable                      | Default         | Notes                           |
| ----------------------------- | --------------- | ------------------------------- |
| `TRINO_HOST`                  | `localhost`     |                                 |
| `TRINO_PORT`                  | `8443` / `8080` | follows `TRINO_HTTP_SCHEME`     |
| `TRINO_USER`                  | **required**    | your Trino account              |
| `TRINO_PASSWORD`              | —               | enables basic auth              |
| `TRINO_HTTP_SCHEME`           | `https`         |                                 |
| `TRINO_CATALOG`               | —               | default catalog                 |
| `TRINO_SCHEMA`                | —               | default schema                  |
| `TRINO_MAX_ROWS`              | `1000`          | `0` disables the cap            |
| `TRINO_QUERY_TIMEOUT_MINUTES` | `5`             | `0` disables the timeout        |
| `TRINO_ALLOW_WRITES`          | `false`         |                                 |
| `TRINO_OUTPUT_DIR`            | `.output`       | exports land here; auto-created |

A password requires `https`; pairing it with `TRINO_HTTP_SCHEME=http` is refused at
startup rather than sent in cleartext.

## Setup

No install step; `uvx` fetches and runs it. Requires [uv](https://docs.astral.sh/uv/) and
Python 3.13+.

```bash
uvx --from git+https://github.com/lqmanh/trino-mcp trino-mcp
```

### Claude Code

```bash
claude mcp add trino --env TRINO_HOST=warehouse.example.com --env TRINO_USER=you -- uvx --from git+https://github.com/lqmanh/trino-mcp trino-mcp
```

Or commit a `.mcp.json` that reads the password from your environment:

```json
{
  "mcpServers": {
    "trino": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/lqmanh/trino-mcp", "trino-mcp"],
      "env": {
        "TRINO_HOST": "warehouse.example.com",
        "TRINO_USER": "you",
        "TRINO_PASSWORD": "${TRINO_PASSWORD}",
        "TRINO_CATALOG": "hive"
      }
    }
  }
}
```

### Codex CLI

In `~/.codex/config.toml`:

```toml
[mcp_servers.trino]
command = "uvx"
args = ["--from", "git+https://github.com/lqmanh/trino-mcp", "trino-mcp"]
env = { TRINO_HOST = "warehouse.example.com", TRINO_USER = "you", TRINO_CATALOG = "hive" }
```

> The ChatGPT app is a different story: its connectors need a **remote** MCP server over
> HTTP. This server speaks stdio only, so it works with Codex CLI (and other local agents)
> but cannot be added to ChatGPT directly.

### OpenCode

In `opencode.json`:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "trino": {
      "type": "local",
      "command": [
        "uvx",
        "--from",
        "git+https://github.com/lqmanh/trino-mcp",
        "trino-mcp"
      ],
      "enabled": true,
      "environment": {
        "TRINO_HOST": "warehouse.example.com",
        "TRINO_USER": "you",
        "TRINO_CATALOG": "hive"
      }
    }
  }
}
```

## Development

```bash
uv sync
uv run pytest
uv run ruff check && uv run ruff format --check && uv run ty check
```

`compose.yml` brings up a throwaway Trino — Iceberg over MinIO, a Postgres source, and
`tpch` — so nothing needs a real cluster. `uv run pytest -m e2e` starts its own copy on a
random port, seeds it, and tears it down; `docker compose up -d` gives you one on
`localhost:8080` to keep. [docs/evaluation.md](docs/evaluation.md) uses it to check how
well an agent actually drives the tools.

When verifying local changes through `uvx`, pass `--no-cache` — uv will otherwise reuse a
previously built wheel for the same version and you'll be testing stale code.

```bash
uvx --no-cache --from . trino-mcp
```

## Prior art

Several Trino MCP servers came before this one, and reading them shaped a good deal of the
design here — particularly around result limits, read-only enforcement, and query
cancellation:

- [tuannvm/mcp-trino](https://github.com/tuannvm/mcp-trino)
- [alaturqua/mcp-trino-python](https://github.com/alaturqua/mcp-trino-python)
- [weijie-tan3/trino-mcp](https://github.com/weijie-tan3/trino-mcp)

This is an independent implementation rather than a direct fork of any of them.
[docs/comparisons.md](docs/comparisons.md) credits the specific ideas each one
contributed, and sets out where this project deliberately goes its own way.

## License

MIT
