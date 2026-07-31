# Agent Guidelines

This file provides guidance to AI agent harnesses when working with code in this
repository.

## Commands

```bash
uv sync                                              # install, including dev group
uv run pytest                                        # full suite, offline, ~2s
uv run pytest tests/test_client.py -k truncation     # one file / one pattern
uv run pytest "tests/test_sql.py::test_reads_are_allowed[SHOW CATALOGS]"
uv run ruff check && uv run ruff format --check && uv run ty check
npx prettier --write README.md docs/                 # markdown only
```

`ty` is the type checker. Editor Pyright, if enabled, reports false import errors and
"union syntax requires 3.10+" — it is not pointed at the venv and its output is noise.

### Verifying through uvx

`uvx` is the delivery path
(`uvx --from git+https://github.com/lqmanh/trino-mcp trino-mcp`), so it is also the
end-to-end test. **Pass `--no-cache`**: uv caches built wheels by version, the version
string rarely changes, and `--refresh-package` does not invalidate it. A stale wheel
silently serves the previous build.

```bash
uvx --no-cache --from . trino-mcp
```

Design assertions so a stale build fails loudly. Asserting a tool call returned `is_error`
proves nothing — an unknown tool name errors identically. Assert on the tool list or on
specific error text.

## Architecture

Four modules, layered bottom-up, each importing only from those below it:

| Module      | Role                                                              |
| ----------- | ----------------------------------------------------------------- |
| `config.py` | Frozen pydantic `Config`, built once by `from_env()` at startup   |
| `sql.py`    | `is_read_only()` — sqlglot AST guard; `normalize()`               |
| `client.py` | Blocking Trino access: caps, deadlines, cancellation, file export |
| `server.py` | MCP tool definitions, `create_server()`, `main()`                 |

`__init__.py` is a facade: it re-exports the public names and nothing else. `__version__`
is defined in `server.py` and re-exported from there, not the reverse — the package root
importing a submodule that imports back is the cycle this layout avoids.

**`create_server(client, config)` takes the client as an argument.** There is no
module-level mutable state, which is what lets every server test hand in a fake instead of
patching globals. Keep it that way.

**The client is synchronous; the server is async.** Tools call the inner `call()` helper,
which runs the blocking method via `anyio.to_thread.run_sync` and converts any exception
into a `ToolError`. Client code must never become async.

**The connection is lazy.** `TrinoClient._connect()` fires on first tool use, not at
startup, so the process starts and answers `list_tools` while the cluster is down.

### Invariants worth knowing before editing

- **Read-only guard.** `sql.py` uses `sqlglot.parse()` and rejects anything that is not
  exactly one statement — `parse_one()` would silently inspect only the first, letting
  `SELECT 1; DROP TABLE t` through. Parse failure fails closed. This is a security
  boundary; changes need a case in `test_sql.py`'s matrix.
- **Identifier quoting.** Catalog/schema/table names arrive as tool arguments and are
  untrusted. Every metadata query routes through `qualify()` / `quote_ident()`. An
  f-string interpolating a name directly turns `list_tables` into arbitrary SQL execution.
- **The row cap fetches `cap + 1`.** That extra row is the only way to distinguish
  "exactly at the cap" from "there is more", which decides the `truncated` flag. A partial
  result the model reads as complete is a correctness bug.
- **The timeout deadline covers execute _and_ consume.** Trino's `execute` returns once
  the first page is ready while the query keeps running. On expiry the query is cancelled
  server-side: `cursor.cancel()` no-ops while `_next_uri` is unset, so a
  `DELETE /v1/query/{id}` fallback follows.
- **Retry is transport-only, and read-only.** `TrinoQueryError` means the cluster
  understood the statement and rejected it. A transport failure means less than it looks:
  Trino keeps executing after the client socket drops, so a retried write can land twice.
  `_attempt` re-runs a statement only when `is_read_only()` says repeating it is harmless
  — note that this holds regardless of `TRINO_ALLOW_WRITES`, which is set in the harness
  config and can change without anyone revisiting this code.
- **The password must never reach a log line.** `Config.password` is a `SecretStr`, so it
  masks in reprs, f-strings, and `model_dump()`, and reading it takes an explicit
  `get_secret_value()` — that is why the whole config is safe to log. `SecretStr` does
  **not** cover a `ValidationError`: pydantic reports the offending input, which here is
  the raw environment dict, so `main()` formats config errors from
  `exc.errors(include_input=False)`. Both halves are load-bearing; the mask is not a
  reason to drop the second. A password is also refused over `TRINO_HTTP_SCHEME=http`,
  because the trino client sends BasicAuthentication over plain http without objecting.
- **The model never picks an output path.** `execute_query` takes `output_file_format`
  (`csv`/`json`) and the server names the file `<timestamp>-<slug>.<ext>` under
  `TRINO_OUTPUT_DIR`. An earlier revision took a model-supplied path and confined it to
  that directory, which could not stop `output_file="README.md"` — the jail's own root
  defaulted to the working tree. The format is also passed down to `_drain_to_file` rather
  than sniffed from the suffix, so the extension always matches the bytes.
- **Exports drain to `.<name>.partial` and are renamed only on success.** That name is the
  reason a failed export leaves nothing: it is server-generated and never reaches the
  caller on the error path, so nobody downstream could clean up an orphan. Unlinking the
  temp also defuses the thread a timeout abandons — it writes on into a removed inode
  instead of growing a file under the name the caller was told about.
- **Logs go to stderr.** Under stdio transport stdout carries JSON-RPC framing; a stray
  `print` desynchronises the client.

### MCP 2.0 specifics

Python types are snake_case (`tool.input_schema`, `result.structured_content`,
`result.is_error`) while the wire format stays camelCase. Returning a pydantic model from
a tool auto-generates an output schema _and_ emits the payload twice — once as structured
content, once as pretty-printed text. `execute_query` is therefore the one tool declared
`@server.tool(structured_output=False)`, returning a `TextContent` of compact JSON,
because its output is the unbounded one.

## Testing

The suite runs offline; nothing reaches a cluster. `anyio` provides the async plugin
(**not** `pytest-asyncio`) — async tests need `pytestmark = pytest.mark.anyio`.

Fakes live in `conftest.py`. `StubbedClient` overrides `_connect()` rather than assigning
`_connection`, so reconnection — and therefore the retry path — stays exercisable.
`FakeTrino` supplies canned results to the server layer. `test_stdio.py` spawns the real
console script; note that MCP clients forward only a fixed allowlist (`HOME`, `LOGNAME`,
`PATH`, `SHELL`, `TERM`, `USER`) to a stdio server, so `TRINO_*` must be passed
explicitly.

Tests cover behaviour this project owns. Pydantic's constraint handling, sqlglot's parser,
and library defaults are not re-tested, and cases that exercise a branch another test
already covers get folded in rather than added. When claiming a test guards something,
verify it by mutating the source and watching it fail.

## Conventions

- PEP 695 generics (`async def call[T](...)`) rather than `TypeVar`.
- **Comment sparingly.** Write one when the code cannot speak for itself: a non-obvious
  _why_, an edge case, a library quirk, a decision that looks wrong until you know the
  reason. Several of the ones here encode findings that cost real debugging. Skip the rest
  — a docstring that restates the function name, or a comment narrating the line below it,
  is upkeep with no reader.
- **MCP-facing docstrings are the exception, because they are API.** A tool function's
  docstring becomes the tool description sent to the model, and `Field(description=...)`
  becomes its schema. Write those for the agent that consumes them: what the tool is for,
  when to reach for it, what the response shape means. Length is justified there in a way
  it is not elsewhere.
- **Declare every runtime import in `[project.dependencies]`**, including packages `mcp`
  already pulls in. No lockfile reaches users, so relying on a transitive dependency means
  relying on `mcp` never dropping it.
- `docs/comparisons.md` cites three reference implementations via SHA-pinned GitHub
  permalinks so they survive upstream code movement.
