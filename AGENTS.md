# Agent Guidelines

## Commands

```bash
uv sync                                            # install, including dev group
uv run pytest                                      # full suite
uv run pytest tests/test_client.py -k truncation   # one file / one pattern
uv run pytest "tests/test_sql.py::test_reads_are_allowed[SHOW CATALOGS]"
uv run ruff check && uv run ruff format --check && uv run ty check
npx prettier --write .
```

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

Five modules, layered bottom-up, each importing only from those below it:

| Module      | Role                                                            |
| ----------- | --------------------------------------------------------------- |
| `config.py` | Frozen pydantic `Config`, built once by `from_env()` at startup |
| `sql.py`    | `is_read_only()` — sqlglot AST guard; `normalize()`             |
| `export.py` | Rows to csv/json/parquet on disk; owns `OutputFormat`           |
| `client.py` | Blocking Trino access: caps, deadlines, cancellation            |
| `server.py` | MCP tool definitions, `create_server()`, `main()`               |

`export.py` imports nothing from this package. It owns Trino _type_ semantics — the
sqlglot dialect, and the Python shape each type arrives in — while `client.py` owns Trino
_cursor_ semantics. Its `Batches` protocol asks only for `fetchmany`, which is what keeps
`description` and its py.typed inaccuracies on the client side.

`__init__.py` is a facade. `__version__` is defined in `server.py` and re-exported from
there, not the reverse — the package root importing a submodule that imports back is the
cycle this layout avoids.

**`create_server(client, config)` takes the client as an argument.** There is no
module-level mutable state, which is what lets every server test hand in a fake instead of
patching globals. Keep it that way.

**The client is synchronous; the server is asynchronous.** Tools reach it through the
inner `call()` helper. Client code must never become async.

**The connection is lazy**, so the process starts and answers `list_tools` while the
cluster is down.

### Boundaries

`sql.py` decides what may run and `client.py` quotes every identifier that reaches a
statement. Both are security boundaries: a change to the read-only guard needs a case in
`test_sql.py`'s matrix, and a metadata query that interpolates a name instead of routing
through `qualify()` turns a tool argument into arbitrary SQL.

### Decisions that look wrong until you know why

- **Retry ignores `TRINO_ALLOW_WRITES`.** A statement is re-run after a transport failure
  only when `is_read_only()` says repeating it is harmless, whatever the flag says — it
  lives in harness config and can change without anyone revisiting this code.
- **The model never picks an output path.** It chooses a format; the server names the file
  under `TRINO_OUTPUT_DIR`. Confining a model-supplied path to that directory was tried
  and could not stop `output_file="README.md"`, because the jail's own root defaulted to
  the working tree.
- **Logs go to stderr**, because stdout carries the JSON-RPC framing.

## Testing

The suite runs offline; nothing reaches a cluster. `anyio` provides the async plugin
(**not** `pytest-asyncio`) — async tests need `pytestmark = pytest.mark.anyio`.

Fakes live in `conftest.py`, one per layer: `Rows` is a bare `export.Batches`,
`FakeCursor` stands in for the DB-API, `StubbedClient` dials it instead of a cluster
(overriding `_connect()`, so reconnection and the retry path stay exercisable), and
`FakeTrino` hands canned results to the server layer. Reach for the lowest one that covers
the behavior — `FakeTrino` replaces the client wholesale, so a test written against it
proves nothing about `client.py`.

Tests cover behavior this project owns. Pydantic's constraint handling, sqlglot's parser,
and library defaults are not re-tested, and cases that exercise a branch another test
already covers get folded in rather than added. When claiming a test guards something,
verify it by mutating the source and watching it fail.

## Conventions

- PEP 695 generics (`async def call[T](...)`) rather than `TypeVar`.
- **Concision is the default, and it applies to everything** — comments, docstrings, copy,
  commit messages, plans, and chat responses alike. Say it once, in the fewest words that
  carry the whole meaning. Cut preamble, restatement, and hedging; if deleting a sentence
  costs the reader nothing, it was not earning its place. Answer what was asked before
  volunteering what was not.
- **Keep implementation detail in the implementation.** A mechanism explained here is a
  second copy that drifts the moment the code moves, and nothing fails when it does. This
  file carries layering, constraints, and decisions; how a function achieves them belongs
  in a comment beside it.
- **Avoid unnecessary comments & docstrings.** Write one only when the code cannot speak
  for itself: a non-obvious _why_, an edge case, a library quirk, a decision that looks
  wrong until you know the reason. Several of the ones here encode findings that cost real
  debugging. Skip the rest — a docstring that restates the function name, or a comment
  narrating the line below it, is upkeep with no reader.
- **MCP-facing docstrings are the exception, because they are API.** A tool function's
  docstring becomes the tool description sent to the model, and `Field(description=...)`
  becomes its schema. Write those for the agent that consumes them: what the tool is for,
  when to reach for it, what the response shape means. Length is justified there in a way
  it is not elsewhere.
- **Declare every runtime import in `[project.dependencies]`**, including packages `mcp`
  already pulls in. No lockfile reaches users, so relying on a transitive dependency means
  relying on `mcp` never dropping it.
