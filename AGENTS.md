# Agent Guidelines

## Commands

```bash
uv sync                                            # install, including dev group
uv run pytest                                      # full suite, offline
uv run pytest tests/test_client.py -k truncation   # one file / one pattern
uv run pytest "tests/test_sql.py::test_reads_are_allowed[SHOW CATALOGS]"
uv run pytest -m e2e                               # against a real Trino; needs docker
uv run ruff check && uv run ruff format --check && uv run ty check
uv run scripts/schema.py                           # regenerate schemas/; test_manifest enforces it
npx prettier --write .
```

### Verifying through uvx

`uvx` is the delivery path
(`uvx --from git+https://github.com/lqmanh/trino-mcp trino-mcp`), so it is also the
end-to-end test. **Pass `--no-cache`**: uv caches wheels by version, the version string
rarely changes, and `--refresh-package` does not invalidate it — a stale wheel silently
serves the previous build.

```bash
uvx --no-cache --from . trino-mcp
```

Design assertions so a stale build fails loudly. `is_error` proves nothing: an unknown
tool name errors identically. Assert on the tool list or on specific error text.

## Architecture

Six modules, layered bottom-up, each importing only from those below it:

| Module        | Role                                                            |
| ------------- | --------------------------------------------------------------- |
| `config.py`   | Frozen pydantic `Config`, built once by `from_env()` at startup |
| `sql.py`      | sqlglot: the read-only guard, plus reading DDL                  |
| `export.py`   | Rows to csv/json/parquet on disk; owns `OutputFormat`           |
| `manifest.py` | The static table facts Trino cannot report; owns their schema   |
| `client.py`   | Blocking Trino access: caps, deadlines, cancellation            |
| `server.py`   | MCP tool definitions, `create_server()`, `main()`               |

`export.py` and `manifest.py` import nothing from this package.

`export.py` owns Trino _type_ semantics — the sqlglot dialect, and the Python shape each
type arrives in — while `client.py` owns _cursor_ semantics. Its `Batches` protocol asks
only for `fetchmany`, keeping `description` and its py.typed inaccuracies on the client
side.

`manifest.py` must not learn what generates a manifest: it defines the format, a pipeline
elsewhere emits it. `schemas/manifest.v1.json` publishes that format for producer authors
and is generated from the pydantic models by `scripts/schema.py` — regenerate it in the
same commit as any change to them, which `test_manifest.py` enforces.

`__init__.py` is a facade. `__version__` lives in `server.py` and is re-exported, not the
reverse — the package root importing a submodule that imports back is the cycle this
avoids.

**`create_server(client, config)` takes the client as an argument.** No module-level
mutable state, which is what lets every server test hand in a fake instead of patching
globals. Keep it that way.

**The client is synchronous; the server is asynchronous.** Tools reach it through the
inner `call()` helper. Client code must never become async.

**The connection is lazy**, so the process starts and answers `list_tools` while the
cluster is down.

### Boundaries

`sql.py` decides what may run; `client.py` quotes every identifier reaching a statement.
Both are security boundaries: a change to the read-only guard needs a case in
`test_sql.py`'s matrix, and a metadata query interpolating a name instead of routing
through `qualify()` turns a tool argument into arbitrary SQL.

The other line runs between Trino and whichever cluster is in front of it. **Branch on
Trino-level facts, never on a deployment's conventions.** A connector name is registered
by the plugin, so `iceberg` and `delta_lake` are the same strings everywhere and
special-casing one supports a standard. Retention windows and naming conventions are
whoever set that cluster up: not a branch, not a default, not a docstring, not a test
fixture.

### Decisions that look wrong until you know why

- **`explain_table` is separate from `describe_table`, and only exists with a manifest.**
  Folding it in would solve discovery for free — the real argument for it. Against:
  `describe_table` reports live cluster truth, and mixing generated data in leaves the
  model unable to tell which half is verified. Conditional registration is the other half:
  a tool's description ships in every session's prompt whether or not it is called, so a
  deployment without a manifest pays nothing.
- **A manifest with an unknown `version` is refused, not partially read.** Every field is
  optional, so a future format whose meanings have shifted validates cleanly and then
  answers confidently and wrongly.
- **An empty manifest is dropped with a warning, not refused.** A server that might answer
  _wrongly_ must not start; one with nothing to say reports the gap. Refusing would also
  take the other tools down over an optional one. The warning is the whole mitigation —
  without it the tool silently ceases to exist for whoever configured a manifest.
- **Retry ignores `TRINO_ALLOW_WRITES`.** A statement is re-run after a transport failure
  only when `is_read_only()` says repeating it is harmless, whatever the flag says: it
  lives in harness config and can change without anyone revisiting this code.
- **The model never picks an output path.** It chooses a format; the server names the file
  under `TRINO_OUTPUT_DIR`. Confining a model-supplied path to that directory was tried
  and could not stop `output_file="README.md"`, because the jail's own root defaulted to
  the working tree.
- **Logs go to stderr**, because stdout carries the JSON-RPC framing.

## Testing

The default run is offline. `anyio` provides the async plugin (**not** `pytest-asyncio`),
so async tests need `pytestmark = pytest.mark.anyio`.

`tests/e2e/` is the exception, marked `e2e` and deselected by `addopts`. It exists for
what a fake cannot check: every fake was written from the same reading of the docs as the
code it stands in for, so a misread survives the offline suite intact — the `SHOW STATS`
column names and the `SHOW CREATE TABLE` rendering both reached it that way. Its fixture
always builds a throwaway stack and never adopts a cluster that happens to be listening: a
reachable port is not an identity check, and these tests seed tables. `docs/evaluation.md`
covers the manual pass over the same stack.

Fakes live in `conftest.py`, one per layer: `Rows` is a bare `export.Batches`,
`FakeCursor` stands in for the DB-API, `StubbedClient` dials it instead of a cluster
(overriding `_connect()`, so reconnection and retry stay exercisable), and `FakeTrino`
hands canned results to the server layer. Reach for the lowest one that covers the
behavior — `FakeTrino` replaces the client wholesale, so a test written against it proves
nothing about `client.py`.

Test what could plausibly break, not everything that could be asserted. A test earns its
place by catching a regression that would not otherwise be obvious: a boundary, a race, a
library quirk, a silently wrong answer. Self-evident logic does not need one — a test
restating a helper cannot fail by accident. But "obvious by reading" is not the test;
"obvious when wrong" is. A one-line mapping whose inversion still returns a plausible
answer needs a case; a helper whose breakage is loud does not. Third-party behavior is not
re-tested, and a case exercising an already-covered branch gets folded in rather than
added.

When claiming a test guards something, verify it by mutating the source and watching it
fail.

## Conventions

- PEP 695 generics (`async def call[T](...)`) rather than `TypeVar`.
- **Concision is the default, everywhere** — comments, docstrings, copy, commit messages,
  plans, and chat responses alike. Say it once, in the fewest words carrying the whole
  meaning. Cut preamble, restatement, and hedging; a sentence whose deletion costs the
  reader nothing was not earning its place. Answer what was asked before volunteering
  more.
- **Keep implementation detail in the implementation.** A mechanism explained here is a
  second copy that drifts the moment the code moves, and nothing fails when it does. This
  file carries layering, constraints, and decisions; how a function achieves them belongs
  in a comment beside it.
- **Avoid unnecessary comments & docstrings.** Write one only when the code cannot speak
  for itself: a non-obvious _why_, an edge case, a library quirk, a decision that looks
  wrong until you know the reason — several here encode findings that cost real debugging.
  A docstring restating the function name, or a comment narrating the line below it, is
  upkeep with no reader.
- **MCP-facing docstrings are the exception, because they are API.** A tool function's
  docstring becomes the description sent to the model, `Field(description=...)` its
  schema. Write both for the agent consuming them: what the tool is for, when to reach for
  it, what the response means. Length is justified where it changes what the model does,
  but every word ships in every session's prompt — a lesson repeated in a docstring and
  two field descriptions costs three times.
- **Declare every runtime import in `[project.dependencies]`**, including packages `mcp`
  already pulls in. No lockfile reaches users, so relying on a transitive dependency means
  relying on `mcp` never dropping it.
