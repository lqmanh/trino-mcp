import logging
import shutil
import threading
from concurrent.futures import Future, wait
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import trino
from trino.dbapi import ColumnDescription, Connection, Cursor
from trino.exceptions import TrinoQueryError

from . import export
from .config import Config
from .export import OutputFormat
from .sql import is_read_only, normalize

log = logging.getLogger(__name__)


class QueryTimeout(Exception):
    """A query exceeded the configured deadline and was canceled."""


class QueryRejected(Exception):
    """Trino understood the statement and refused it."""


@dataclass(frozen=True, slots=True)
class Names:
    names: list[str]
    truncated: bool


@dataclass(frozen=True, slots=True)
class QueryResult:
    # Rows stay in the list-of-lists shape the Trino DB-API hands back, so they
    # reach json.dumps without a per-row rebuild.
    columns: list[str]
    rows: list[list[Any]]
    truncated: bool

    @property
    def row_count(self) -> int:
        return len(self.rows)


def quote_ident(identifier: str) -> str:
    """Quote a Trino identifier so it cannot terminate the surrounding statement.

    Catalog, schema, and table names arrive as tool arguments, which makes them
    untrusted; interpolating them raw is how ``list_tables`` turns into a way to
    run arbitrary SQL.
    """
    return '"' + identifier.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """Quote a Trino string literal so it cannot terminate the surrounding statement.

    The sibling of ``quote_ident`` for tool arguments that are values rather than
    names: a LIKE pattern reaches a statement by the same route a catalog name
    does, and closing the quote would end it just as effectively.
    """
    return "'" + value.replace("'", "''") + "'"


def qualify(*parts: str) -> str:
    if not parts or not all(parts):
        raise ValueError(
            f"catalog, schema, and table names must all be non-empty, got {parts}; "
            "an empty one would silently address a different object"
        )
    return ".".join(quote_ident(part) for part in parts)


def _like(pattern: str) -> str:
    return f" LIKE {quote_literal(pattern)}" if pattern else ""


def _description(cursor: Cursor) -> list[ColumnDescription]:
    """The current result set's columns, empty when there is not one.

    trino ships ``py.typed`` and declares ``description -> List[ColumnDescription]``,
    but returns None for a query that finished or was canceled before its columns
    arrived — which the timeout and truncation paths both produce.
    """
    return cursor.description or []


class TrinoClient:
    """A lazily-connected Trino client.

    The connection opens on first use rather than at construction so the process
    starts (and answers ``list_tools``) even when the cluster is briefly down.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._connection: Connection | None = None

    # -- connection management -------------------------------------------------

    def _connect(self) -> Connection:
        config = self._config
        auth = (
            trino.auth.BasicAuthentication(
                config.user, config.password.get_secret_value()
            )
            if config.password
            else None
        )
        log.info("connecting to Trino: %s", config)
        return trino.dbapi.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            catalog=config.catalog,
            schema=config.db_schema,
            http_scheme=config.http_scheme,
            auth=auth,
            # Lands in system.runtime.queries.source, so queries from this server
            # are attributable without stamping a comment onto every statement.
            source="trino-mcp",
        )

    def _cursor(self) -> Cursor:
        if self._connection is None:
            self._connection = self._connect()
        return self._connection.cursor()

    def _reset(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                log.debug("closing the stale connection failed", exc_info=True)
        self._connection = None

    def close(self) -> None:
        self._reset()

    # -- execution -------------------------------------------------------------

    def execute(self, query: str) -> QueryResult:
        """Run a statement and return at most ``max_rows`` rows."""
        return self._attempt(self._collect, query)

    def execute_to_file(self, query: str, path: Path, fmt: OutputFormat) -> int:
        """Stream every row to ``path`` in ``fmt`` and return how many were written.

        The row cap is deliberately not applied here. Writing to a file is how a
        caller asks for the whole result set without it passing through the
        model, so capping it would defeat the point; rows are fetched in batches
        so memory stays bounded regardless of result size.

        Rows go to a sibling temp file that is renamed into place only once the
        drain has finished, so ``path`` either holds a complete result or does
        not exist. A sibling scratch directory covers whatever the chosen format
        needs on the way. Both names are generated here and never reach the
        caller, so nothing downstream could clean up after a failure — which is
        why both are removed here, on every path out.
        """
        partial = path.with_name(f".{path.name}.partial")
        workspace = path.with_name(f".{path.name}.workspace")
        workspace.mkdir(exist_ok=True)
        try:
            written = self._attempt(
                lambda cursor: self._drain_to_file(cursor, partial, fmt, workspace),
                query,
            )
        except BaseException:
            # A timeout leaves the worker thread still writing. Unlinking now
            # means it writes on into an already-removed inode, so the rows go
            # nowhere and the space comes back when its handle closes. If the
            # deadline beats the file being opened at all, the worker recreates
            # the temp afterwards: hidden and never renamed, but it does linger.
            with suppress(OSError):
                partial.unlink(missing_ok=True)
            raise
        finally:
            # The scratch directory goes the same way, and an abandoned worker
            # cannot resurrect what it holds: with the directory gone, a staging
            # file it had not opened yet fails to open at all.
            shutil.rmtree(workspace, ignore_errors=True)
        partial.replace(path)
        return written

    def _attempt(self, consume: Any, query: str) -> Any:
        """Execute ``query`` and hand the cursor to ``consume``, under the deadline.

        Retries once on a transport failure, which is usually a connection that
        went stale between calls. A ``TrinoQueryError`` means the cluster
        understood the statement and rejected it, so retrying would just run a
        failing (possibly non-idempotent) statement twice.
        """
        text = normalize(query)
        try:
            return self._guarded(self._cursor(), text, consume)
        except QueryTimeout:
            raise
        except TrinoQueryError as exc:
            # Both branches skip the retry; this one also unwraps a repr that
            # would otherwise reach the model with the message buried in it.
            raise QueryRejected(f"{exc.error_name}: {exc.message}") from exc
        except Exception:
            # Anything reaching here is transport-level. A dropped socket is not
            # evidence the statement did not run: Trino keeps executing after
            # the client goes away, so a retried write can land twice.
            if not is_read_only(text):
                log.warning("not retrying a write statement after a transport failure")
                raise
            log.info("retrying on a fresh connection after a transport failure")
            self._reset()
            return self._guarded(self._cursor(), text, consume)

    def _guarded(self, cursor: Cursor, query: str, consume: Any) -> Any:
        """Run execute+consume, canceling server-side if it overruns.

        Trino's ``execute`` returns once the first page is ready while the query
        keeps running, so the deadline has to cover consumption too — not just
        the call that starts it.
        """
        minutes = self._config.query_timeout_minutes
        if minutes <= 0:
            cursor.execute(query)
            return consume(cursor)

        # A bare Future driven by a daemon thread, not a ThreadPoolExecutor: the
        # executor joins its workers at interpreter exit, so one runaway query
        # would hang shutdown. And wait() rather than result(timeout=...), which
        # cannot tell our deadline from a TimeoutError raised by the query —
        # socket.timeout is exactly that, and trino times requests out at 30s.
        outcome: Future[Any] = Future()

        def work() -> None:
            try:
                cursor.execute(query)
                outcome.set_result(consume(cursor))
            except BaseException as exc:  # noqa: BLE001 - re-raised on the caller's thread
                outcome.set_exception(exc)

        threading.Thread(target=work, daemon=True).start()

        if not wait([outcome], timeout=minutes * 60).done:
            query_id = self._cancel(cursor)
            raise QueryTimeout(
                f"Query exceeded the {minutes:g}-minute timeout and was canceled "
                f"(query_id={query_id or 'unknown'}). Narrow it with WHERE or LIMIT, "
                "avoid SELECT * on large tables, or raise TRINO_QUERY_TIMEOUT_MINUTES."
            )
        return outcome.result()

    def _cancel(self, cursor: Cursor) -> str | None:
        query_id = getattr(cursor, "query_id", None)
        log.warning("canceling query %s after timeout", query_id or "unknown")

        try:
            cursor.cancel()
        except Exception:
            log.debug("cursor.cancel() raised", exc_info=True)

        # cursor.cancel() silently no-ops while _next_uri is unset, which is
        # exactly the state of a query still in its opening request — the case a
        # timeout is most likely to catch. The REST call is the reliable path.
        if query_id:
            config = self._config
            url = (
                f"{config.http_scheme}://{config.host}:{config.port}/v1/query/{query_id}"
            )
            try:
                session = getattr(
                    getattr(cursor, "_request", None), "_http_session", None
                )
                if session is not None:
                    session.delete(url, timeout=5)
                else:
                    log.debug("no HTTP session on the cursor; skipping the REST cancel")
            except Exception:
                log.debug("REST cancel failed", exc_info=True)
        return query_id

    # -- result consumption ----------------------------------------------------

    def _collect(self, cursor: Cursor) -> QueryResult:
        columns = [column[0] for column in _description(cursor)]
        if not columns:
            return QueryResult(columns=[], rows=[], truncated=False)

        cap = self._config.max_rows
        if cap <= 0:
            return QueryResult(columns=columns, rows=cursor.fetchall(), truncated=False)

        # Fetch one extra row: its presence is what distinguishes "exactly at the
        # cap" from "there was more", and the model needs to be told which.
        rows: list[list[Any]] = []
        while len(rows) <= cap:
            batch = cursor.fetchmany(cap + 1 - len(rows))
            if not batch:
                break
            rows.extend(batch)

        truncated = len(rows) > cap
        if truncated:
            del rows[cap:]
            # Stop the cluster streaming pages nobody will read.
            try:
                cursor.cancel()
            except Exception:
                log.debug("canceling after truncation failed", exc_info=True)
            log.warning("result truncated to %d rows (TRINO_MAX_ROWS)", cap)

        return QueryResult(columns=columns, rows=rows, truncated=truncated)

    def _drain_to_file(
        self, cursor: Cursor, path: Path, fmt: OutputFormat, workspace: Path
    ) -> int:
        description = _description(cursor)
        return export.write(
            cursor,
            path,
            fmt,
            [column[0] for column in description],
            # column[1] is `type_code`, annotated int and holding the Trino type
            # name — the same py.typed inaccuracy `_description` documents.
            [str(column[1]) for column in description],
            workspace,
        )

    # -- metadata --------------------------------------------------------------

    def _names(self, query: str) -> Names:
        result = self.execute(query)
        return Names([row[0] for row in result.rows], result.truncated)

    def list_catalogs(self, pattern: str = "") -> Names:
        return self._names(f"SHOW CATALOGS{_like(pattern)}")

    def list_schemas(self, catalog: str, pattern: str = "") -> Names:
        return self._names(f"SHOW SCHEMAS FROM {qualify(catalog)}{_like(pattern)}")

    def list_tables(self, catalog: str, schema: str, pattern: str = "") -> Names:
        return self._names(f"SHOW TABLES FROM {qualify(catalog, schema)}{_like(pattern)}")

    def search_columns(self, catalog: str, pattern: str, schema: str = "") -> QueryResult:
        where = f"column_name LIKE {quote_literal(pattern)}"
        if schema:
            where += f" AND table_schema = {quote_literal(schema)}"
        return self.execute(
            "SELECT table_schema, table_name, column_name, data_type "
            f"FROM {qualify(catalog)}.information_schema.columns "
            f"WHERE {where} ORDER BY table_schema, table_name, column_name"
        )

    def describe_table(self, catalog: str, schema: str, table: str) -> QueryResult:
        return self.execute(f"DESCRIBE {qualify(catalog, schema, table)}")

    def explain(self, query: str, plan_format: str) -> str:
        result = self.execute(f"EXPLAIN (TYPE {plan_format}) {normalize(query)}")
        return "\n".join(str(row[0]) for row in result.rows)
