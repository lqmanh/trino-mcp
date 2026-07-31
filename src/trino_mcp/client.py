"""Blocking Trino access: row caps, query timeouts, and file output.

Everything here is synchronous. The server layer runs it off the event loop.
"""

import csv
import json
import logging
import queue
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import trino
from trino.dbapi import Connection, Cursor
from trino.exceptions import TrinoQueryError

from .config import Config
from .sql import is_read_only, normalize

log = logging.getLogger(__name__)

# Rows pulled per round trip when streaming to a file.
_FILE_BATCH = 1000


class QueryTimeout(Exception):
    """A query exceeded the configured deadline and was cancelled."""


@dataclass(frozen=True, slots=True)
class QueryResult:
    """Columnar results plus whether the row cap cut them short.

    Deliberately not a pydantic model: it carries every row of every query, and
    validating them would cost real time to prove something already known.
    """

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


def qualify(*parts: str) -> str:
    return ".".join(quote_ident(part) for part in parts if part)


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

    def execute_to_file(self, query: str, path: Path) -> int:
        """Stream every row to ``path`` and return how many were written.

        The row cap is deliberately not applied here. Writing to a file is how a
        caller asks for the whole result set without it passing through the
        model, so capping it would defeat the point; rows are fetched in batches
        so memory stays bounded regardless of result size.
        """
        return self._attempt(lambda cursor: self._drain_to_file(cursor, path), query)

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
        except (QueryTimeout, TrinoQueryError):
            raise
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
        """Run execute+consume, cancelling server-side if it overruns.

        Trino's ``execute`` returns once the first page is ready while the query
        keeps running, so the deadline has to cover consumption too — not just
        the call that starts it.
        """
        minutes = self._config.query_timeout_minutes
        if minutes <= 0:
            cursor.execute(query)
            return consume(cursor)

        # A raw daemon thread rather than a ThreadPoolExecutor: the executor joins
        # its workers at interpreter exit, so one runaway query would hang shutdown.
        outcome: queue.Queue[Any] = queue.Queue(maxsize=1)

        def work() -> None:
            try:
                cursor.execute(query)
                outcome.put(consume(cursor))
            except BaseException as exc:  # noqa: BLE001 - re-raised on the caller's thread
                outcome.put(exc)

        threading.Thread(target=work, daemon=True).start()

        try:
            result = outcome.get(timeout=minutes * 60)
        except queue.Empty:
            query_id = self._cancel(cursor)
            raise QueryTimeout(
                f"Query exceeded the {minutes:g}-minute timeout and was cancelled "
                f"(query_id={query_id or 'unknown'}). Narrow it with WHERE or LIMIT, "
                "avoid SELECT * on large tables, or raise TRINO_QUERY_TIMEOUT_MINUTES."
            ) from None

        if isinstance(result, BaseException):
            raise result
        return result

    def _cancel(self, cursor: Cursor) -> str | None:
        query_id = getattr(cursor, "query_id", None)
        log.warning("cancelling query %s after timeout", query_id or "unknown")

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
        if cursor.description is None:
            return QueryResult(columns=[], rows=[], truncated=False)

        columns = [column[0] for column in cursor.description]
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
                log.debug("cancelling after truncation failed", exc_info=True)
            log.warning("result truncated to %d rows (TRINO_MAX_ROWS)", cap)

        return QueryResult(columns=columns, rows=rows, truncated=truncated)

    def _drain_to_file(self, cursor: Cursor, path: Path) -> int:
        if cursor.description is None:
            path.write_text(
                "[]\n" if path.suffix.lower() != ".csv" else "", encoding="utf-8"
            )
            return 0

        columns = [column[0] for column in cursor.description]
        written = 0

        if path.suffix.lower() == ".csv":
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(columns)
                while batch := cursor.fetchmany(_FILE_BATCH):
                    writer.writerows(batch)
                    written += len(batch)
            return written

        # JSON: emit incrementally so a large result never sits in memory twice.
        with path.open("w", encoding="utf-8") as handle:
            handle.write("[")
            while batch := cursor.fetchmany(_FILE_BATCH):
                for row in batch:
                    if written:
                        handle.write(",")
                    handle.write("\n  ")
                    json.dump(dict(zip(columns, row, strict=True)), handle, default=str)
                    written += 1
            handle.write("\n]\n" if written else "]\n")
        return written

    # -- metadata --------------------------------------------------------------

    def _names(self, query: str) -> list[str]:
        return [row[0] for row in self.execute(query).rows]

    def list_catalogs(self) -> list[str]:
        return self._names("SHOW CATALOGS")

    def list_schemas(self, catalog: str) -> list[str]:
        return self._names(f"SHOW SCHEMAS FROM {qualify(catalog)}")

    def list_tables(self, catalog: str, schema: str) -> list[str]:
        return self._names(f"SHOW TABLES FROM {qualify(catalog, schema)}")

    def describe_table(self, catalog: str, schema: str, table: str) -> QueryResult:
        return self.execute(f"DESCRIBE {qualify(catalog, schema, table)}")

    def explain(self, query: str, plan_format: str) -> str:
        result = self.execute(f"EXPLAIN (TYPE {plan_format}) {normalize(query)}")
        return "\n".join(str(row[0]) for row in result.rows)
