import csv
import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal, NamedTuple, Protocol

import duckdb
from sqlglot import expressions as exp

log = logging.getLogger(__name__)

_BATCH = 1000

OutputFormat = Literal["csv", "json", "parquet"]


class Batches(Protocol):
    """The only thing a writer here needs from a result set.

    Narrow on purpose. Reading ``description`` would pull trino's py.typed
    inaccuracies into this module, and those belong beside the cursor code that
    already documents them.
    """

    def fetchmany(self, size: int) -> list[list[Any]]: ...


def _quote(identifier: str) -> str:
    """Quote a DuckDB identifier.

    Deliberately not ``client.quote_ident``: that one is a security boundary over
    untrusted tool arguments, this one names columns in SQL generated right here.
    Sharing them would couple the boundary to a coincidence of syntax.
    """
    return '"' + identifier.replace('"', '""') + '"'


def _literal(value: str) -> str:
    """Single-quoted SQL literal, for the DuckDB statements that take no parameter."""
    return "'" + value.replace("'", "''") + "'"


def _optional(convert: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Lift a converter so NULL passes straight through."""
    return lambda value: None if value is None else convert(value)


_stringify = _optional(str)
# Every byte escaped, rather than passing printable ones through: the result has
# to survive JSON and then DuckDB's own blob parsing, and being unambiguous is
# worth more than a smaller staging file nobody keeps.
_blob_text = _optional(lambda value: "".join(f"\\x{byte:02x}" for byte in value))


def _adapter(datatype: exp.DataType) -> Callable[[Any], Any] | None:
    """Convert a Trino value into something DuckDB's binder accepts, or None.

    Trino returns a ROW as a tuple and DuckDB refuses to bind a sequence to a
    STRUCT, so those become dicts keyed by the field names the type declares.
    Bytes have no JSON form at all and go out as escaped text. None means the
    value already binds, which keeps scalar columns — nearly all of them — off
    the per-row conversion path entirely.
    """
    kind = datatype.this

    if kind == exp.DataType.Type.VARBINARY:
        return _blob_text

    if kind == exp.DataType.Type.STRUCT:
        fields = [
            (field.name, _adapter(field.args["kind"])) for field in datatype.expressions
        ]
        if all(inner is None for _, inner in fields):
            names = [name for name, _ in fields]
            return _optional(lambda value: dict(zip(names, value, strict=False)))
        return _optional(
            lambda value: {
                name: inner(item) if inner else item
                for (name, inner), item in zip(fields, value, strict=False)
            }
        )

    if kind == exp.DataType.Type.ARRAY and datatype.expressions:
        element = _adapter(datatype.expressions[0])
        if element is None:
            return None
        convert = element  # rebound so the closure captures a non-optional
        return _optional(lambda value: [convert(item) for item in value])

    if kind == exp.DataType.Type.MAP and len(datatype.expressions) > 1:
        entry = _adapter(datatype.expressions[1])
        if entry is None:
            return None
        convert_value = entry
        return _optional(
            lambda value: {key: convert_value(item) for key, item in value.items()}
        )

    return None


class _ExportColumn(NamedTuple):
    """How one Trino column crosses into Parquet.

    ``restore`` is the type to cast the staged column back to, set only when the
    two differ.
    """

    declaration: str
    adapt: Callable[[Any], Any] | None
    restore: str | None


def _duckdb_column(
    connection: duckdb.DuckDBPyConnection, trino_type: str
) -> _ExportColumn:
    """Resolve one Trino column type into its staged JSON form and DuckDB type.

    sqlglot already speaks both dialects, so ``row(...)`` becomes ``STRUCT(...)``
    and ``array(...)`` becomes ``...[]`` without hand-written rules. A type DuckDB
    lacks — ``ipaddress`` in core Trino, plus whatever a connector invents — is
    stringified rather than failing the whole export, which is why the rendering
    is probed instead of trusted.

    Binary is the one thing JSON cannot carry, so those columns stage as text and
    are cast back on the way in. The swap happens on the type tree rather than at
    the top level because DuckDB's CAST recurses into lists, structs and maps —
    which is what makes ``array(varbinary)`` work rather than quietly staging a
    Python repr against a BLOB[] declaration.
    """
    try:
        datatype = exp.DataType.build(trino_type, dialect="trino")
        rendered = datatype.sql(dialect="duckdb")
        connection.execute(f"SELECT CAST(NULL AS {rendered})")
    except Exception:  # noqa: BLE001 - an unmappable type is demoted, not fatal
        log.info(
            "no DuckDB equivalent for Trino type %s; exporting it as VARCHAR", trino_type
        )
        return _ExportColumn("VARCHAR", _stringify, None)

    staged = datatype.copy()
    binary = [
        node
        for node in staged.find_all(exp.DataType)
        if node.this == exp.DataType.Type.VARBINARY
    ]
    for node in binary:
        node.set("this", exp.DataType.Type.VARCHAR)
    if not binary:
        return _ExportColumn(rendered, _adapter(datatype), None)
    return _ExportColumn(staged.sql(dialect="duckdb"), _adapter(datatype), rendered)


def _write_csv(rows: Batches, path: Path, columns: list[str]) -> int:
    written = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        if columns:
            writer.writerow(columns)
        while batch := rows.fetchmany(_BATCH):
            writer.writerows(batch)
            written += len(batch)
    return written


def _write_json(rows: Batches, path: Path, columns: list[str]) -> int:
    # Bound once: json.dumps only reuses its cached encoder when every keyword is
    # default, so passing default= builds a fresh JSONEncoder for every call.
    encode = json.JSONEncoder(default=str).encode
    written = 0
    with path.open("w", encoding="utf-8") as handle:
        handle.write("[")
        while batch := rows.fetchmany(_BATCH):
            chunk = ",\n  ".join(
                encode(dict(zip(columns, row, strict=True))) for row in batch
            )
            handle.write(("," if written else "") + "\n  " + chunk)
            written += len(batch)
        handle.write("\n]\n" if written else "]\n")
    return written


def _write_parquet(
    rows: Batches, path: Path, columns: list[str], types: list[str], workspace: Path
) -> int:
    """Stream a result set into a Parquet file, staging the rows as NDJSON.

    Binding Python values into DuckDB row by row manages about 1.4k rows/s, which
    an uncapped export cannot afford; handing DuckDB a file to read instead is
    roughly 400x faster and lets it choose its own row groups. JSON rather than
    CSV because it tells NULL apart from the empty string, survives newlines
    inside a value, and carries row/array/map columns without a second encoding.

    The staging file is larger than the Parquet it produces, which is why it goes
    in the caller's ``workspace`` rather than anywhere this function picks.
    """
    if not columns:
        # DuckDB refuses a relation that projects nothing, so a result set that
        # arrived without columns cannot be expressed faithfully. The placeholder
        # keeps the file parseable, which beats the zero bytes csv gets away with.
        with duckdb.connect() as connection:
            connection.execute(
                f"COPY (SELECT NULL AS empty WHERE false) "
                f"TO {_literal(str(path))} (FORMAT PARQUET)"
            )
        return 0

    with duckdb.connect() as connection:
        resolved = [_duckdb_column(connection, trino_type) for trino_type in types]
        # Walk only the columns that need converting. Nearly all of them do not,
        # and testing every cell of every row costs with the column count.
        fixups = [
            (index, column.adapt)
            for index, column in enumerate(resolved)
            if column.adapt is not None
        ]
        encode = json.JSONEncoder(default=str).encode

        def line(row: list[Any]) -> str:
            if fixups:
                row = list(row)
                for index, adapt in fixups:
                    row[index] = adapt(row[index])
            return encode(dict(zip(columns, row, strict=True)))

        stage = workspace / "rows.ndjson"
        written = 0
        with stage.open("w", encoding="utf-8") as handle:
            while batch := rows.fetchmany(_BATCH):
                handle.write("\n".join(line(row) for row in batch) + "\n")
                written += len(batch)

        staged = ", ".join(
            f"{_literal(name)}: {_literal(column.declaration)}"
            for name, column in zip(columns, resolved, strict=True)
        )
        projection = ", ".join(
            _quote(name)
            if column.restore is None
            else f"CAST({_quote(name)} AS {column.restore}) AS {_quote(name)}"
            for name, column in zip(columns, resolved, strict=True)
        )
        connection.execute(
            f"COPY (SELECT {projection} FROM read_json({_literal(str(stage))}, "
            f"columns = {{{staged}}}, format = 'newline_delimited')) "
            f"TO {_literal(str(path))} (FORMAT PARQUET, COMPRESSION zstd)"
        )
        return written


def write(
    rows: Batches,
    path: Path,
    fmt: OutputFormat,
    columns: list[str],
    types: list[str],
    workspace: Path,
) -> int:
    """Drain ``rows`` into ``path`` as ``fmt`` and report how many were written.

    ``columns`` and ``types`` describe the result set because this module never
    introspects the cursor itself — see ``Batches``. Only Parquet reads ``types``.
    An empty ``columns`` means the result set arrived without any; each writer
    still has to leave a file its format's readers can open.

    ``workspace`` is an existing directory for scratch that the caller destroys
    afterwards. A writer needing room must use it rather than allocating its own,
    so one place cleans up everything an export creates — including on the
    timeout path, where the drain is abandoned mid-write.
    """
    match fmt:
        case "csv":
            return _write_csv(rows, path, columns)
        case "json":
            return _write_json(rows, path, columns)
        case "parquet":
            return _write_parquet(rows, path, columns, types, workspace)
