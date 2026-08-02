import logging
from collections.abc import Iterable

import sqlglot
from sqlglot import expressions as exp

log = logging.getLogger(__name__)

# sqlglot logs a warning every time it falls back to a generic Command node,
# which happens for perfectly ordinary SHOW and DESCRIBE statements.
logging.getLogger("sqlglot").setLevel(logging.ERROR)

# sqlglot normalizes Trino's write syntax onto these standard expression classes.
_WRITE_TYPES = (
    exp.Alter,
    exp.Analyze,
    exp.Create,
    exp.Delete,
    exp.Drop,
    exp.Grant,
    exp.Insert,
    exp.Merge,
    exp.Refresh,
    exp.Revoke,
    exp.TruncateTable,
    exp.Update,
)

# SHOW and EXPLAIN parse to a generic Command node rather than a dedicated type,
# so they need a name check rather than an isinstance check.
_READ_ONLY_COMMANDS = frozenset({"SHOW", "EXPLAIN"})

# Iceberg's table property. Hive and Delta spell theirs `partitioned_by`, which
# sqlglot gives a dedicated node; this one arrives as a generic Property.
_PARTITIONING_PROPERTY = "partitioning"


def normalize(query: str) -> str:
    """Trim a statement into the exact form sent to Trino.

    Trino rejects a trailing semicolon, so it is stripped here rather than at the
    call site — the string that gets checked must be the string that gets
    executed, or the check is guarding something else.
    """
    return query.strip().rstrip(";").strip()


def is_read_only(query: str) -> bool:
    """Report whether ``query`` only reads.

    Returns ``False`` when the statement is empty, unparseable, or more than one
    statement — none of those are evidence that it is safe.

    Note that a ``SELECT`` may still reach a connector or UDF with side effects;
    this checks the statement, not what the statement ultimately triggers.
    """
    text = normalize(query)
    if not text:
        return False

    try:
        statements = [
            node for node in sqlglot.parse(text, read="trino") if node is not None
        ]
    except Exception:  # noqa: BLE001 - any parse failure must fail closed
        log.warning("could not parse query as Trino SQL; treating as a write")
        return False

    # Trino accepts one statement per request, and `parse_one` would quietly
    # check only the first — letting "SELECT 1; DROP TABLE t" through.
    if len(statements) != 1:
        return False

    statement = statements[0]

    if isinstance(statement, exp.Describe):
        return True

    if isinstance(statement, exp.Command):
        head = text.split(maxsplit=1)[0].upper()
        if head not in _READ_ONLY_COMMANDS:
            return False
        # EXPLAIN ANALYZE actually runs the statement it is explaining.
        return not (head == "EXPLAIN" and "ANALYZE" in text.upper().split())

    return not any(isinstance(node, _WRITE_TYPES) for node in statement.walk())


def partition_spec(ddl: str) -> list[str]:
    """The partition transforms in a ``SHOW CREATE TABLE`` statement, verbatim.

    Whichever property a connector spells them under, the value is an array of
    strings, and they come back unparsed on purpose: ``month(created_at)`` tells
    a reader that a day-level filter still scans a whole month, which the source
    column alone does not.
    """
    try:
        tree = sqlglot.parse_one(ddl, read="trino")
    except Exception:  # noqa: BLE001 - an unreadable DDL is a missing section, not a failure
        log.warning("could not parse SHOW CREATE TABLE output")
        return []

    arrays = [prop.this for prop in tree.find_all(exp.PartitionedByProperty)]
    arrays += [
        prop.args["value"]
        for prop in tree.find_all(exp.Property)
        if prop.name == _PARTITIONING_PROPERTY and "value" in prop.args
    ]
    return [
        item.name
        for array in arrays
        if isinstance(array, exp.Array)
        for item in array.expressions
    ]


def table_comment(ddl: str) -> str:
    """The table's ``COMMENT``, read from a ``SHOW CREATE TABLE`` statement.

    ``DESCRIBE`` returns the comment on each *column* and never the one on the
    table, so this is the only place a table's own description surfaces.
    """
    try:
        tree = sqlglot.parse_one(ddl, read="trino")
    except Exception:  # noqa: BLE001 - an unreadable DDL is a missing section, not a failure
        log.warning("could not parse SHOW CREATE TABLE output")
        return ""
    return next((prop.name for prop in tree.find_all(exp.SchemaCommentProperty)), "")


def partition_source_columns(specs: Iterable[str]) -> list[str]:
    """The columns a ``WHERE`` clause has to name to prune those partitions.

    Partitioning is hidden: ``month(created_at)`` creates no
    ``created_at_month`` column, so the transform is unusable as a filter and
    the source column is what prunes.

    Repeats are dropped — two transforms over one column are one column.
    """
    columns: list[str] = []
    for spec in specs:
        try:
            node = sqlglot.parse_one(spec, read="trino")
        except Exception:  # noqa: BLE001 - one unreadable transform must not lose the rest
            log.warning("could not parse partition transform %r", spec)
            continue
        for column in node.find_all(exp.Column):
            if column.name not in columns:
                columns.append(column.name)
    return columns
