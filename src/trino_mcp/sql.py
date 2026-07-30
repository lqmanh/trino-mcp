"""Read-only detection for Trino SQL.

Parsing the statement and inspecting the AST is the only approach that holds up:
keyword matching has to special-case every read-only statement that happens to
contain a write keyword (``SHOW CREATE TABLE``, a column literally named
``delete``), and it silently fails open on the cases it hasn't been taught.
"""

import logging

import sqlglot
from sqlglot import expressions as exp

log = logging.getLogger(__name__)

# sqlglot logs a warning every time it falls back to a generic Command node,
# which happens for perfectly ordinary SHOW and DESCRIBE statements.
logging.getLogger("sqlglot").setLevel(logging.ERROR)

# Statement types that modify data, schema, or grants. sqlglot normalises Trino
# syntax onto these standard expression classes.
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
            # Anything else arriving as an unparsed Command is unknown, so refuse.
            return False
        # EXPLAIN ANALYZE actually runs the statement it is explaining.
        return not (head == "EXPLAIN" and "ANALYZE" in text.upper().split())

    return not any(isinstance(node, _WRITE_TYPES) for node in statement.walk())
