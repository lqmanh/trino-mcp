import pytest

from trino_mcp.sql import is_read_only, normalize


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1",
        "SELECT 1;",
        "SELECT * FROM t WHERE x = 1",
        "WITH c AS (SELECT 1) SELECT * FROM c",
        "SHOW CATALOGS",
        "SHOW CREATE TABLE a.b.c",  # contains CREATE but only reads
        "DESCRIBE a.b.c",
        "EXPLAIN SELECT 1",
        "SELECT delete_flag FROM t",  # write keyword as an identifier
    ],
)
def test_reads_are_allowed(query: str) -> None:
    assert is_read_only(query)


@pytest.mark.parametrize(
    "query",
    [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET x = 1",
        "DELETE FROM t",
        "DROP TABLE t",
        "CREATE TABLE t (a int)",
        "ALTER TABLE t EXECUTE optimize",
        "GRANT SELECT ON t TO u",
        "EXPLAIN ANALYZE SELECT 1",  # ANALYZE runs the statement
        "SELECT * FROM t; DROP TABLE t",  # only the first would be inspected
        "not sql at all ((",
        "",
        "   ;   ",
    ],
)
def test_writes_and_ambiguity_are_refused(query: str) -> None:
    assert not is_read_only(query)


def test_normalize_strips_the_trailing_semicolon_trino_rejects() -> None:
    assert normalize("  SELECT 1;  ") == "SELECT 1"
