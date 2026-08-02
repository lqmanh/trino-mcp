import pytest

from trino_mcp.sql import (
    is_read_only,
    partition_source_columns,
    partition_spec,
    table_comment,
)


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
        # A catalog's system procedures are how DDL reaches a source database
        # without a write keyword in sight — the postgresql connector's
        # `execute` runs arbitrary SQL against the live server.
        "CALL pg_prod.system.execute('ALTER TABLE t ADD PRIMARY KEY (id)')",
        "CALL hive.system.sync_partition_metadata('sch', 't', 'ADD')",
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


ICEBERG_DDL = """CREATE TABLE sch.orders (id bigint, created_at timestamp(6))
COMMENT 'One row per order.'
WITH (format = 'PARQUET', partitioning = ARRAY['month(created_at)','region'])"""

HIVE_DDL = """CREATE TABLE sch.events (payload varchar, dt varchar)
WITH (format = 'JSON', partitioned_by = ARRAY['dt'])"""


@pytest.mark.parametrize(
    ("ddl", "expected"),
    [
        (ICEBERG_DDL, ["month(created_at)", "region"]),
        # Hive spells the property `partitioned_by`, which sqlglot parses to a
        # different node than Iceberg's `partitioning` — one branch each.
        (HIVE_DDL, ["dt"]),
        ("CREATE TABLE t (a int) WITH (format = 'PARQUET')", []),
        ("not a create statement (((", []),
    ],
)
def test_the_partition_spec_is_read_from_either_connectors_property(
    ddl: str, expected: list[str]
) -> None:
    assert partition_spec(ddl) == expected


@pytest.mark.parametrize(
    ("specs", "expected"),
    [
        # Hidden partitioning: there is no created_at_month column, so returning
        # the transform verbatim sends a filter after a column that is not there.
        (["month(created_at)"], ["created_at"]),
        (["day(logged_at)", "region"], ["logged_at", "region"]),
        (["bucket(id, 16)", "truncate(name, 10)"], ["id", "name"]),
        # Two grains over one column are still one column to filter on.
        (["day(ts)", "month(ts)"], ["ts"]),
        # An unreadable transform must not cost the ones beside it.
        (["(((", "region"], ["region"]),
    ],
)
def test_transforms_resolve_to_the_column_a_filter_must_name(
    specs: list[str], expected: list[str]
) -> None:
    assert partition_source_columns(specs) == expected


@pytest.mark.parametrize(
    ("ddl", "expected"),
    [
        (ICEBERG_DDL, "One row per order."),
        # No COMMENT clause is the common case, and it is not an error.
        (HIVE_DDL, ""),
        ("not a create statement (((", ""),
    ],
)
def test_the_table_comment_is_read_from_the_ddl(ddl: str, expected: str) -> None:
    """`DESCRIBE` carries column comments and never the table's own."""
    assert table_comment(ddl) == expected
