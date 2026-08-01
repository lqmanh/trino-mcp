import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest

from trino_mcp import export
from trino_mcp.export import OutputFormat

from .conftest import Rows


def test_writing_json_produces_a_parseable_array(tmp_path: Path) -> None:
    destination = tmp_path / "out.json"

    written = export.write(
        Rows([[n] for n in range(3)]), destination, "json", ["n"], ["bigint"], tmp_path
    )

    assert written == 3
    assert json.loads(destination.read_text()) == [{"n": 0}, {"n": 1}, {"n": 2}]


@pytest.mark.parametrize(("fmt", "expected"), [("csv", ""), ("json", "[]\n")])
def test_a_result_set_with_no_columns_writes_an_empty_file(
    tmp_path: Path, fmt: OutputFormat, expected: str
) -> None:
    """A result set can arrive with no columns at all, and the caller has already
    been told a path. It has to hold something a reader can open.
    """
    destination = tmp_path / "out"

    assert export.write(Rows([]), destination, fmt, [], [], tmp_path) == 0
    assert destination.read_text() == expected


def test_a_parquet_export_with_no_columns_is_still_a_readable_file(
    tmp_path: Path,
) -> None:
    """DuckDB cannot write a relation that projects nothing, so this path cannot
    be faithful. A placeholder column keeps the file parseable, which beats the
    zero bytes csv gets away with.
    """
    destination = tmp_path / "out.parquet"

    assert export.write(Rows([]), destination, "parquet", [], [], tmp_path) == 0
    with duckdb.connect() as connection:
        assert connection.execute(f"SELECT count(*) FROM '{destination}'").fetchone() == (
            0,
        )


def test_parquet_keeps_every_column_type_including_nested(tmp_path: Path) -> None:
    """The schema comes from the declared column types, never from the values.

    Inferring it instead is what breaks on the second batch, when a decimal shows
    up at a wider scale or a column that was all NULL turns out to hold something.
    The nested cases are here because they are the ones with no scalar fallback:
    csv writes a row as its unparseable Python repr, and a type Trino has but
    DuckDB does not (ipaddress) has to degrade rather than fail the export.
    """
    columns = [
        "n",
        "amount",
        "at",
        "note",
        "blob",
        "blobs",
        "boxed",
        "tags",
        "attrs",
        "person",
        "addr",
    ]
    types = [
        "bigint",
        "decimal(38,10)",
        "timestamp(6) with time zone",
        "varchar",
        "varbinary",
        # Binary below the top level: staged as text and cast back like the
        # scalar, because handling only the outermost type wrote a Python repr
        # against a BLOB[] declaration and nothing complained.
        "array(varbinary)",
        "row(payload varbinary)",
        "array(varchar)",
        "map(varchar, bigint)",
        "row(name varchar, age integer)",
        "ipaddress",
    ]
    data: list[list[Any]] = [
        [
            7,
            Decimal("19.9900000000"),
            datetime(2026, 1, 1, tzinfo=UTC),
            # An empty string must not arrive as NULL, and a value carrying the
            # staging format's own delimiter must survive it.
            'a,b"c\nd',
            b"\x00\x01bin",  # bytes have no JSON form; they travel as escaped text
            [b"\x00\x01bin"],
            (b"\x00\x01bin",),
            ["a", "b"],
            {"k": 1},
            ("alice", 30),  # trino hands a ROW back as a tuple, which DuckDB refuses
            "10.0.0.1",
        ],
        [None] * 3 + [""] + [None] * 7,
    ]
    destination = tmp_path / "out.parquet"

    written = export.write(Rows(data), destination, "parquet", columns, types, tmp_path)

    assert written == 2
    with duckdb.connect() as connection:
        schema = dict(
            connection.execute(
                f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM '{destination}')"
            ).fetchall()
        )
        first, second = connection.execute(
            f"SELECT * FROM '{destination}' ORDER BY n NULLS LAST"
        ).fetchall()

    assert schema == {
        "n": "BIGINT",
        "amount": "DECIMAL(38,10)",
        "at": "TIMESTAMP WITH TIME ZONE",
        "note": "VARCHAR",
        "blob": "BLOB",
        "blobs": "BLOB[]",
        "boxed": "STRUCT(payload BLOB)",
        "tags": "VARCHAR[]",
        "attrs": "MAP(VARCHAR, BIGINT)",
        "person": 'STRUCT("name" VARCHAR, age INTEGER)',
        "addr": "VARCHAR",
    }
    assert first[1] == Decimal("19.9900000000")
    assert first[3] == 'a,b"c\nd'
    assert first[4] == b"\x00\x01bin"
    assert first[5] == [b"\x00\x01bin"]
    assert first[6] == {"payload": b"\x00\x01bin"}
    assert first[7] == ["a", "b"]
    assert first[9] == {"name": "alice", "age": 30}
    # The empty string is the one that a CSV staging format loses to NULL.
    assert second[3] == ""
    assert second[1] is None
