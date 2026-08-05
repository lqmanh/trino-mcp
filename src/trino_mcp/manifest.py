"""The static table facts Trino has no way to report.

A cluster answers what its tables *are* — columns, types, sizes, partitioning,
freshness. It cannot answer what makes a row unique, which columns join two
tables, or where a value came from. On a lakehouse those facts do not exist at
all: Iceberg has no keys and no foreign keys, so a grain is a property of how a
table is built rather than of the table itself.

This module reads them from a file someone else generates. Nothing here knows
what produced it — the format is the contract, and any pipeline that can emit it
is a valid producer.
"""

import json
from pathlib import Path
from typing import Annotated, Any

from pydantic import BaseModel, Field

SCHEMA_VERSION = 1

# Points at `main`, so every published version has to stay there: a producer
# written against v1 dereferences this, and replacing the file on a version bump
# would break it.
SCHEMA_ID = (
    "https://raw.githubusercontent.com/lqmanh/trino-mcp/main/schemas/"
    f"manifest.v{SCHEMA_VERSION}.json"
)


class Join(BaseModel):
    """How this table joins to another. Not a foreign key: nothing enforces it."""

    columns: Annotated[
        list[str], Field(description="Columns on this table, in key order.")
    ]
    target: Annotated[str, Field(description="Fully qualified catalog.schema.table.")]
    target_columns: Annotated[
        list[str], Field(description="Columns on the target, positionally matched.")
    ]


class Column(BaseModel):
    """A column carrying something its name and Trino type do not convey."""

    name: Annotated[str, Field(description="Column name.")]
    note: Annotated[
        str | None,
        Field(description="What the type hides, e.g. a varchar that holds JSON."),
    ] = None
    sources: Annotated[
        list[str],
        Field(description="Upstream columns it derives from, as table.column."),
    ] = []


class Table(BaseModel):
    """What is known about one table beyond what the cluster reports."""

    grain: Annotated[
        list[str],
        Field(description="Columns that together identify one row. Empty when unknown."),
    ] = []
    unique_keys: Annotated[
        list[list[str]],
        Field(description="Other column sets known to be unique, besides the grain."),
    ] = []
    replica_of: Annotated[
        str | None,
        Field(description="Source relation this table copies, if it is a replica."),
    ] = None
    upstream: Annotated[
        list[str],
        Field(description="Tables this one is derived from, if it is computed."),
    ] = []
    joins: Annotated[
        list[Join], Field(description="Known join paths to other tables.")
    ] = []
    columns: Annotated[
        list[Column],
        Field(
            description="Only columns with a note or known sources; not the full list."
        ),
    ] = []


class Manifest(BaseModel):
    """A set of table facts, generated at a point in time."""

    version: Annotated[
        int,
        Field(description="Format version. Bumped only when a field changes meaning."),
    ]
    generated_at: Annotated[
        str, Field(description="ISO 8601 time the manifest was produced.")
    ]
    tables: Annotated[
        dict[str, Table],
        Field(description="Keyed by fully qualified catalog.schema.table."),
    ]


def load(path: Path) -> Manifest:
    """Read and validate a manifest.

    Rejects a version it does not know rather than reading the fields it happens
    to recognize: a format change that redefines a field would otherwise be
    answered confidently and wrongly.
    """
    manifest = Manifest.model_validate(json.loads(path.read_text(encoding="utf-8")))
    if manifest.version != SCHEMA_VERSION:
        raise ValueError(
            f"manifest is version {manifest.version}; this build reads "
            f"version {SCHEMA_VERSION}"
        )
    return manifest


class Store:
    """A manifest kept in step with the file it was read from.

    Loaded eagerly so a broken file fails at startup like any other bad setting,
    then re-read whenever the file changes — regenerating a manifest should not
    need a server restart. Only the stat happens per lookup; parsing waits for
    the mtime to actually move.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        # Stat first: reading it afterwards can only pick up content newer than
        # the recorded mtime, which the next lookup then re-reads. The reverse
        # order would cache stale content under a fresh mtime, permanently.
        self._mtime = path.stat().st_mtime
        self._manifest = load(path)
        self._index = _index(self._manifest)

    def __len__(self) -> int:
        """How many tables the manifest covers, as last read."""
        return len(self._manifest.tables)

    def _current(self) -> None:
        mtime = self.path.stat().st_mtime
        if mtime != self._mtime:
            self._manifest = load(self.path)
            self._index = _index(self._manifest)
            self._mtime = mtime

    def lookup(self, table: str) -> tuple[Table | None, Manifest]:
        """The facts for ``table``, and the manifest they came from.

        The manifest travels with the answer so a caller can report how old it
        is, and what it covers when the table is not in it.
        """
        self._current()
        return self._index.get(table.lower()), self._manifest


def _index(manifest: Manifest) -> dict[str, Table]:
    """Case-folded lookup, because Trino folds unquoted identifiers to lowercase."""
    return {name.lower(): table for name, table in manifest.tables.items()}


def json_schema() -> dict[str, Any]:
    """The published JSON Schema for the manifest format."""
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_ID,
        **Manifest.model_json_schema(),
    }
