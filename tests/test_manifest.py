import json
import os
from pathlib import Path
from typing import Any

import pytest

from trino_mcp import manifest

from .conftest import MANIFEST

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "schemas"
    / f"manifest.v{manifest.SCHEMA_VERSION}.json"
)


def write(path: Path, body: Any) -> Path:
    path.write_text(json.dumps(body))
    return path


def test_a_manifest_from_a_future_version_is_refused(tmp_path: Path) -> None:
    """Reading the fields we recognize out of an unknown format is the bad outcome.

    Every field here is optional, so a v2 manifest whose meanings have shifted
    validates cleanly and answers questions wrongly rather than failing.
    """
    path = write(tmp_path / "m.json", MANIFEST | {"version": 2})

    with pytest.raises(ValueError, match="version 2"):
        manifest.load(path)


def test_lookup_folds_case(tmp_path: Path) -> None:
    """Trino lowercases unquoted identifiers, so a caller may qualify either way.

    A miss is reported as an absent table, which reads as "no grain" rather than
    as a failed lookup.
    """
    store = manifest.Store(write(tmp_path / "m.json", MANIFEST))

    found, _ = store.lookup("CAT.SCH.Orders")
    assert found is not None
    assert found.grain == ["id"]


def test_a_rewritten_manifest_is_picked_up(tmp_path: Path) -> None:
    """Regenerating one must not need a restart, or the served facts drift.

    Caching on mtime is what makes that cheap; caching without checking it is
    what would serve yesterday's grain forever.
    """
    path = write(tmp_path / "m.json", MANIFEST)
    store = manifest.Store(path)
    assert len(store) == 2

    replacement = MANIFEST | {"tables": {"cat.sch.orders": {"grain": ["reference"]}}}
    write(path, replacement)
    # st_mtime has sub-second resolution but not infinite; make the change visible
    # without making the test wait for a real clock tick.
    os.utime(path, (0, 0))

    found, _ = store.lookup("cat.sch.orders")
    assert found is not None
    assert found.grain == ["reference"]
    assert len(store) == 1


def test_the_published_schema_matches_the_models() -> None:
    """`schemas/manifest.v1.json` is the contract a generator is written against.

    It ships in the repo rather than being derived at read time, so nothing
    fails when the models move underneath it.
    """
    assert json.loads(SCHEMA_FILE.read_text()) == manifest.json_schema(), (
        "schemas/ is out of date; regenerate it with `uv run scripts/schema.py`"
    )
