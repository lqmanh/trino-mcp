from pathlib import Path

import pytest
from pydantic import ValidationError

from trino_mcp import Config

ALL_VARS = [
    "TRINO_HOST",
    "TRINO_PORT",
    "TRINO_USER",
    "TRINO_PASSWORD",
    "TRINO_HTTP_SCHEME",
    "TRINO_CATALOG",
    "TRINO_SCHEMA",
    "TRINO_MAX_ROWS",
    "TRINO_QUERY_TIMEOUT_MINUTES",
    "TRINO_ALLOW_WRITES",
    "TRINO_OUTPUT_DIR",
    "TRINO_MANIFEST_PATH",
]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start every case from an environment with no TRINO_* leakage.

    TRINO_USER is required, so it is set here rather than in every case; the
    test that covers its absence deletes it again.
    """
    for name in ALL_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TRINO_USER", "tester")


def test_port_follows_the_scheme_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unset port must not leave an https default on a plain-http cluster."""
    assert Config.from_env().port == 8443

    monkeypatch.setenv("TRINO_HTTP_SCHEME", "http")
    assert Config.from_env().port == 8080


def test_user_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    """No silent fallback: the username is half the credential under password auth."""
    monkeypatch.delenv("TRINO_USER")

    with pytest.raises(ValidationError, match="user"):
        Config.from_env()


def test_password_is_refused_over_plain_http(monkeypatch: pytest.MonkeyPatch) -> None:
    """The trino client would send it base64-encoded in the clear without objecting."""
    monkeypatch.setenv("TRINO_PASSWORD", "hunter2")
    monkeypatch.setenv("TRINO_HTTP_SCHEME", "http")

    with pytest.raises(ValidationError, match="cleartext"):
        Config.from_env()


def test_default_output_dir_is_absolute() -> None:
    """A relative default path is resolved so returned file paths are usable."""
    assert Config.from_env().output_dir.is_absolute()


def test_a_missing_output_dir_is_allowed_but_a_file_is_not(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The export path creates the directory itself, so absence is fine.

    A file under that name is not: without this it surfaces as a raw errno at the
    first export rather than at startup.
    """
    monkeypatch.setenv("TRINO_OUTPUT_DIR", str(tmp_path / "created" / "later"))
    assert Config.from_env().output_dir.name == "later"

    not_a_dir = tmp_path / "results.csv"
    not_a_dir.write_text("")
    monkeypatch.setenv("TRINO_OUTPUT_DIR", str(not_a_dir))

    with pytest.raises(ValidationError, match="not a directory"):
        Config.from_env()


def test_a_manifest_path_must_already_exist(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Unlike the output directory, nothing creates this one later.

    Pointing at a file that is not there would otherwise start a server whose
    `explain_table` knows nothing about any table, which reads as coverage.
    """
    monkeypatch.setenv("TRINO_MANIFEST_PATH", str(tmp_path / "absent.json"))

    with pytest.raises(ValidationError, match="not a file"):
        Config.from_env()

    present = tmp_path / "manifest.json"
    present.write_text("{}")
    monkeypatch.setenv("TRINO_MANIFEST_PATH", str(present))
    assert Config.from_env().manifest_path == present


def test_the_password_does_not_survive_rendering_the_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`main` logs the config whole at startup, so `str` is the path that matters."""
    monkeypatch.setenv("TRINO_PASSWORD", "hunter2")
    config = Config.from_env()

    assert "hunter2" not in str(config)
    assert config.password is not None
    assert config.password.get_secret_value() == "hunter2"
