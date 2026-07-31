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


def test_output_dir_defaults_to_dot_output_and_need_not_exist(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Both are what the export path relies on: it creates the directory itself."""
    assert Config.from_env().output_dir == Path(".output")

    monkeypatch.setenv("TRINO_OUTPUT_DIR", str(tmp_path / "created" / "later"))
    assert Config.from_env().output_dir.name == "later"


def test_output_dir_pointing_at_a_file_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Otherwise it surfaces as a raw errno at the first export, not at startup."""
    not_a_dir = tmp_path / "results.csv"
    not_a_dir.write_text("")
    monkeypatch.setenv("TRINO_OUTPUT_DIR", str(not_a_dir))

    with pytest.raises(ValidationError, match="not a directory"):
        Config.from_env()


def test_the_password_is_masked_everywhere_it_travels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The config is logged whole at startup, so the field itself must be safe.

    SecretStr guards the value rather than one container's repr, so the paths a
    future edit is most likely to reach for are all pinned here.
    """
    monkeypatch.setenv("TRINO_PASSWORD", "hunter2")
    config = Config.from_env()

    assert "hunter2" not in repr(config)
    assert "hunter2" not in str(config)
    assert "hunter2" not in f"{config.password}"
    assert "hunter2" not in str(config.model_dump())
    assert config.password is not None
    assert config.password.get_secret_value() == "hunter2"
