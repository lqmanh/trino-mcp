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


def test_summary_reports_auth_without_leaking_the_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The startup line is how an operator sees whether auth is actually on."""
    assert "auth=none" in Config.from_env().summary

    monkeypatch.setenv("TRINO_PASSWORD", "hunter2")
    config = Config.from_env()

    assert "auth=password" in config.summary
    assert "hunter2" not in config.summary
    assert "hunter2" not in repr(config)
