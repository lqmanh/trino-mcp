"""A disposable Trino, built fresh for every run.

Never adopts a cluster that happens to be listening. A reachable port is not an
identity check, and this suite seeds tables — pointing it at whatever answered
would be how a test writes into production. The stack gets its own compose
project and an ephemeral host port so it cannot collide with a real cluster, or
with one someone brought up by hand for evaluation.
"""

import os
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest
import trino

from trino_mcp import Config
from trino_mcp.client import TrinoClient

PROJECT = "trino-mcp-e2e"
ROOT = Path(__file__).resolve().parents[2]
SEED = Path(__file__).parent / "seed.sql"


def _compose(*args: str, timeout: float = 600) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "-p", PROJECT, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        # TRINO_PORT=0 asks the kernel for a free port; the assigned one is read
        # back with `compose port` once the stack is up.
        env=os.environ | {"TRINO_PORT": "0"},
        check=False,
    )


def _statements(sql: str) -> list[str]:
    body = "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--")
    )
    return [statement.strip() for statement in body.split(";") if statement.strip()]


def _seed(config: Config) -> None:
    """Run the fixture data in, through the driver rather than the client.

    Seeding must not depend on the code under test, and the CLI is not
    guaranteed to be on the host.
    """
    with trino.dbapi.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        http_scheme=config.http_scheme,
    ) as connection:
        for statement in _statements(SEED.read_text()):
            cursor = connection.cursor()
            cursor.execute(statement)
            # CTAS reports its row count as a result set; draining it is what
            # waits for the write to actually finish.
            cursor.fetchall()


@pytest.fixture(scope="session")
def live_config() -> Iterator[Config]:
    if shutil.which("docker") is None:
        pytest.skip("docker is not installed")

    # Clears anything a previously killed run left behind, so a crash costs the
    # next run nothing.
    _compose("down", "-v")

    started = _compose("up", "-d", "--wait")
    if started.returncode:
        _compose("down", "-v")
        pytest.fail(f"could not start the stack:\n{started.stderr}")

    try:
        published = _compose("port", "trino", "8080")
        if published.returncode or ":" not in published.stdout:
            pytest.fail(f"could not resolve the trino port:\n{published.stderr}")

        config = Config(
            host="localhost",
            port=int(published.stdout.strip().rsplit(":", 1)[1]),
            user="e2e",
            http_scheme="http",
        )
        _seed(config)
        yield config
    finally:
        _compose("down", "-v")


@pytest.fixture(scope="session")
def live(live_config: Config) -> Iterator[TrinoClient]:
    client = TrinoClient(live_config)
    try:
        yield client
    finally:
        client.close()
