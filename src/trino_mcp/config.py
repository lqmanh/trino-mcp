"""Trino connection settings, read once from the environment at startup."""

import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, model_validator

# Trino's defaults differ by scheme, so an unset TRINO_PORT resolves against
# TRINO_HTTP_SCHEME rather than a single constant.
_DEFAULT_PORTS = {"http": 8080, "https": 8443}


class Config(BaseModel):
    """Everything the server needs to reach a Trino cluster.

    Frozen so a tool call cannot mutate the settings the process started with.
    """

    model_config = {"frozen": True}

    host: str = "localhost"
    port: int = Field(default=8443, ge=1, le=65535)
    user: str = Field(min_length=1)
    password: SecretStr | None = None
    http_scheme: Literal["http", "https"] = "https"
    catalog: str | None = None
    # `schema` would shadow an attribute on BaseModel, hence the prefix.
    db_schema: str | None = None
    max_rows: int = Field(default=1000, ge=0)
    query_timeout_minutes: float = Field(default=5.0, ge=0)
    allow_writes: bool = False
    output_dir: Path = Field(default_factory=Path.cwd)

    @model_validator(mode="after")
    def _refuse_cleartext_password(self) -> "Config":
        """Reject a password paired with plain http.

        The trino client sends BasicAuthentication over http without complaint,
        so the credential reaches the wire base64-encoded before Trino rejects
        it for want of TLS. An absent password is fine — unauthenticated
        clusters are normal locally.
        """
        if self.password and self.http_scheme == "http":
            raise ValueError(
                "TRINO_PASSWORD with TRINO_HTTP_SCHEME=http would send the "
                "credential in cleartext; use https"
            )
        return self

    @classmethod
    def from_env(cls) -> "Config":
        """Build a config from TRINO_* environment variables.

        Unset (or empty) variables fall back to the field defaults; set ones are
        coerced and validated by pydantic, so a bad value fails at startup with a
        readable message instead of at the first query.
        """
        env = {
            "host": os.environ.get("TRINO_HOST"),
            "port": os.environ.get("TRINO_PORT"),
            "user": os.environ.get("TRINO_USER"),
            "password": os.environ.get("TRINO_PASSWORD"),
            "http_scheme": os.environ.get("TRINO_HTTP_SCHEME"),
            "catalog": os.environ.get("TRINO_CATALOG"),
            "db_schema": os.environ.get("TRINO_SCHEMA"),
            "max_rows": os.environ.get("TRINO_MAX_ROWS"),
            "query_timeout_minutes": os.environ.get("TRINO_QUERY_TIMEOUT_MINUTES"),
            "allow_writes": os.environ.get("TRINO_ALLOW_WRITES"),
            "output_dir": os.environ.get("TRINO_OUTPUT_DIR"),
        }
        values = {key: value for key, value in env.items() if value}

        # Resolve the port default against the scheme actually in use. Kept as a
        # string so pydantic does the coercion (and the range check) in one place.
        if "port" not in values:
            scheme = values.get("http_scheme", "https")
            values["port"] = str(_DEFAULT_PORTS.get(scheme, 8443))

        # model_validate rather than cls(**values): these are strings off the
        # environment, and this is exactly the coerce-and-validate path.
        return cls.model_validate(values)
