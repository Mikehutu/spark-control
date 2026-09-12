"""Configuration for the Spark Control gateway.

Secrets and host/recipe settings are read from environment variables only. No
token material is ever hardcoded, committed, or logged.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True)
class Settings:
    """Gateway settings loaded from the environment.

    Only the secrets/config the gateway-core slice needs are declared here.
    Recipe fields (node IPs, port, container, script paths) are added by the
    server-lifecycle slice via a recipe registry.
    """

    token: str
    gateway_host: str = "127.0.0.1"
    gateway_port: int = 8001
    #: SQLite file for the job ledger ("" -> in-memory, tests only).
    job_store_path: str = ""
    #: Optional monitor URLs surfaced to the shell ("" = hide that tile).
    monitor_sparkdash_url: str = ""
    monitor_grafana_url: str = ""
    #: Optional default recipe id ("" = first recipe in the catalog).
    default_recipe: str = ""

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        """Build settings from an env mapping (defaults to ``os.environ``)."""
        data = env if env is not None else os.environ
        token = data.get("SPARKCTL_TOKEN", "")
        if not token:
            raise ConfigError("SPARKCTL_TOKEN env var must be set (non-empty)")
        gateway_port = int(data.get("GATEWAY_PORT", "8001"))
        return cls(
            token=token,
            gateway_host=data.get("GATEWAY_HOST", "127.0.0.1"),
            gateway_port=gateway_port,
            job_store_path=data.get("SPARKCTL_JOB_STORE", ""),
            monitor_sparkdash_url=data.get("MONITOR_SPARKDASH_URL", ""),
            monitor_grafana_url=data.get("MONITOR_GRAFANA_URL", ""),
            default_recipe=data.get("SPARKCTL_DEFAULT_RECIPE", ""),
        )
