"""Subprocess-based :class:`SshClient` adapter (deployment wiring).

Wraps the system ``ssh`` binary — no asyncssh dependency. Reuses the host's
``~/.ssh/config`` (aliases, keys, known_hosts), so nothing about credentials
passes through this code. BatchMode keeps it non-interactive: auth failures
surface immediately instead of hanging for a password prompt.

Contract (matches ``SshClient`` in lifecycle):
- returns :class:`CommandResult` with the remote exit code / output
- raises ``ConnectionError`` when ssh itself fails (exit 255: unreachable
  host, refused, or key auth rejected) — recipes map that to UNREACHABLE
"""

from __future__ import annotations

import asyncio
import subprocess

from .lifecycle import CommandResult

_SSH_BASE = ("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10")


class SubprocessSshClient:
    """Run commands on a host via the system ssh binary (async wrapper)."""

    async def run(self, host: str, command: str, timeout: float = 60.0) -> CommandResult:
        def blocking() -> subprocess.CompletedProcess[str]:
            return subprocess.run(  # noqa: S603 - fixed argv, host from config
                [*_SSH_BASE, host, command],
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )

        try:
            proc = await asyncio.to_thread(blocking)
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f"ssh command timed out after {timeout}s: {command}") from exc
        if proc.returncode == 255:
            # ssh-level failure (unreachable/refused/auth) — never a script result
            raise ConnectionError(
                f"ssh to {host} failed (exit 255): {(proc.stderr or '').strip()[:200]}"
            )
        return CommandResult(
            exit_code=proc.returncode, stdout=proc.stdout, stderr=proc.stderr
        )
