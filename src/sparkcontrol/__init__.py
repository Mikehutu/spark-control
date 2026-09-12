"""Spark Control gateway — one-tap mobile start/stop for the DGX Spark cluster.

The gateway wraps existing per-recipe start/stop scripts over asyncssh and
exposes a tiny, tailnet-only HTTPS API consumed by a PWA. It never reimplements
the script logic; it only arbitrates which recipe runs on the canonical
node-a:8888 port.
"""
