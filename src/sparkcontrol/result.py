"""A minimal Result type to express fallible operations without exceptions.

The PRD interfaces (IAuthGate, IServerControl, INodeProbe, IJobRunner, IDeploy)
return ``Result[Value, Error]``. We keep it deliberately small so no third-party
dependency is required.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

__all__ = ["Result", "Ok", "Err", "is_ok"]

T = TypeVar("T")
E = TypeVar("E")


@dataclass(frozen=True)
class Ok(Generic[T]):
    value: T


@dataclass(frozen=True)
class Err(Generic[E]):
    error: E


Result = Ok[T] | Err[E]


def is_ok(result: Result[T, E]) -> bool:
    """True when *result* is an :class:`Ok`."""
    return isinstance(result, Ok)
