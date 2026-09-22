"""Reference-counted sharing for physical transport connections."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from typing import Generic, TypeVar

from .contracts import TransportLifecycle
from .errors import ClosedError, ValidationError

TransportT = TypeVar("TransportT", bound=TransportLifecycle)
_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class _PoolEntry(Generic[TransportT]):
    transport: TransportT
    leases: int


class TransportLease(Generic[TransportT]):
    """Explicit ownership token for one shared transport reference."""

    def __init__(
        self,
        pool: SharedTransportPool[TransportT],
        key: Hashable,
        transport: TransportT,
    ) -> None:
        self._pool = pool
        self._key = key
        self._transport: TransportT | None = transport
        self._lock = threading.Lock()

    @property
    def transport(self) -> TransportT:
        """Return the shared transport while this lease is active."""

        with self._lock:
            if self._transport is None:
                raise ClosedError("transport lease is closed")
            return self._transport

    def close(self) -> None:
        """Release this reference idempotently."""

        with self._lock:
            if self._transport is None:
                return
            transport = self._transport
            self._transport = None
        self._pool._release(self._key, transport)

    def __enter__(self) -> TransportT:
        return self.transport

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class SharedTransportPool(Generic[TransportT]):
    """Share exactly one open transport for each caller-defined channel key.

    Factories must construct transports without opening physical resources.
    The pool serializes factory, open and final close operations so an acquire
    cannot overlap the previous connection's shutdown. Failed opens are closed
    before their entries are removed. If cleanup also fails, the pool retains
    the transport without issuing a lease; a later acquire recovers that same
    object before any caller can use the channel again.
    """

    def __init__(self) -> None:
        self._entries: dict[Hashable, _PoolEntry[TransportT]] = {}
        self._lock = threading.RLock()

    def acquire(
        self, key: Hashable, factory: Callable[[], TransportT]
    ) -> TransportLease[TransportT]:
        """Acquire an open shared transport for ``key``."""

        if not callable(factory):
            raise ValidationError("factory must be callable")
        try:
            hash(key)
        except TypeError as error:
            raise ValidationError("transport pool key must be hashable") from error

        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                transport = factory()
                entry = _PoolEntry(transport=transport, leases=0)
                self._entries[key] = entry
                try:
                    transport.open()
                except BaseException as open_error:
                    try:
                        transport.close()
                    except BaseException as cleanup_error:
                        _LOGGER.warning(
                            "transport cleanup after pool open failure failed: %s",
                            cleanup_error,
                        )
                        # Keep ownership even when cleanup is interrupted. A
                        # user interrupt must not become an ordinary open error.
                        if isinstance(open_error, Exception) and not isinstance(
                            cleanup_error, Exception
                        ):
                            raise
                    else:
                        del self._entries[key]
                    raise
            elif entry.leases == 0:
                entry.transport.recover()
            entry.leases += 1
            return TransportLease(self, key, entry.transport)

    def lease_count(self, key: Hashable) -> int:
        """Return the active reference count for ``key``."""

        with self._lock:
            entry = self._entries.get(key)
            return 0 if entry is None else entry.leases

    def active_keys(self) -> tuple[Hashable, ...]:
        """Return keys currently retained by the pool."""

        with self._lock:
            return tuple(self._entries)

    def _release(self, key: Hashable, transport: TransportT) -> None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None or entry.transport is not transport:
                raise ClosedError("transport lease does not belong to this pool")
            if entry.leases <= 0:
                raise ClosedError("transport lease count is already zero")
            entry.leases -= 1
            if entry.leases > 0:
                return
            try:
                entry.transport.close()
            except Exception:
                # Keep the zero-reference entry so a later acquire recovers the
                # same object instead of opening a duplicate physical channel.
                raise
            else:
                del self._entries[key]


__all__ = ["SharedTransportPool", "TransportLease"]
