"""Reference-counted sharing for physical transport connections."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Hashable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from .contracts import TransportLifecycle
from .errors import ClosedError, DeviceNotReadyError, ValidationError

TransportT = TypeVar("TransportT", bound=TransportLifecycle)
_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class _PoolEntry(Generic[TransportT]):
    transport: TransportT
    lease_tokens: set[object] = field(default_factory=set)

    @property
    def leases(self) -> int:
        return len(self.lease_tokens)


@dataclass(frozen=True, slots=True)
class _KeyOperation:
    owner: int


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
        self._cleanup_transport: TransportT | None = None
        self._entry: _PoolEntry[TransportT] | None = None
        self._token = object()
        self._close_operation: object | None = None
        self._lock = threading.Lock()

    @property
    def transport(self) -> TransportT:
        """Return the shared transport while this lease is active."""

        with self._lock:
            if self._transport is None:
                raise ClosedError("transport lease is closed")
            return self._transport

    def close(self) -> None:
        """Release once, retrying failed idle cleanup without reopening a channel."""

        operation = object()
        try:
            with self._lock:
                if self._close_operation is not None:
                    return
                transport = self._transport
                if transport is None:
                    transport = self._cleanup_transport
                if transport is None:
                    return
                self._close_operation = operation
                # Retain retry responsibility before invalidating access. A
                # token discard is idempotent even if the pool wait or the
                # physical close is interrupted.
                self._cleanup_transport = transport
                self._transport = None
            self._pool._release(self._key, transport, self._token, self._entry)
            with self._lock:
                self._entry = None
                self._cleanup_transport = None
        finally:
            with self._lock:
                if self._close_operation is operation:
                    self._close_operation = None

    def __enter__(self) -> TransportT:
        return self.transport

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class SharedTransportPool(Generic[TransportT]):
    """Share exactly one open transport for each caller-defined channel key.

    Factories must construct transports without opening physical resources.
    The pool serializes factory, open and final close operations for each key
    so an acquire cannot overlap the previous connection's shutdown. Metadata
    queries and other keys do not wait for transport callbacks. Failed opens
    are closed before their entries are removed. If cleanup also fails, the pool retains
    the transport without issuing a lease; a later acquire recovers that same
    object before any caller can use the channel again. Call ``close_idle(key)``
    to retry retained cleanup without acquiring or reopening a connection.
    """

    def __init__(self) -> None:
        self._entries: dict[Hashable, _PoolEntry[TransportT]] = {}
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._operations: dict[Hashable, _KeyOperation] = {}

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

        entry: _PoolEntry[TransportT] | None = None
        lease: TransportLease[TransportT] | None = None
        try:
            with self._key_operation(key):
                with self._lock:
                    entry = self._entries.get(key)
                if entry is None:
                    transport = factory()
                    entry = _PoolEntry(transport=transport)
                    with self._lock:
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
                            if isinstance(open_error, Exception) and not isinstance(
                                cleanup_error, Exception
                            ):
                                raise
                        else:
                            with self._lock:
                                del self._entries[key]
                        raise
                elif entry.leases == 0:
                    entry.transport.recover()
                lease = TransportLease(self, key, entry.transport)
                lease._entry = entry
                with self._lock:
                    entry.lease_tokens.add(lease._token)
            return lease
        except BaseException:
            # Cover construction, token insertion, reservation release, and
            # return handoff. A zero-lease entry remains owned by the pool and
            # can be closed with close_idle() or recovered by another acquire.
            if entry is not None and lease is not None:
                with self._lock:
                    entry.lease_tokens.discard(lease._token)
            raise

    def close_idle(self, key: Hashable) -> None:
        """Retry closing a retained zero-lease connection without reopening it.

        Missing entries and entries with active leases are left untouched.
        Close is serialized with acquire; a failure propagates and leaves the
        entry in the pool for another close or recovery attempt.
        """

        with self._key_operation(key):
            with self._lock:
                entry = self._entries.get(key)
                if entry is None or entry.leases > 0:
                    return
            entry.transport.close()
            with self._lock:
                del self._entries[key]

    def lease_count(self, key: Hashable) -> int:
        """Return the active reference count for ``key``."""

        with self._lock:
            entry = self._entries.get(key)
            return 0 if entry is None else entry.leases

    def active_keys(self) -> tuple[Hashable, ...]:
        """Return keys currently retained by the pool."""

        with self._lock:
            return tuple(self._entries)

    def _release(
        self,
        key: Hashable,
        transport: TransportT,
        token: object,
        expected_entry: _PoolEntry[TransportT] | None,
    ) -> None:
        with self._key_operation(key):
            with self._lock:
                entry = self._entries.get(key)
                if (
                    entry is None
                    or entry.transport is not transport
                    or entry is not expected_entry
                ):
                    return
                entry.lease_tokens.discard(token)
                if entry.leases > 0:
                    return
            # The per-key reservation, rather than the metadata lock, owns
            # this potentially blocking operation and its failed cleanup.
            entry.transport.close()
            with self._lock:
                del self._entries[key]

    @contextmanager
    def _key_operation(self, key: Hashable) -> Iterator[None]:
        operation = _KeyOperation(threading.get_ident())
        try:
            with self._condition:
                while key in self._operations:
                    if self._operations[key].owner == operation.owner:
                        raise DeviceNotReadyError(
                            "transport pool lifecycle operation already in progress "
                            "for this key"
                        )
                    self._condition.wait()
                self._operations[key] = operation
            yield
        finally:
            try:
                self._finish_key_operation(key, operation)
            except BaseException:
                # An interrupt during reservation retirement must not leave
                # same-key waiters asleep or make every later call reentrant.
                self._finish_key_operation(key, operation)
                raise

    def _finish_key_operation(self, key: Hashable, operation: _KeyOperation) -> None:
        with self._condition:
            try:
                if self._operations.get(key) is operation:
                    del self._operations[key]
            finally:
                self._condition.notify_all()


__all__ = ["SharedTransportPool", "TransportLease"]
