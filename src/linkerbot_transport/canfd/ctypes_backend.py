"""ctypes wrapper for the vendor CANFD dynamic library.

The vendor library documents two process-level entry points in
``动态库接口函数使用说明`` §3.1/3.2:

- ``LibCANbus_Init`` allocates global system resources.
- ``LibCANbus_Exit`` releases them and explicitly "prevents memory leaks".

Linux libraries exporting these entry points share one Init/Exit pair per loaded
``.so``. The verified legacy Linux device API manages its lifecycle internally;
it has neither entry point and exposes ``CAN_GetDevType`` instead of
``CAN_GetCHType``. Windows HCanbus.dll also manages its own lifecycle and
addresses each enumerated channel with one devNum.
This module keeps that vendor lifecycle reference count separate from the operating
system handle lifetime: loaded main-library and Linux dependency handles are
cached strongly for the rest of the process and are never explicitly unloaded.

Forking after a CANFDInterface is open is not supported: the refcount lives in
the parent's memory and child copies will desync. Call ``L30.close()`` before
``os.fork()`` if you must fork.
"""

import atexit
import ctypes
import logging
import os
import platform
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from linkerbot_transport.errors import TimeoutError, TransportError, ValidationError

from .types import CANFDConfigOptions, CANFDMessage, dlc_to_length

CANFD_DEFAULT_SEND_TIMEOUT_MS = 10
CANFD_DEFAULT_RECEIVE_TIMEOUT_MS = 10
CANFD_DEFAULT_MAX_RECEIVE_FRAMES = 64
CANFD_MAX_RECEIVE_FRAMES = 2500
CANFD_TRANSMIT_FRAME_COUNT = 1
# Vendor PDF §3.22: CANFD_Transmit's items parameter caps at 100 frames per
# call. We preallocate a buffer of this size so the batch path never has to
# malloc on the send hot path. Practical batches inside the dispatcher are
# bounded by SEND_BATCH_MAX (much smaller — typically 32).
CANFD_TRANSMIT_MAX_BATCH = 100
CANFD_EXTENDED_FRAME_FLAG = 1
CANFD_STANDARD_FRAME_FLAG = 0
CANFD_REMOTE_FRAME_DISABLED = 0
CANFD_STATUS_OK = 0
# Vendor V1.3 tables 1-3: transfer direction/failure and bus diagnostics.
_FRAME_TRANSMITTED = 0x01
_FRAME_FAILED = 0x02
# Normal bus (0x08) and empty TX mailbox (0x20) are not error indicators.
_BUS_DIAGNOSTIC_MASK = 0x01 | 0x02 | 0x04 | 0x10 | 0x40 | 0x80
CANFD_SCAN_NO_DEVICE = 0
CANFD_FRAME_DATA_SIZE = 64
_DEV_INFO_FIELD_LENGTH = 32
_C_STRING_TERMINATOR = b"\x00"
_ENV_LIBRARY_PATH = "LINKERBOT_CANFD_LIB"
_LINUX_LIBRARY_NAME = "libcanbus.so"
_WINDOWS_LIBRARY_NAME = "HCanbus.dll"
_LINUX_USB_LIBRARY_NAME = "libusb-1.0.so.0"
# One budget shared by all Python lock waits in a native close attempt.
_CLOSE_WAIT_TIMEOUT_S = 1.0

_logger = logging.getLogger(__name__)

_library_load_lock = threading.Lock()
_library_handle_cache: dict[str, Any] = {}
_dll_directory_handles: dict[str, Any] = {}
_dependency_load_lock = threading.Lock()
_dependency_preload_attempted: set[str] = set()
_dependency_handle_cache: dict[str, Any] = {}
_lifecycle_lock = threading.Lock()
_lifecycle_references: dict[str, set[object]] = {}
_lifecycle_lib: dict[str, Any] = {}
# Scanning can replace vendor-global channel tables. Coordinate it with opens
# and closes, and reject public enumeration while channels still own handles.
_enumeration_lock = threading.Lock()
_open_channel_counts: dict[str, int] = {}
_scanned_device_counts: dict[str, int] = {}
_finalize_safe = True


@dataclass
class _LifecycleReference:
    key: str
    owners: set[object]
    token: object = field(default_factory=object)

    @property
    def released(self) -> bool:
        return self.token not in self.owners


def _canonical_library_key(candidate: str | Path) -> str:
    """Return a stable process-cache key for a loader candidate.

    Filesystem candidates are normalized to absolute real paths so symlinks
    share one handle. Bare sonames/DLL names must remain loader-resolved; they
    are namespaced by platform so repeated system-loader requests still share
    one Python handle without pretending to know the loader's resolved path.
    """
    value = os.fspath(candidate)
    has_separator = os.sep in value or (os.altsep is not None and os.altsep in value)
    if isinstance(candidate, Path) or os.path.isabs(value) or has_separator:
        path = os.path.realpath(os.path.abspath(os.path.expanduser(value)))
        return os.path.normcase(path)
    return f"loader:{platform.system().lower()}:{os.path.normcase(value)}"


def _library_key(lib: Any) -> str:
    """Return a stable refcount key for the loaded vendor library.

    The native handle unifies a system-loader soname with its resolved absolute
    path, including when enumeration is followed by transport construction.
    Fakes without handles use a normalized path or their Python identity.
    """
    handle = getattr(lib, "_handle", None)
    if isinstance(handle, int) and handle:
        return f"native:{platform.system().lower()}:{handle}"
    name = getattr(lib, "_name", None)
    if name:
        try:
            return _canonical_library_key(str(name))
        except OSError:
            return str(name)
    return f"id:{id(lib)}"


def _scan_device_count(lib: Any) -> int:
    """Scan an idle library, or reuse its live snapshot under enumeration lock."""
    key = _library_key(lib)
    if _open_channel_counts.get(key, 0):
        count = _scanned_device_counts.get(key)
        if count is None:
            raise TransportError(
                "MiniCANFD has open channels without a scan snapshot; "
                "close all channels before rescanning"
            )
        return count
    _scanned_device_counts.pop(key, None)
    count = lib.CAN_ScanDevice()
    if count < CANFD_SCAN_NO_DEVICE:
        raise TransportError(f"CAN_ScanDevice failed with status {count}")
    _scanned_device_counts[key] = count
    return count


def _linux_uses_legacy_api(lib: Any) -> bool:
    """Recognize the verified legacy export set, rejecting partial lifecycles."""
    has_init = hasattr(lib, "LibCANbus_Init")
    has_exit = hasattr(lib, "LibCANbus_Exit")
    if has_init and has_exit:
        return False
    if (
        not has_init
        and not has_exit
        and hasattr(lib, "CAN_GetDevType")
        and not hasattr(lib, "CAN_GetCHType")
    ):
        return True
    missing = "LibCANbus_Exit" if has_init else "LibCANbus_Init"
    raise TransportError(
        f"vendor library is missing {missing}; expected a complete Init/Exit "
        "pair or the verified legacy Linux CAN_GetDevType API"
    )


def _lifecycle_acquire(lib: Any) -> _LifecycleReference | None:
    """Increment refcount, calling ``LibCANbus_Init`` on first acquire.

    Raises ``TransportError`` if ``LibCANbus_Init`` returns non-zero on the first
    call; the refcount is not bumped in that case so a later acquire will
    retry. Subsequent acquires for the same library skip the vendor call.
    A previous failed Exit is retried before starting a new lifecycle.
    """
    if _linux_uses_legacy_api(lib):
        return None
    key = _library_key(lib)
    with _lifecycle_lock:
        owners = _lifecycle_references.get(key)
        if not owners:
            pending = _lifecycle_lib.get(key)
            if pending is not None:
                error = _exit_library(pending)
                if error is not None:
                    raise error
                _lifecycle_lib.pop(key, None)
            status = lib.LibCANbus_Init()
            if status != CANFD_STATUS_OK:
                raise TransportError(f"LibCANbus_Init failed with status {status}")
            _lifecycle_lib[key] = lib
        if owners is None:
            owners = set()
            _lifecycle_references[key] = owners
        ref = _LifecycleReference(key, owners)
        owners.add(ref.token)
        return ref


@contextmanager
def _close_lock_before(lock: Any, deadline: float | None, phase: str) -> Iterator[None]:
    acquired = (
        lock.acquire()
        if deadline is None
        else lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
    )
    if not acquired:
        raise TimeoutError(
            f"MiniCANFD close timed out waiting for {phase}; "
            "cleanup ownership retained, retry close() after the operation returns"
        )
    try:
        yield
    finally:
        lock.release()


def _lifecycle_release(
    ref: _LifecycleReference | None, *, deadline: float | None = None
) -> TransportError | None:
    """Release once and retry pending Exit without consuming another reference.

    An Exit failure is returned and logged. Both a repeated close and the next
    acquire can retry cleanup. A released reference cannot decrement references
    belonging to interfaces opened after its failed cleanup was recovered.
    """
    if ref is None or not _finalize_safe:
        return
    with _close_lock_before(_lifecycle_lock, deadline, "library lifecycle"):
        key = ref.key
        owners = _lifecycle_references.get(key)
        if owners is not None and owners is not ref.owners:
            # A newer Init owns this key; a stale released reference cannot
            # decrement it or call Exit underneath its active users.
            return None
        ref.owners.discard(ref.token)
        if ref.owners:
            return None
        _lifecycle_references.pop(key, None)
        lib = _lifecycle_lib.get(key)
        if lib is None:
            return
        # Keep acquire/release serialized through the vendor Exit call. If the
        # lock were released first, another thread could call Init and then have
        # this stale Exit tear down the resources underneath its live interface.
        error = _exit_library(lib)
        if error is None:
            _lifecycle_lib.pop(key, None)
        else:
            _logger.warning("%s", error)
        return error


def _exit_library(lib: Any) -> TransportError | None:
    """Release vendor resources without forgetting a failed cleanup."""
    try:
        status = lib.LibCANbus_Exit()
    except Exception as error:
        return TransportError(f"LibCANbus_Exit failed: {error}", cause=error)
    if status != CANFD_STATUS_OK:
        return TransportError(f"LibCANbus_Exit failed with status {status}")
    return None


def _lifecycle_snapshot() -> dict[str, int]:
    """Return a copy of the current refcount table (testing helper)."""
    with _lifecycle_lock:
        return {key: len(owners) for key, owners in _lifecycle_references.items()}


class CanFDConfig(ctypes.Structure):
    """Vendor CANFD initialization structure."""

    _fields_ = [
        ("NomBaud", ctypes.c_uint),
        ("DatBaud", ctypes.c_uint),
        ("NomPre", ctypes.c_ushort),
        ("NomTseg1", ctypes.c_ubyte),
        ("NomTseg2", ctypes.c_ubyte),
        ("NomSJW", ctypes.c_ubyte),
        ("DatPre", ctypes.c_ubyte),
        ("DatTseg1", ctypes.c_ubyte),
        ("DatTseg2", ctypes.c_ubyte),
        ("DatSJW", ctypes.c_ubyte),
        ("Config", ctypes.c_ubyte),
        ("Model", ctypes.c_ubyte),
        ("Cantype", ctypes.c_ubyte),
    ]


class CanFDMsg(ctypes.Structure):
    """Vendor CANFD frame structure."""

    _fields_ = [
        ("ID", ctypes.c_uint),
        ("TimeStamp", ctypes.c_uint),
        ("FrameType", ctypes.c_ubyte),
        ("DLC", ctypes.c_ubyte),
        ("ExternFlag", ctypes.c_ubyte),
        ("RemoteFlag", ctypes.c_ubyte),
        ("BusSatus", ctypes.c_ubyte),
        ("ErrSatus", ctypes.c_ubyte),
        ("TECounter", ctypes.c_ubyte),
        ("RECounter", ctypes.c_ubyte),
        ("Data", ctypes.c_ubyte * 64),
    ]


class DevInfo(ctypes.Structure):
    """USB-CANFD adapter information returned by the vendor library."""

    _fields_ = [
        ("HW_Type", ctypes.c_char * _DEV_INFO_FIELD_LENGTH),
        ("HW_Ser", ctypes.c_char * _DEV_INFO_FIELD_LENGTH),
        ("HW_Ver", ctypes.c_char * _DEV_INFO_FIELD_LENGTH),
        ("FW_Ver", ctypes.c_char * _DEV_INFO_FIELD_LENGTH),
        ("MF_Date", ctypes.c_char * _DEV_INFO_FIELD_LENGTH),
    ]


class CANFDInterface:
    """Low-level CANFD interface backed by libcanbus.so or HCanbus.dll.

    Concurrency model (changed in fix#141):

    - ``send()`` is protected by ``_tx_lock`` only — the vendor USB write
      endpoint is single-writer.
    - ``receive()`` is protected by ``_rx_lock`` only — the vendor USB read
      endpoint is single-reader. The two locks are independent so a long-
      running receive (the dispatcher's 10 ms timeout) never blocks send.
    - Transition of the open/closed flag is guarded by ``_state_lock`` and
      atomically reflected in ``_closed_event``. Every public method consults
      the event at entry, so a close cleanly cancels future calls without
      racing the per-direction locks. ``_close_lock`` serializes complete close
      attempts; ``_close_complete_event`` records when the current attempt has
      finished for diagnostics and lifecycle tests.

    Close contract (mandatory ordering, encoded in :meth:`close`):

    1. Under ``_state_lock``, flip ``_closed_event``. Subsequent
       ``send/receive/read_dev_info`` calls fail fast.
    2. Briefly acquire ``_tx_lock`` then ``_rx_lock``. Any in-flight call from
       another thread has the matching lock; reacquiring forces ``close()`` to
       wait until the vendor call returns. All Python lock waits in one close
       attempt share a one-second budget. Expiry reports a retryable timeout,
       retaining resources still owned by in-flight operations.
    3. Call ``CAN_CloseDevice``. A failed call retains ownership so a later
       ``close()`` can retry it.
    4. After the first successful device close, release the lifecycle refcount
       and notify concurrent close callers.

    When a dispatcher owns the interface, ``dispatcher.stop()`` first gives
    both workers a bounded join and then requests interface close. A native
    I/O call that ignores its timeout is never interrupted by concurrent
    resource release. Native close/Exit calls themselves cannot be preempted.
    """

    def __init__(
        self,
        device_index: int = 0,
        channel_index: int = 0,
        library_path: str | Path | None = None,
        config: CANFDConfigOptions | None = None,
    ) -> None:
        """Open and initialize a CANFD adapter channel.

        Lifecycle-aware Linux libraries hold an Init/Exit reference while open.
        Legacy Linux and Windows libraries manage their own lifecycle.
        A failed initialization closes its channel; if
        cleanup also fails, the exception retains the interface for managed
        close/recover instead of releasing ownership prematurely.
        """
        for value, name in (
            (device_index, "device_index"),
            (channel_index, "channel_index"),
        ):
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or not 0 <= value <= 0xFFFFFFFF
            ):
                raise ValidationError(f"{name} must be an unsigned 32-bit integer")
        self._windows = platform.system() == "Windows"
        if self._windows and channel_index != 0:
            raise ValidationError(
                "HCanbus.dll uses one enumerated channel number: set device_index "
                "to devNum and leave channel_index=0"
            )

        self._device_index = device_index
        self._channel_index = channel_index
        self._channel_args = (
            (device_index,) if self._windows else (device_index, channel_index)
        )
        self._device_open = False
        self._config = config or CANFDConfigOptions()
        # Independent locks per direction. send and receive run truly
        # concurrently — they only contend at the per-direction lock, which
        # is itself uncontended in the dispatcher's one-thread-per-direction
        # model. See close contract in the class docstring.
        self._tx_lock = threading.Lock()
        self._rx_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._closed_event = threading.Event()
        self._close_complete_event = threading.Event()
        self._close_succeeded = False
        # Marks "uninitialized" before _open returns; flipped to False on
        # successful open and consulted by close to short-circuit a double-
        # close on a never-opened instance.
        self._closed_event.set()
        self._close_complete_event.set()
        self._lifecycle_ref: _LifecycleReference | None = None
        self._pending_receive_buffer: ctypes.Array[CanFDMsg] | None = None
        self._pending_receive_index = 0
        self._pending_receive_count = 0
        self._pending_receive_error: Exception | None = None
        self._lib = _load_library(library_path)
        _bind_signatures(self._lib, windows=self._windows)
        # Hot-path zero-alloc buffers. The TX frame is reused across every
        # send under _tx_lock; the TX batch buffer is reused across every
        # send_batch under the same lock; the RX buffer is reused across
        # every receive under _rx_lock. Pre-allocating them eliminates
        # ~5 KB of ctypes allocation per receive cycle (100 Hz) and replaces
        # a Python for loop with ctypes.memmove on the send path.
        self._tx_frame = CanFDMsg()
        self._tx_batch = (CanFDMsg * CANFD_TRANSMIT_MAX_BATCH)()
        self._rx_buffer = (CanFDMsg * CANFD_DEFAULT_MAX_RECEIVE_FRAMES)()
        # acquire the vendor lifecycle refcount before any device-level call;
        # release it if anything below this point raises so we don't leak the
        # Init reference on a partially-initialized interface.
        if not self._windows:
            self._lifecycle_ref = _lifecycle_acquire(self._lib)
        try:
            self._open()
        except BaseException as error:
            try:
                self.close()
            except BaseException as cleanup_error:
                _logger.exception(
                    "CAN FD initialization cleanup failed; close must retry"
                )
                setattr(error, "_failed_interface", self)
                if isinstance(error, Exception) and not isinstance(
                    cleanup_error, Exception
                ):
                    setattr(cleanup_error, "_failed_interface", self)
                    raise cleanup_error from error
            raise

    @property
    def device_index(self) -> int:
        """Adapter device index passed to the vendor library."""
        return self._device_index

    @property
    def channel_index(self) -> int:
        """CANFD channel index passed to the vendor library."""
        return self._channel_index

    @property
    def config(self) -> CANFDConfigOptions:
        """CANFD configuration used to initialize this interface."""
        return self._config

    def _open(self) -> None:
        # Instance I/O locks are unnecessary before workers receive this object.
        # The process-wide guard prevents enumeration racing the vendor open.
        with _enumeration_lock:
            self._open_locked()

    def _open_locked(self) -> None:
        if (
            not self._windows
            and _linux_uses_legacy_api(self._lib)
            and self._channel_index != 0
        ):
            raise ValidationError(
                "legacy Linux MiniCANFD supports only confirmed channel 0; "
                "the device-level type query does not describe other channels"
            )
        count = _scan_device_count(self._lib)
        if count == CANFD_SCAN_NO_DEVICE:
            raise TransportError("No CANFD adapter found")
        if self._device_index >= count:
            raise TransportError(
                f"CANFD device_index {self._device_index} out of range; "
                f"found {count} adapter(s) in the scan snapshot; "
                "close all channels before rescanning changed USB topology"
            )

        status = self._lib.CAN_OpenDevice(*self._channel_args)
        if status != CANFD_STATUS_OK:
            raise TransportError(f"CAN_OpenDevice failed with status {status}")
        self._device_open = True
        key = _library_key(self._lib)
        _open_channel_counts[key] = _open_channel_counts.get(key, 0) + 1
        config = self._build_ctypes_config()
        status = self._lib.CANFD_Init(*self._channel_args, ctypes.byref(config))
        if status != CANFD_STATUS_OK:
            raise TransportError(f"CANFD_Init failed with status {status}")

        # _open is called from __init__ before the object is published.
        self._close_complete_event.set()
        self._closed_event.clear()

    def _build_ctypes_config(self) -> CanFDConfig:
        return CanFDConfig(
            NomBaud=self._config.nom_baud,
            DatBaud=self._config.dat_baud,
            NomPre=0,
            NomTseg1=0,
            NomTseg2=0,
            NomSJW=0,
            DatPre=0,
            DatTseg1=0,
            DatTseg2=0,
            DatSJW=0,
            Config=self._config.config,
            Model=self._config.model,
            Cantype=self._config.cantype,
        )

    def send(
        self,
        message: CANFDMessage,
        timeout_ms: int = CANFD_DEFAULT_SEND_TIMEOUT_MS,
    ) -> None:
        """Transmit one CANFD frame through the vendor library.

        Holds ``_tx_lock`` only — independent of receive, so a long-running
        receive timeout does not block transmits. Reuses ``self._tx_frame``
        to avoid allocating a fresh ``CanFDMsg`` on every send.
        """
        if timeout_ms < 0:
            raise ValidationError("timeout_ms must be non-negative")
        # Fail fast if close has been initiated; we may still race with close
        # acquiring _tx_lock, but the lock acquisition itself is bounded by
        # the vendor send timeout (10 ms default) so close drains cleanly.
        self._ensure_open()
        with self._tx_lock:
            self._ensure_open()
            self._fill_tx_frame(message)
            sent = self._lib.CANFD_Transmit(
                *self._channel_args,
                ctypes.byref(self._tx_frame),
                CANFD_TRANSMIT_FRAME_COUNT,
                timeout_ms,
            )
            if sent != CANFD_TRANSMIT_FRAME_COUNT:
                raise TransportError(
                    "CANFD_Transmit failed: expected "
                    f"{CANFD_TRANSMIT_FRAME_COUNT} frame, got {sent}"
                )

    def send_batch(
        self,
        messages: list[CANFDMessage],
        timeout_ms: int = CANFD_DEFAULT_SEND_TIMEOUT_MS,
    ) -> None:
        """Transmit multiple CANFD frames in a single vendor call.

        Vendor PDF §3.22 documents that ``CANFD_Transmit`` accepts an array
        of up to 100 frames in one call. At 17-joint PVT @ 100 Hz this turns
        3 USB control transfers per cycle (position+speed+torque) into 1,
        cutting USB protocol overhead by roughly 3×.

        Order is preserved on the wire: frame ``messages[i]`` is transmitted
        before ``messages[j]`` for ``i < j``. This matters for protocols
        like L30 where the device expects target updates in a specific
        sequence within a control cycle.

        ``messages[i]`` is filled into preallocated slot ``self._tx_batch[i]``
        so this path stays alloc-free for batches up to ``CANFD_TRANSMIT_MAX_BATCH``.

        Args:
            messages: Frames to transmit, in order. Length must be in
                ``[0, CANFD_TRANSMIT_MAX_BATCH]``. Empty list is a no-op.
            timeout_ms: Vendor send timeout for the batched call.

        Raises:
            ValidationError: If ``len(messages) > CANFD_TRANSMIT_MAX_BATCH``
                or ``timeout_ms`` is negative.
            TransportError: If the interface is closed or the vendor library
                reports a short send (``sent != len(messages)``).
        """
        if timeout_ms < 0:
            raise ValidationError("timeout_ms must be non-negative")
        count = len(messages)
        if count == 0:
            return
        if count > CANFD_TRANSMIT_MAX_BATCH:
            raise ValidationError(
                f"send_batch supports at most {CANFD_TRANSMIT_MAX_BATCH} "
                f"frames per call (vendor limit), got {count}"
            )
        if count == 1:
            # Reuse the single-frame fast path which writes into _tx_frame
            # and avoids touching the larger batch buffer.
            self.send(messages[0], timeout_ms)
            return

        self._ensure_open()
        with self._tx_lock:
            self._ensure_open()
            for index, message in enumerate(messages):
                self._fill_frame(self._tx_batch[index], message)
            sent = self._lib.CANFD_Transmit(
                *self._channel_args,
                self._tx_batch,
                count,
                timeout_ms,
            )
            if sent != count:
                raise TransportError(
                    f"CANFD_Transmit batch short send: expected {count}, got {sent}"
                )

    def receive(
        self,
        max_frames: int = CANFD_DEFAULT_MAX_RECEIVE_FRAMES,
        timeout_ms: int = CANFD_DEFAULT_RECEIVE_TIMEOUT_MS,
    ) -> list[CANFDMessage]:
        """Deliver up to max_frames data frames in native receive order.

        Holds ``_rx_lock`` only — independent of send. The vendor library is
        single-reader per channel, so the lock prevents accidental concurrent
        callers from corrupting its internal ring buffer.

        Reuses ``self._rx_buffer`` when ``max_frames`` fits the preallocated
        capacity (the common case — the dispatcher uses the default). Larger
        requests allocate a fresh array. Returned messages are independent
        immutable copies, so callers may keep them after the next receive
        overwrites the buffer. RTR and transmit echoes have no device reply.

        Before reporting a later frame error, deliver the valid prefix. The
        error is raised on the next valid call, before consuming the retained
        batch suffix or reading the driver again. At most one native batch is
        retained, and a smaller max_frames also bounds delivery of that suffix.
        Successful data carrying a bus diagnostic is delivered before reporting
        that diagnostic; the snapshot does not make its payload invalid.
        """
        if (
            not isinstance(max_frames, int)
            or isinstance(max_frames, bool)
            or not 1 <= max_frames <= CANFD_MAX_RECEIVE_FRAMES
        ):
            raise ValidationError(
                f"max_frames must be between 1 and {CANFD_MAX_RECEIVE_FRAMES}"
            )
        if timeout_ms < 0:
            raise ValidationError("timeout_ms must be non-negative")

        self._ensure_open()
        with self._rx_lock:
            self._ensure_open()
            if self._pending_receive_error is not None:
                error = self._pending_receive_error
                self._pending_receive_error = None
                raise error
            if self._pending_receive_buffer is None:
                if max_frames <= CANFD_DEFAULT_MAX_RECEIVE_FRAMES:
                    buffer = self._rx_buffer
                else:
                    buffer = (CanFDMsg * max_frames)()
                received = self._lib.CANFD_Receive(
                    *self._channel_args,
                    buffer,
                    max_frames,
                    timeout_ms,
                )
                if received < CANFD_STATUS_OK:
                    raise TransportError(f"CANFD_Receive failed with status {received}")
                if received == CANFD_STATUS_OK:
                    return []
                if received > max_frames:
                    raise TransportError(
                        "CANFD_Receive returned more frames than requested: "
                        f"requested {max_frames}, got {received}"
                    )
                self._pending_receive_buffer = buffer
                self._pending_receive_index = 0
                self._pending_receive_count = received

            result = []
            remaining = self._pending_receive_count - self._pending_receive_index
            try:
                for _ in range(min(max_frames, remaining)):
                    frame = self._pending_receive_buffer[self._pending_receive_index]
                    # Consume exactly once, even when decoding raises. The
                    # native batch has already left the adapter's queue.
                    self._pending_receive_index += 1
                    message = self._frame_to_message(frame)
                    if message is not None:
                        result.append(message)
                    if frame.BusSatus & _BUS_DIAGNOSTIC_MASK or frame.ErrSatus:
                        raise self._frame_receive_error(frame)
            except Exception as error:
                if not result:
                    raise
                # Retain the exception without its receive-frame locals, which
                # would otherwise keep an exhausted large native array alive.
                self._pending_receive_error = error.with_traceback(None)
            finally:
                if self._pending_receive_index == self._pending_receive_count:
                    self._pending_receive_buffer = None
                    self._pending_receive_index = self._pending_receive_count = 0
            return result

    def read_dev_info(self) -> dict[str, str]:
        """Read USB-CANFD adapter information from the vendor library.

        Uses ``_tx_lock`` because the vendor library treats CAN_ReadDevInfo as
        another control-endpoint write.
        """
        self._ensure_open()
        with self._tx_lock:
            self._ensure_open()
            info = DevInfo()
            status = self._lib.CAN_ReadDevInfo(self._device_index, ctypes.byref(info))
            if status != CANFD_STATUS_OK:
                raise TransportError(f"CAN_ReadDevInfo failed with status {status}")
            return {
                "hw_type": _decode_c_string(info.HW_Type),
                "hw_serial": _decode_c_string(info.HW_Ser),
                "hw_version": _decode_c_string(info.HW_Ver),
                "fw_version": _decode_c_string(info.FW_Ver),
                "manufacture_date": _decode_c_string(info.MF_Date),
            }

    def close(self) -> None:
        """Close the CANFD adapter channel and release the vendor refcount.

        Mandatory ordering (see class docstring):

        1. Flip ``_closed_event`` under ``_state_lock``; from this point any
           future send/receive call from any thread fails fast.
        2. Drain in-flight send/receive by acquiring ``_tx_lock`` and
           ``_rx_lock``. Concurrent-close, I/O, enumeration and lifecycle lock
           waits share one budget; expiry raises ``TimeoutError`` without
           releasing resources still in use. New I/O stays rejected.
        3. Call ``CAN_CloseDevice``.
        4. On success, release the lifecycle refcount and notify concurrent
           close callers.

        ``CAN_CloseDevice`` failures reject future I/O but retain device and
        lifecycle ownership so a later ``close()`` can retry the vendor call.
        A later ``LibCANbus_Exit`` failure is also reported and retryable without
        closing the already released channel or decrementing its reference twice.
        Native cleanup calls themselves cannot be preempted in this process;
        the deadline bounds Python lock waits, not vendor close/Exit execution.
        """
        # Serialize the full transaction. A caller arriving after a failed
        # attempt becomes the next owner and retries CAN_CloseDevice instead of
        # confusing "I/O rejected" with "vendor resources released".
        deadline = time.monotonic() + _CLOSE_WAIT_TIMEOUT_S
        with _close_lock_before(self._close_lock, deadline, "another close attempt"):
            if self._close_succeeded:
                return
            with _close_lock_before(self._state_lock, deadline, "interface state"):
                self._closed_event.set()
            self._close_complete_event.clear()
            try:
                # Step 2: drain. Acquiring then immediately releasing each
                # direction lock waits out any in-flight vendor call.
                with _close_lock_before(self._tx_lock, deadline, "transmit I/O"):
                    pass
                with _close_lock_before(self._rx_lock, deadline, "receive I/O"):
                    # Once admission is closed, old data and deferred errors
                    # must not survive even a failed native-close attempt.
                    self._pending_receive_buffer = None
                    self._pending_receive_index = self._pending_receive_count = 0
                    self._pending_receive_error = None

                # Step 3: actually close the vendor side.
                if self._device_open:
                    with _close_lock_before(
                        _enumeration_lock, deadline, "library enumeration"
                    ):
                        try:
                            close_status = self._lib.CAN_CloseDevice(
                                *self._channel_args
                            )
                        except Exception as error:
                            raise TransportError(
                                f"CAN_CloseDevice failed: {error}"
                            ) from error
                        if close_status != CANFD_STATUS_OK:
                            raise TransportError(
                                f"CAN_CloseDevice failed with status {close_status}"
                            )
                        self._device_open = False
                        key = _library_key(self._lib)
                        remaining = _open_channel_counts[key] - 1
                        if remaining:
                            _open_channel_counts[key] = remaining
                        else:
                            _open_channel_counts.pop(key)
                            _scanned_device_counts.pop(key, None)

                # Step 4: a successful vendor close commits lifecycle release.
                if self._lifecycle_ref is not None:
                    cleanup_error = _lifecycle_release(
                        self._lifecycle_ref, deadline=deadline
                    )
                    if cleanup_error is not None:
                        raise cleanup_error
                    self._lifecycle_ref = None
                self._close_succeeded = True
            finally:
                # Publish the end of every attempt. Waiters remain serialized by
                # _close_lock and retry if this attempt did not commit success.
                self._close_complete_event.set()

    def _ensure_open(self) -> None:
        if self._closed_event.is_set():
            raise TransportError("CANFD interface is closed")

    def _fill_frame(self, target: "CanFDMsg", message: CANFDMessage) -> None:
        """Populate ``target`` (a vendor ``CanFDMsg`` slot) for transmission.

        Generic helper shared by single-frame :meth:`send` (which writes into
        the preallocated ``self._tx_frame``) and batch :meth:`send_batch`
        (which writes into successive slots of ``self._tx_batch``). Uses
        ``ctypes.memmove`` for the payload instead of a Python for-loop —
        at 17×PVT @ 100 Hz this is the difference between ~50 µs and ~5 µs
        of CPU per frame. The tail beyond ``len(data)`` is zeroed so the
        vendor library never observes stale bytes from a previous frame's
        DLC padding.
        """
        frame_type = (
            message.frame_type
            if message.frame_type is not None
            else self._config.frame_type
        )
        if frame_type & 0x08 and not frame_type & 0x04:
            raise ValidationError("FrameType BRS bit requires the CAN FD bit")
        if not frame_type & 0x04 and message.dlc is not None and message.dlc > 8:
            raise ValidationError("classic CAN frames cannot use a DLC greater than 8")
        target.ID = message.arbitration_id
        target.TimeStamp = 0
        target.FrameType = frame_type
        target.DLC = message.dlc or 0
        target.ExternFlag = (
            CANFD_EXTENDED_FRAME_FLAG
            if message.is_extended_id
            else CANFD_STANDARD_FRAME_FLAG
        )
        target.RemoteFlag = CANFD_REMOTE_FRAME_DISABLED
        target.BusSatus = CANFD_STATUS_OK
        target.ErrSatus = CANFD_STATUS_OK
        target.TECounter = 0
        target.RECounter = 0
        data = message.data
        length = len(data)
        data_addr = ctypes.addressof(target.Data)
        if length:
            ctypes.memmove(data_addr, data, length)
        if length < CANFD_FRAME_DATA_SIZE:
            ctypes.memset(data_addr + length, 0, CANFD_FRAME_DATA_SIZE - length)

    def _fill_tx_frame(self, message: CANFDMessage) -> None:
        """Compatibility shim: fill the preallocated ``_tx_frame``."""
        self._fill_frame(self._tx_frame, message)

    def _message_to_frame(self, message: CANFDMessage) -> CanFDMsg:
        """Build a stand-alone ``CanFDMsg`` (compat path).

        The hot send path uses :meth:`_fill_tx_frame` against the preallocated
        ``_tx_frame``. This wrapper is preserved so test suites that monkeypatch
        the ctypes layer or call the helper directly keep working.
        """
        frame = CanFDMsg()
        saved = self._tx_frame
        self._tx_frame = frame
        try:
            self._fill_tx_frame(message)
        finally:
            self._tx_frame = saved
        return frame

    def _frame_to_message(self, frame: CanFDMsg) -> CANFDMessage | None:
        if frame.FrameType & _FRAME_FAILED:
            raise self._frame_receive_error(frame)
        if frame.RemoteFlag or frame.FrameType & _FRAME_TRANSMITTED:
            return None
        length = dlc_to_length(frame.DLC)
        # ctypes.string_at copies length bytes in a single C call; the previous
        # ``bytes(frame.Data[:length])`` materializes a Python list first which
        # is multiple times slower at 64-byte payloads.
        if length:
            payload = ctypes.string_at(ctypes.addressof(frame.Data), length)
        else:
            payload = b""
        return CANFDMessage(
            arbitration_id=frame.ID,
            data=payload,
            dlc=frame.DLC,
            is_extended_id=bool(frame.ExternFlag),
            frame_type=frame.FrameType,
            timestamp=frame.TimeStamp,
        )

    def _frame_receive_error(self, frame: CanFDMsg) -> TransportError:
        return TransportError(
            "MiniCANFD receive diagnostic: "
            f"device_index={self._device_index}, channel_index={self._channel_index}, "
            f"ID=0x{frame.ID:x}, TimeStamp={frame.TimeStamp}, "
            f"FrameType=0x{frame.FrameType:02x}, BusSatus=0x{frame.BusSatus:02x}, "
            f"ErrSatus=0x{frame.ErrSatus:02x}, "
            f"TECounter={frame.TECounter}, RECounter={frame.RECounter}"
        )

    def __enter__(self) -> "CANFDInterface":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager and close the CANFD interface."""
        self.close()

    def __del__(self) -> None:
        """Best-effort close on garbage collection.

        ``__del__`` runs at unpredictable times — possibly during interpreter
        shutdown when ``ctypes`` or the vendor library is already unloaded.
        Swallow every exception so we never produce ``Exception ignored in
        __del__`` noise; the module-level ``_finalize_safe`` flag short-circuits
        the lifecycle release path during shutdown.
        """
        try:
            closed = getattr(self, "_closed_event", None)
            if closed is None or getattr(self, "_close_succeeded", False):
                return
            if (
                not getattr(self, "_device_open", False)
                and getattr(self, "_lifecycle_ref", None) is None
            ):
                return
            self.close()
        except BaseException:
            pass


def _load_library(library_path: str | Path | None) -> Any:
    _preload_vendor_dependencies()
    errors: list[str] = []
    loader = _ctypes_loader()
    for candidate in _library_candidates(library_path):
        # Resolve selected files before caching or loading. A relative basename
        # passed to dlopen would search system directories instead of that file.
        # Bare string candidates intentionally retain system-loader semantics.
        if isinstance(candidate, Path):
            candidate = Path(
                os.path.realpath(os.path.abspath(os.path.expanduser(candidate)))
            )
        key = _canonical_library_key(candidate)
        with _library_load_lock:
            cached = _library_handle_cache.get(key)
            if cached is not None:
                return cached
            directory_handle = None
            directory = ""
            try:
                load_path = str(candidate)
                if platform.system() == "Windows" and isinstance(candidate, Path):
                    directory = str(Path(load_path).parent)
                    add_directory = getattr(os, "add_dll_directory", None)
                    if (
                        add_directory is not None
                        and directory not in _dll_directory_handles
                    ):
                        directory_handle = add_directory(directory)
                lib = loader(load_path)
            except OSError as error:
                if directory_handle is not None:
                    directory_handle.close()
                errors.append(f"{candidate}: {error}")
                continue
            except BaseException:
                if directory_handle is not None:
                    directory_handle.close()
                raise
            if directory_handle is not None:
                _dll_directory_handles[directory] = directory_handle
            _library_handle_cache[key] = lib
            return lib

    attempted = "\n".join(errors) if errors else "no candidates"
    platform_hint = ""
    if platform.system() == "Windows":
        platform_hint = (
            f" Python is {ctypes.sizeof(ctypes.c_void_p) * 8}-bit: use the matching "
            "HCanbus.dll, install the vendor USB driver and Visual C++ runtime, "
            "and keep dependency DLLs alongside it."
        )
    raise TransportError(
        "Unable to load CANFD vendor library. The system loader and installation "
        "directories are tried by default; the SDK does not add the current "
        "working directory. "
        "pass library_path=... or set "
        f"{_ENV_LIBRARY_PATH} to the libcanbus.so/HCanbus.dll path if the library is "
        f"not installed in a system search path.{platform_hint} Attempted:\n{attempted}"
    )


def _preload_vendor_dependencies() -> None:
    if platform.system() != "Linux":
        return
    key = _canonical_library_key(_LINUX_USB_LIBRARY_NAME)
    with _dependency_load_lock:
        if key in _dependency_preload_attempted:
            return
        _dependency_preload_attempted.add(key)
        try:
            handle = ctypes.CDLL(_LINUX_USB_LIBRARY_NAME, mode=ctypes.RTLD_GLOBAL)
        except OSError:
            return
        _dependency_handle_cache[key] = handle


def _library_candidates(library_path: str | Path | None) -> list[str | Path]:
    library_name = _default_library_name()
    candidates: list[str | Path] = []
    if library_path is not None:
        return [Path(library_path)]
    env_path = os.environ.get(_ENV_LIBRARY_PATH)
    if env_path:
        return [Path(env_path)]

    candidates.append(library_name)

    package_root = Path(__file__).resolve().parents[2]
    if platform.system() != "Windows":
        candidates.append(Path("/usr/local/lib") / library_name)
    candidates.append(package_root / "vendor" / "libcanbus" / library_name)
    candidates.append(package_root / "hand" / library_name)
    return candidates


def _default_library_name() -> str:
    return (
        _WINDOWS_LIBRARY_NAME if platform.system() == "Windows" else _LINUX_LIBRARY_NAME
    )


def _ctypes_loader() -> Any:
    if platform.system() == "Windows":
        # WinDLL is intentionally resolved lazily because ctypes does not
        # expose it on non-Windows hosts where this module is type-checked.
        return getattr(ctypes, "WinDLL")  # noqa: B009
    return ctypes.CDLL


def _bind_signatures(lib: Any, *, windows: bool | None = None) -> None:
    if windows is None:
        windows = platform.system() == "Windows"
    required = (
        "CAN_ScanDevice",
        "CAN_OpenDevice",
        "CAN_CloseDevice",
        "CAN_ReadDevInfo",
        "CANFD_Init",
        "CANFD_Transmit",
        "CANFD_Receive",
    )
    for name in required:
        if not hasattr(lib, name):
            raise TransportError(f"incompatible CAN FD vendor library: missing {name}")
    channel_types = [ctypes.c_uint] if windows else [ctypes.c_uint, ctypes.c_uint]
    # Linux PDF §3.1/3.2. These entry points do not exist in Windows HCanbus.dll.
    if not windows and hasattr(lib, "LibCANbus_Init"):
        lib.LibCANbus_Init.restype = ctypes.c_int
        lib.LibCANbus_Init.argtypes = []
    if not windows and hasattr(lib, "LibCANbus_Exit"):
        lib.LibCANbus_Exit.restype = ctypes.c_int
        lib.LibCANbus_Exit.argtypes = []

    lib.CAN_ScanDevice.restype = ctypes.c_int
    lib.CAN_ScanDevice.argtypes = []

    lib.CAN_OpenDevice.restype = ctypes.c_int
    lib.CAN_OpenDevice.argtypes = channel_types

    lib.CAN_CloseDevice.restype = ctypes.c_int
    lib.CAN_CloseDevice.argtypes = channel_types

    lib.CAN_ReadDevInfo.restype = ctypes.c_int
    lib.CAN_ReadDevInfo.argtypes = [ctypes.c_uint, ctypes.POINTER(DevInfo)]

    lib.CANFD_Init.restype = ctypes.c_int
    lib.CANFD_Init.argtypes = [
        *channel_types,
        ctypes.POINTER(CanFDConfig),
    ]

    lib.CANFD_Transmit.restype = ctypes.c_int
    lib.CANFD_Transmit.argtypes = [
        *channel_types,
        ctypes.POINTER(CanFDMsg),
        ctypes.c_uint,
        ctypes.c_int,
    ]

    lib.CANFD_Receive.restype = ctypes.c_int
    lib.CANFD_Receive.argtypes = [
        *channel_types,
        ctypes.POINTER(CanFDMsg),
        ctypes.c_int,
        ctypes.c_int,
    ]


def _decode_c_string(value: bytes) -> str:
    return bytes(value).split(_C_STRING_TERMINATOR, 1)[0].decode(errors="replace")


def _mark_finalize_unsafe() -> None:
    """Module teardown hook: stop touching the vendor library at exit.

    By the time interpreter shutdown reaches the per-object ``__del__`` of an
    abandoned ``CANFDInterface``, ``ctypes`` and the vendor ``.so`` may already
    be unloaded. Flipping this flag turns the lifecycle release path into a
    no-op so we never re-enter a torn-down vendor library.
    """
    global _finalize_safe
    _finalize_safe = False


atexit.register(_mark_finalize_unsafe)
