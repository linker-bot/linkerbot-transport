"""CAN FD frame conversion and lifecycle shared by python-can backends."""

from __future__ import annotations

import math
import threading
from collections.abc import Callable, Mapping
from typing import Any

import can

from linkerbot_transport._python_can import (
    BusOption,
    ChannelClaim,
    freeze_bus_options,
    open_bus,
    positive_int,
    rollback_bus_open,
    validate_channel,
)
from linkerbot_transport.errors import LinkerbotError, TransportError, ValidationError

from .types import CANFDMessage, dlc_to_length, length_to_dlc


def restore_fd_timing(values: tuple[tuple[str, int], ...]) -> can.BitTimingFd | None:
    if not values:
        return None
    kwargs: dict[str, Any] = dict(values)
    return can.BitTimingFd(**kwargs)


def resolve_fd_options(
    channel: str,
    interface: str,
    bitrate: int,
    data_bitrate: int,
    fd: bool,
    receive_own_messages: bool,
    bus_options: Mapping[str, BusOption] | None,
    timing: can.BitTimingFd | None,
) -> tuple[str, tuple[tuple[str, BusOption], ...], tuple[tuple[str, int], ...]]:
    """Validate and snapshot options without loading a driver or opening a port."""
    validate_channel(channel, interface)
    positive_int(bitrate, "bitrate")
    positive_int(data_bitrate, "data_bitrate")
    for name, value in (("fd", fd), ("receive_own_messages", receive_own_messages)):
        if not isinstance(value, bool):
            raise ValidationError(f"{name} must be bool")
    options = dict(freeze_bus_options(bus_options))
    if interface == "slcan":
        if bitrate not in (
            10_000,
            20_000,
            50_000,
            100_000,
            125_000,
            250_000,
            500_000,
            750_000,
            1_000_000,
        ):
            raise ValidationError(
                "nominal bitrate is not supported by python-can SLCAN"
            )
        if "@" in channel:
            channel, baud = channel.rsplit("@", 1)
            try:
                serial_baud = int(baud)
            except ValueError as error:
                raise ValidationError("invalid SLCAN channel@baudrate") from error
            if "tty_baudrate" in options and options["tty_baudrate"] != serial_baud:
                raise ValidationError("conflicting SLCAN serial baudrates")
            options["tty_baudrate"] = serial_baud
        validate_channel(channel, interface)
        serial_baud = options.setdefault("tty_baudrate", 115_200)
        if (
            not isinstance(serial_baud, int)
            or isinstance(serial_baud, bool)
            or serial_baud <= 0
        ):
            raise ValidationError("tty_baudrate must be a positive int")
        if fd:
            if data_bitrate not in (2_000_000, 5_000_000):
                raise ValidationError(
                    "python-can SLCAN FD supports 2 or 5 Mbit/s data bitrate"
                )
            if timing is None:
                try:
                    timing = can.BitTimingFd.from_sample_point(
                        f_clock=80_000_000,
                        nom_bitrate=bitrate,
                        nom_sample_point=80,
                        data_bitrate=data_bitrate,
                        data_sample_point=75,
                    )
                except ValueError as error:
                    raise ValidationError(
                        f"invalid SLCAN FD timing: {error}"
                    ) from error
    if timing is not None:
        if not isinstance(timing, can.BitTimingFd) or not fd:
            raise ValidationError("timing must be BitTimingFd with fd=True")
        if timing.nom_bitrate != bitrate or timing.data_bitrate != data_bitrate:
            raise ValidationError("timing bitrates must match bitrate and data_bitrate")
    return (
        channel,
        freeze_bus_options(options),
        () if timing is None else tuple(sorted(dict(timing).items())),
    )


class PythonCANFDBackend:
    """Wrap one python-can bus; the dispatcher owns concurrent TX/RX shutdown."""

    def __init__(
        self,
        channel: str,
        interface: str,
        *,
        bitrate: int = 1_000_000,
        data_bitrate: int = 5_000_000,
        fd: bool = True,
        receive_own_messages: bool = False,
        bus_options: Mapping[str, BusOption] | None = None,
        timing: can.BitTimingFd | None = None,
        prepare: Callable[[], None] | None = None,
    ) -> None:
        channel, options, timing_values = resolve_fd_options(
            channel,
            interface,
            bitrate,
            data_bitrate,
            fd,
            receive_own_messages,
            bus_options,
            timing,
        )
        self._channel = channel
        self._bitrate = bitrate
        self._data_bitrate = data_bitrate
        self._fd = fd
        self._interface = interface
        self._lock = threading.Lock()
        self._closed = False
        self._pending_receive_error: Exception | None = None
        self._claim = ChannelClaim(
            interface,
            channel,
            (
                bitrate,
                data_bitrate if fd else None,
                fd,
                receive_own_messages,
                options,
                timing_values,
            ),
        )
        try:
            if prepare is not None:
                prepare()
            kwargs: dict[str, Any] = dict(options)
            if timing_values:
                kwargs["timing"] = restore_fd_timing(timing_values)
            self._bus = open_bus(
                channel=channel,
                interface=interface,
                bitrate=bitrate,
                data_bitrate=data_bitrate,
                fd=fd,
                receive_own_messages=receive_own_messages,
                **kwargs,
            )
        except BaseException as error:
            rollback_bus_open(error, self._claim)
            raise

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def bitrate(self) -> int:
        return self._bitrate

    @property
    def data_bitrate(self) -> int:
        return self._data_bitrate

    @property
    def fd(self) -> bool:
        return self._fd

    def send(self, message: CANFDMessage, timeout_ms: int = 10) -> None:
        if timeout_ms < 0:
            raise ValidationError("timeout_ms must be non-negative")
        if self._closed:
            raise TransportError("python-can FD backend is closed")
        flags = (
            (0x04 if self._fd else 0)
            if message.frame_type is None
            else message.frame_type
        )
        if flags & ~0x0C:
            raise ValidationError("python-can supports only FD and BRS FrameType bits")
        is_fd, brs = bool(flags & 0x04), bool(flags & 0x08)
        if brs and not is_fd:
            raise ValidationError("FrameType BRS bit requires the CAN FD bit")
        if is_fd and not self._fd:
            raise ValidationError("CAN FD message cannot be sent with fd=False")
        dlc = (
            message.dlc if message.dlc is not None else length_to_dlc(len(message.data))
        )
        if not is_fd and dlc > 8:
            raise ValidationError("classic CAN frames cannot use a DLC greater than 8")
        # Our DLC is encoded (0..15); python-can's dlc is the byte count.
        wire_length = dlc_to_length(dlc)
        try:
            self._bus.send(
                can.Message(
                    arbitration_id=message.arbitration_id,
                    is_extended_id=message.is_extended_id,
                    is_fd=is_fd,
                    bitrate_switch=brs,
                    data=message.data.ljust(wire_length, b"\x00"),
                    check=True,
                ),
                timeout=timeout_ms / 1000,
            )
        except (can.CanError, OSError) as error:
            raise TransportError(
                f"{self._interface} CAN FD send failed: {error}", cause=error
            ) from error

    def receive(self, max_frames: int = 64, timeout_ms: int = 10) -> list[CANFDMessage]:
        """Deliver valid frames before reporting a later batch error on the next call."""
        if self._closed:
            raise TransportError("python-can FD backend is closed")
        positive_int(max_frames, "max_frames")
        if timeout_ms < 0:
            raise ValidationError("timeout_ms must be non-negative")
        if self._pending_receive_error is not None:
            error = self._pending_receive_error
            self._pending_receive_error = None
            raise error
        result = []
        # One blocking wait followed by a bounded, nonblocking drain. Remote
        # frames have no device payload; bus errors must not look like replies.
        try:
            for index in range(max_frames):
                try:
                    message = self._bus.recv(
                        timeout=timeout_ms / 1000 if index == 0 else 0
                    )
                except (can.CanError, OSError) as error:
                    raise TransportError(
                        f"{self._interface} CAN FD receive failed: {error}", cause=error
                    ) from error
                if message is None:
                    break
                if message.is_error_frame:
                    raise TransportError(
                        f"{self._interface} reported a CAN bus error frame"
                    )
                if message.is_remote_frame:
                    continue
                result.append(_to_canfd_message(message))
        except Exception as error:
            if not result:
                raise
            # These frames have already left the driver queue. Deliver them,
            # then report this same error before reading any newer frames.
            # Include parser/conversion failures; process interrupts propagate.
            self._pending_receive_error = error
        return result

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                self._bus.shutdown()
            except LinkerbotError:
                raise
            except Exception as error:
                raise TransportError(
                    f"{self._interface} CAN FD shutdown failed: {error}", cause=error
                ) from error
            # Interrupted or failed shutdown keeps both the handle and claim.
            self._closed = True
            self._pending_receive_error = None
            self._claim.release()


def _to_canfd_message(message: can.Message) -> CANFDMessage:
    data = bytes(message.data)
    wire_size = dlc_to_length(length_to_dlc(message.dlc))
    if len(data) != message.dlc or wire_size != message.dlc:
        raise TransportError("invalid CAN FD payload length from python-can")
    if not message.is_fd and (message.dlc > 8 or message.bitrate_switch):
        raise TransportError("invalid classic CAN flags from python-can")
    timestamp = message.timestamp
    return CANFDMessage(
        arbitration_id=message.arbitration_id,
        data=data,
        is_extended_id=message.is_extended_id,
        frame_type=(0x04 if message.is_fd else 0)
        | (0x08 if message.bitrate_switch else 0),
        timestamp=int(timestamp * 1_000_000)
        if math.isfinite(timestamp) and timestamp >= 0
        else None,
    )
