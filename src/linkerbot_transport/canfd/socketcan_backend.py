"""Linux SocketCAN FD backend for ``CANFDMessageDispatcher``.

Wraps python-can's ``socketcan`` interface in CAN FD mode. Unlike the ctypes
backend (vendor ``libcanbus.so`` / ``HCanbus.dll``) which configures the
adapter bitrate from the SDK at open time, SocketCAN's bitrate lives at the
kernel level and is configured once via ``ip link``. Multiple processes share
the same ``can0`` link, so this backend treats the link as observed shared
state rather than something it owns:

- If ``<channel>`` is already up with matching ``bitrate`` /
  ``data_bitrate`` / ``fd``, the backend uses it as-is.
- If ``<channel>`` is up with mismatched parameters, the backend raises
  :class:`TransportError` showing the exact ``ip link`` command needed to fix it,
  unless ``auto_reconfigure=True`` is passed.
- If ``<channel>`` is not configured, the backend either runs ``ip link``
  itself (``auto_reconfigure=True``) or raises :class:`TransportError` with the
  exact command for the user to run manually.

``auto_reconfigure`` defaults to ``False`` because the operations it would run
(``ip link set ... down`` / ``up``) require ``CAP_NET_ADMIN`` and side-affect
any other process currently using the link.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from collections.abc import Mapping
from typing import TYPE_CHECKING

from linkerbot_transport._python_can import BusOption
from linkerbot_transport.errors import TransportError

from .python_can_backend import PythonCANFDBackend
from .types import (
    CANFD_DEFAULT_DATA_BAUD,
    CANFD_DEFAULT_NOMINAL_BAUD,
)

DEFAULT_NOMINAL_BITRATE = CANFD_DEFAULT_NOMINAL_BAUD
DEFAULT_DATA_BITRATE = CANFD_DEFAULT_DATA_BAUD

if TYPE_CHECKING:
    import can


class _LinkState:
    """Decoded SocketCAN link state from ``ip -d -j link show``."""

    __slots__ = ("up", "fd", "bitrate", "data_bitrate")

    def __init__(
        self,
        *,
        up: bool,
        fd: bool,
        bitrate: int | None,
        data_bitrate: int | None,
    ) -> None:
        self.up = up
        self.fd = fd
        self.bitrate = bitrate
        self.data_bitrate = data_bitrate

    def matches(self, *, bitrate: int, data_bitrate: int, fd: bool) -> bool:
        if self.fd != fd:
            return False
        if self.bitrate != bitrate:
            return False
        if fd and self.data_bitrate != data_bitrate:
            return False
        return True


class SocketCANFDBackend(PythonCANFDBackend):
    """Linux SocketCAN backend in CAN FD mode.

    Implements :class:`linkerbot_transport.canfd.CANFDBackend`. Construct one of
    these and pass it to :class:`CANFDMessageDispatcher` via ``interface=`` to
    use SocketCAN instead of the vendor ctypes backend.
    """

    def __init__(
        self,
        channel: str,
        *,
        bitrate: int = DEFAULT_NOMINAL_BITRATE,
        data_bitrate: int = DEFAULT_DATA_BITRATE,
        fd: bool = True,
        auto_reconfigure: bool = False,
        receive_own_messages: bool = False,
        bus_options: Mapping[str, BusOption] | None = None,
        timing: can.BitTimingFd | None = None,
    ) -> None:
        """Open a SocketCAN FD link.

        Args:
            channel: SocketCAN interface name (for example ``"can0"``).
            bitrate: Nominal (arbitration) bitrate in bits/sec.
                Defaults to 1 Mbit/s.
            data_bitrate: Data-phase bitrate in bits/sec when ``fd=True``.
                Defaults to 5 Mbit/s.
            fd: Whether to enable CAN FD framing. Set to False for plain
                CAN 2.0 over the same backend.
            auto_reconfigure: When True, the backend will run ``ip link``
                itself if the link is missing or has mismatched parameters.
                Requires ``CAP_NET_ADMIN`` (typically root). Default False.
            receive_own_messages: Forwarded to ``can.Bus``; whether the bus
                echoes locally-sent frames back through ``recv()``.

        Raises:
            ValidationError: If the channel name or bitrates are invalid.
            TransportError: If ``ip`` is missing, the link is misconfigured and
                ``auto_reconfigure`` is False, or ``ip link`` itself fails.
        """
        super().__init__(
            channel,
            "socketcan",
            bitrate=bitrate,
            data_bitrate=data_bitrate,
            fd=fd,
            receive_own_messages=receive_own_messages,
            bus_options=bus_options,
            timing=timing,
            prepare=lambda: self._ensure_link_state(auto_reconfigure=auto_reconfigure),
        )

    def _ensure_link_state(self, *, auto_reconfigure: bool) -> None:
        state = _read_link_state(self._channel)
        if state is None:
            if auto_reconfigure:
                _bring_up(self._channel, self._bitrate, self._data_bitrate, self._fd)
                return
            setup_cmd = _format_setup_command(
                self._channel, self._bitrate, self._data_bitrate, self._fd
            )
            raise TransportError(
                f"SocketCAN channel {self._channel!r} is not configured. Run:\n"
                f"  {setup_cmd}\n"
                f"or pass auto_reconfigure=True (requires CAP_NET_ADMIN)."
            )
        if state.matches(
            bitrate=self._bitrate, data_bitrate=self._data_bitrate, fd=self._fd
        ):
            if state.up:
                return
            if auto_reconfigure:
                _bring_up(self._channel, self._bitrate, self._data_bitrate, self._fd)
                return
            setup_cmd = _format_setup_command(
                self._channel, self._bitrate, self._data_bitrate, self._fd
            )
            raise TransportError(
                f"SocketCAN channel {self._channel!r} is configured but down. Run:\n"
                f"  {setup_cmd}\n"
                f"or pass auto_reconfigure=True."
            )
        if auto_reconfigure:
            if state.up:
                _bring_down(self._channel)
            _bring_up(self._channel, self._bitrate, self._data_bitrate, self._fd)
            return
        if state.up:
            reconfigure_cmd = _format_reconfigure_command(
                self._channel, self._bitrate, self._data_bitrate, self._fd
            )
        else:
            reconfigure_cmd = _format_setup_command(
                self._channel, self._bitrate, self._data_bitrate, self._fd
            )
        state_name = "up" if state.up else "down"
        raise TransportError(
            f"SocketCAN channel {self._channel!r} is {state_name} but parameters mismatch. "
            f"Current: bitrate={state.bitrate}, data_bitrate={state.data_bitrate}, "
            f"fd={state.fd}. Wanted: bitrate={self._bitrate}, "
            f"data_bitrate={self._data_bitrate}, fd={self._fd}.\n"
            f"Run:\n  {reconfigure_cmd}\n"
            f"or pass auto_reconfigure=True."
        )


def _format_setup_command(
    channel: str, bitrate: int, data_bitrate: int, fd: bool
) -> str:
    """``ip link`` command to bring up a link that does not exist yet."""
    parts = [
        "sudo ip link set",
        channel,
        "up type can",
        f"bitrate {bitrate}",
    ]
    if fd:
        parts.append(f"dbitrate {data_bitrate} fd on")
    return " ".join(parts)


def _format_reconfigure_command(
    channel: str, bitrate: int, data_bitrate: int, fd: bool
) -> str:
    """``ip link`` command to reconfigure a link that is already up."""
    return f"sudo ip link set {channel} down && " + _format_setup_command(
        channel, bitrate, data_bitrate, fd
    )


def _read_link_state(channel: str) -> _LinkState | None:
    """Return the parsed link state, or None if the link does not exist."""
    if shutil.which("ip") is None:
        raise TransportError(
            "'ip' command not found; SocketCAN backend requires Linux with iproute2"
        )
    proc = subprocess.run(
        ["ip", "-d", "-j", "link", "show", channel],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return None
    try:
        data = json.loads(proc.stdout or "[]")
    except json.JSONDecodeError as error:
        raise TransportError(
            f"failed to parse 'ip -d -j link show {channel}' output: {error}"
        ) from error
    if not data:
        return None
    entry = data[0]
    flags = entry.get("flags", []) or []
    up = "UP" in flags
    linkinfo = entry.get("linkinfo") or {}
    info_data = linkinfo.get("info_data") or {}
    bittiming = info_data.get("bittiming") or {}
    data_bittiming = info_data.get("data_bittiming") or {}
    ctrlmode = info_data.get("ctrlmode") or []
    fd = "FD" in ctrlmode
    return _LinkState(
        up=up,
        fd=fd,
        bitrate=bittiming.get("bitrate"),
        data_bitrate=data_bittiming.get("bitrate"),
    )


def _bring_down(channel: str) -> None:
    _run_ip(["link", "set", channel, "down"])


def _bring_up(channel: str, bitrate: int, data_bitrate: int, fd: bool) -> None:
    args = ["link", "set", channel, "up", "type", "can", "bitrate", str(bitrate)]
    if fd:
        args += ["dbitrate", str(data_bitrate), "fd", "on"]
    _run_ip(args)


def _run_ip(args: list[str]) -> None:
    proc = subprocess.run(
        ["ip", *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        msg = proc.stderr.strip() or proc.stdout.strip() or "no output"
        raise TransportError(
            f"'ip {' '.join(args)}' failed (rc={proc.returncode}): {msg}. "
            "auto_reconfigure requires CAP_NET_ADMIN; run as root or grant capability."
        )
