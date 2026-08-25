"""Compatibility aliases for ``linkerbot_transport.canfd``."""

import sys

from linkerbot_transport.canfd import (
    CANFDBackend,
    CanFDConfig,
    CANFDConfigOptions,
    CANFDInterface,
    CANFDMessage,
    CANFDMessageDispatcher,
    CanFDMsg,
    CANFDSendReceipt,
    DevInfo,
    SocketCANFDBackend,
    dlc_to_length,
    length_to_dlc,
)
from linkerbot_transport.canfd import ctypes_backend as ctypes_backend
from linkerbot_transport.canfd import dispatcher as dispatcher
from linkerbot_transport.canfd import socketcan_backend as socketcan_backend
from linkerbot_transport.canfd import types as types

sys.modules[f"{__name__}.ctypes_backend"] = ctypes_backend
sys.modules[f"{__name__}.dispatcher"] = dispatcher
sys.modules[f"{__name__}.socketcan_backend"] = socketcan_backend
sys.modules[f"{__name__}.types"] = types

__all__ = [
    "CANFDBackend",
    "CANFDConfigOptions",
    "CANFDInterface",
    "CANFDMessage",
    "CANFDMessageDispatcher",
    "CANFDSendReceipt",
    "CanFDConfig",
    "CanFDMsg",
    "DevInfo",
    "SocketCANFDBackend",
    "dlc_to_length",
    "length_to_dlc",
]
