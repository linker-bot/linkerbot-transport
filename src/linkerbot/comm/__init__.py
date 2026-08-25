"""Compatibility imports for the historical ``linkerbot.comm`` namespace."""

from linkerbot_transport.can import CanInterface, CANMessageDispatcher
from linkerbot_transport.canfd import (
    CANFDConfigOptions,
    CANFDInterface,
    CANFDMessage,
    CANFDMessageDispatcher,
    CANFDSendReceipt,
)

__all__ = [
    "CANFDConfigOptions",
    "CANFDInterface",
    "CANFDMessage",
    "CANFDMessageDispatcher",
    "CANFDSendReceipt",
    "CANMessageDispatcher",
    "CanInterface",
]
