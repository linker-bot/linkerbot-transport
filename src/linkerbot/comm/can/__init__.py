"""Compatibility alias for ``linkerbot_transport.can``."""

import sys

from linkerbot_transport.can import CanInterface, CANMessageDispatcher
from linkerbot_transport.can import can as can
from linkerbot_transport.can import types as types

sys.modules[f"{__name__}.can"] = can
sys.modules[f"{__name__}.types"] = types

__all__ = ["CANMessageDispatcher", "CanInterface"]
