"""Public namespace and compatibility contract tests."""

import json
import os
import subprocess
import sys

from linkerbot.comm import CANFDMessageDispatcher as LegacyTopLevelCANFDDispatcher
from linkerbot.comm.can import CANMessageDispatcher as LegacyCANDispatcher
from linkerbot.comm.canfd import (
    CANFDMessage as LegacyCANFDMessage,
)
from linkerbot.comm.canfd import (
    CANFDMessageDispatcher as LegacyCANFDDispatcher,
)
from linkerbot.exceptions import TransportError as LegacyTransportError
from linkerbot.queue import IterableQueue as LegacyIterableQueue
from linkerbot.relay import DataRelay as LegacyDataRelay
from linkerbot_transport.can import CANMessageDispatcher
from linkerbot_transport.canfd import CANFDMessage, CANFDMessageDispatcher
from linkerbot_transport.errors import TransportError
from linkerbot_transport.queue import IterableQueue
from linkerbot_transport.relay import DataRelay


def test_compatibility_exports_are_canonical_objects() -> None:
    assert LegacyCANDispatcher is CANMessageDispatcher
    assert LegacyCANFDMessage is CANFDMessage
    assert LegacyCANFDDispatcher is CANFDMessageDispatcher
    assert LegacyTopLevelCANFDDispatcher is CANFDMessageDispatcher
    assert LegacyTransportError is TransportError
    assert LegacyIterableQueue is IterableQueue
    assert LegacyDataRelay is DataRelay


def test_base_import_does_not_require_python_can() -> None:
    script = """
import builtins
import json

real_import = builtins.__import__

def guarded_import(name, *args, **kwargs):
    if name == "can" or name.startswith("can."):
        raise AssertionError("base import attempted to load python-can")
    return real_import(name, *args, **kwargs)

builtins.__import__ = guarded_import
import linkerbot_transport
print(json.dumps(sorted(linkerbot_transport.__all__)))
"""
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(sys.path)
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert "TransportError" in json.loads(result.stdout)
