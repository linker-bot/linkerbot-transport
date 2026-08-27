"""Public namespace and compatibility contract tests."""

import json
import os
import subprocess
import sys

from linkerbot.comm import CANFDMessageDispatcher as LegacyTopLevelCANFDDispatcher
from linkerbot.comm import CANFDSendReceipt as LegacyTopLevelCANFDSendReceipt
from linkerbot.comm.can import CANMessageDispatcher as LegacyCANDispatcher
from linkerbot.comm.canfd import (
    CANFDMessage as LegacyCANFDMessage,
)
from linkerbot.comm.canfd import (
    CANFDMessageDispatcher as LegacyCANFDDispatcher,
)
from linkerbot.comm.canfd import CANFDSendReceipt as LegacyCANFDSendReceipt
from linkerbot.exceptions import TransportError as LegacyTransportError
from linkerbot.queue import IterableQueue as LegacyIterableQueue
from linkerbot.relay import DataRelay as LegacyDataRelay
from linkerbot_transport import (
    CANRouteKey,
    ManagedMessageTransport,
    ReceiveRouter,
    RoutedMessage,
    RouteSubscription,
    SendOptions,
    SendOutcome,
    SendPolicy,
    SendReceipt,
    TransmitArbiter,
)
from linkerbot_transport.can import CANMessageDispatcher
from linkerbot_transport.canfd import (
    CANFDMessage,
    CANFDMessageDispatcher,
    CANFDSendReceipt,
)
from linkerbot_transport.errors import TransportError
from linkerbot_transport.queue import IterableQueue
from linkerbot_transport.relay import DataRelay


def test_compatibility_exports_are_canonical_objects() -> None:
    assert LegacyCANDispatcher is CANMessageDispatcher
    assert LegacyCANFDMessage is CANFDMessage
    assert LegacyCANFDDispatcher is CANFDMessageDispatcher
    assert LegacyCANFDSendReceipt is CANFDSendReceipt
    assert LegacyTopLevelCANFDDispatcher is CANFDMessageDispatcher
    assert LegacyTopLevelCANFDSendReceipt is CANFDSendReceipt
    assert LegacyTransportError is TransportError
    assert LegacyIterableQueue is IterableQueue
    assert LegacyDataRelay is DataRelay


def test_managed_transport_is_available_from_package_root() -> None:
    assert ManagedMessageTransport.__module__ == "linkerbot_transport._managed"


def test_send_receipt_is_shared_by_all_transport_namespaces() -> None:
    assert CANFDSendReceipt is SendReceipt
    assert SendOutcome.DISPATCHED.value == "dispatched"


def test_transmit_arbiter_contract_is_available_from_package_root() -> None:
    options = SendOptions(endpoint_id="l6", policy=SendPolicy.FIFO)
    arbiter = TransmitArbiter[int](lambda _: SendReceipt.completed())

    assert options.endpoint_id == "l6"
    assert not arbiter.get_health().running


def test_receive_routing_contract_is_available_from_package_root() -> None:
    router = ReceiveRouter[int, int](lambda message: message)
    router.open(generation=1)
    route = router.subscribe(7)

    assert isinstance(route, RouteSubscription)
    assert CANRouteKey(0x27, False).arbitration_id == 0x27
    assert router.route(7, generation=1)
    assert isinstance(route.receive(timeout_s=0), RoutedMessage)
    router.close()


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
