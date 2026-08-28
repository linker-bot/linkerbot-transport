"""Public namespace contract for the Transport 1.0 API."""

import json
import os
import subprocess
import sys
from pathlib import Path

import linkerbot_transport
from linkerbot_transport import (
    CANRouteKey,
    ReceiveRouter,
    RoutedMessage,
    RouteSubscription,
    SendOptions,
    SendPolicy,
    SendReceipt,
    TransmitArbiter,
    canfd,
    errors,
)
from linkerbot_transport import can as classic_can

ROOT_EXPORTS = {
    "CANRouteKey",
    "ClosedError",
    "DeviceFaultError",
    "DeviceNotReadyError",
    "DeviceStateError",
    "ErrorCode",
    "LinkerbotError",
    "MessageTransport",
    "QueueFullError",
    "ReceiveRouter",
    "ReceiveRouterHealth",
    "RouteHealth",
    "RouteSubscription",
    "RoutedMessage",
    "SendOptions",
    "SendOutcome",
    "SendPolicy",
    "SendReceipt",
    "SendResult",
    "SharedTransportPool",
    "TimeoutError",
    "TransmitArbiter",
    "TransmitHealth",
    "TransportError",
    "TransportHealth",
    "TransportLease",
    "TransportLifecycle",
    "TransportState",
    "UnsupportedOperationError",
    "ValidationError",
    "normalize_error",
}

CAN_EXPORTS = {
    "CANRouteKey",
    "CANTransport",
    "CANTransportKey",
    "CanInterface",
    "RouteSubscription",
    "RoutedMessage",
    "SendOutcome",
    "SendReceipt",
    "SendResult",
}

CANFD_EXPORTS = {
    "CANFDBackend",
    "CANFDConfigOptions",
    "CANFDMessage",
    "CANFDTransport",
    "CANFDTransportKey",
    "CANRouteKey",
    "RouteSubscription",
    "RoutedMessage",
    "SendOutcome",
    "SendReceipt",
    "SendResult",
    "SocketCANFDTransport",
    "SocketCANFDTransportKey",
    "dlc_to_length",
    "length_to_dlc",
}

ERROR_EXPORTS = {
    "ClosedError",
    "DeviceFaultError",
    "DeviceNotReadyError",
    "DeviceStateError",
    "ErrorCode",
    "LinkerbotError",
    "QueueFullError",
    "TimeoutError",
    "TransportError",
    "UnsupportedOperationError",
    "ValidationError",
    "normalize_error",
}


def test_public_exports_are_frozen() -> None:
    assert set(linkerbot_transport.__all__) == ROOT_EXPORTS
    assert set(classic_can.__all__) == CAN_EXPORTS
    assert set(canfd.__all__) == CANFD_EXPORTS
    assert set(errors.__all__) == ERROR_EXPORTS


def test_old_compatibility_packages_are_not_in_source_distribution() -> None:
    source_root = Path(__file__).parents[1] / "src"
    old_namespace = source_root / "linkerbot"

    assert not any(old_namespace.rglob("*.py"))
    assert not any(old_namespace.rglob("py.typed"))
    assert not (source_root / "linkerbot_transport" / "queue.py").exists()
    assert not (source_root / "linkerbot_transport" / "relay.py").exists()


def test_old_aliases_and_implementation_classes_are_not_public() -> None:
    for name in ("CANError", "HandSDKError", "SDKError", "StateError"):
        assert not hasattr(errors, name)
    assert not hasattr(linkerbot_transport, "ManagedMessageTransport")
    assert not hasattr(classic_can, "CANMessageDispatcher")
    assert not hasattr(canfd, "CANFDMessageDispatcher")
    assert not hasattr(canfd, "CANFDSendReceipt")
    assert not hasattr(canfd, "SocketCANFDBackend")


def test_transmit_and_receive_contracts_remain_available() -> None:
    options = SendOptions(endpoint_id="l6", policy=SendPolicy.FIFO)
    arbiter = TransmitArbiter[int](lambda _: SendReceipt.completed())
    router = ReceiveRouter[int, int](lambda message: message)
    router.open(generation=1)
    route = router.subscribe(7)

    assert options.endpoint_id == "l6"
    assert not arbiter.get_health().running
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
    assert set(json.loads(result.stdout)) == ROOT_EXPORTS
