"""Keep explicit send timeouts distinct from a Future wait expiring."""

import ast
import builtins
from pathlib import Path
from types import SimpleNamespace

import can
import pytest
import serial

from linkerbot_transport import ErrorCode, SendReceipt
from linkerbot_transport.can import CANTransport
from linkerbot_transport.can.can import CANMessageDispatcher
from linkerbot_transport.canfd import CANFDMessage, PythonCANFDTransport
from linkerbot_transport.errors import TimeoutError as SDKTimeout
from linkerbot_transport.errors import TransportError


class TimeoutPort:
    def __init__(self):
        self.closed = False

    def read(self, size=1):
        return b""

    def write(self, data):
        raise serial.SerialTimeoutException("explicit write timeout")

    def close(self):
        self.closed = True


@pytest.mark.parametrize("fd", [False, True])
def test_real_serial_backend_send_timeout_retains_sdk_code_and_cause(monkeypatch, fd):
    port = TimeoutPort()
    monkeypatch.setattr(serial, "serial_for_url", lambda *a, **kw: port)
    transport = (
        PythonCANFDTransport("timeout-fd", "serial", fd=False)
        if fd
        else CANTransport("timeout-classic", "serial")
    )
    transport.open()
    try:
        frame = (
            CANFDMessage(0x123, b"x")
            if fd
            else can.Message(arbitration_id=0x123, data=b"x")
        )
        receipt = transport.send(frame)
        with pytest.raises(SDKTimeout) as raised:
            receipt.result(timeout=0)
        assert raised.value.code is ErrorCode.TIMEOUT
        assert isinstance(raised.value.cause, can.CanTimeoutError)
        assert isinstance(raised.value.cause.__cause__, serial.SerialTimeoutException)
        assert transport.get_health().last_error_code is ErrorCode.TIMEOUT
    finally:
        transport.close()
    assert port.closed


@pytest.mark.parametrize(
    "error_type", [can.CanTimeoutError, builtins.TimeoutError, can.CanOperationError]
)
def test_queued_classic_send_classifies_types_without_guessing_text(
    monkeypatch, error_type
):
    dispatcher = CANMessageDispatcher("queued-timeout", "virtual")
    error = error_type("timeout")
    monkeypatch.setattr(
        dispatcher._bus, "send", lambda *a, **kw: (_ for _ in ()).throw(error)
    )
    try:
        receipt = dispatcher.send(can.Message(arbitration_id=1))
        expected = TransportError if error_type is can.CanOperationError else SDKTimeout
        with pytest.raises(expected) as raised:
            receipt.result(timeout=1)
        assert raised.value.cause is error
    finally:
        dispatcher.stop()


@pytest.mark.parametrize("filename", ["runtime.md", "runtime_zh.md"])
@pytest.mark.parametrize("terminal", [False, True])
def test_documented_receipt_example_catches_the_correct_timeout(filename, terminal):
    text = (Path(__file__).parents[1] / "docs" / filename).read_text()
    section = text.split("`SendReceipt`", 1)[1]
    snippet = section.split("```python\n", 1)[1].split("```", 1)[0]
    tree = ast.parse(snippet)
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler):
            node.body.append(
                ast.Assign(
                    targets=[ast.Name(id="branch", ctx=ast.Store())],
                    value=ast.Constant(value=node.type.id),
                )
            )
    receipt = (
        SendReceipt.failed(SDKTimeout("backend expired")) if terminal else SendReceipt()
    )
    namespace = {
        "transport": SimpleNamespace(send=lambda message: receipt),
        "message": object(),
    }
    try:
        exec(compile(ast.fix_missing_locations(tree), filename, "exec"), namespace)
        assert namespace["branch"] == (
            "SendTimeout" if terminal else "FutureWaitTimeout"
        )
        assert receipt.done() is terminal
    finally:
        if not receipt.done():
            receipt._fail(SDKTimeout("test cleanup"))
