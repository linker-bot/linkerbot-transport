"""Real python-can parser, with only the serial endpoint substituted."""

from collections import deque

import can
import pytest
import serial

from linkerbot_transport._slcan import ManagedSlcanBus


class InputPort:
    def __init__(self):
        self.incoming = deque()
        self.write_timeout = None
        self.closed = False
        self.reads = 0

    @property
    def in_waiting(self):
        return len(self.incoming)

    def read(self, size=1):
        self.reads += 1
        return bytes([self.incoming.popleft()]) if self.incoming else b""

    def write(self, data):
        return len(data)

    def flush(self):
        pass

    def reset_input_buffer(self):
        self.incoming.clear()

    def close(self):
        self.closed = True


@pytest.fixture
def endpoint(monkeypatch):
    port = InputPort()
    monkeypatch.setattr(serial, "serial_for_url", lambda *a, **kw: port)
    bus = ManagedSlcanBus("stream-test", bitrate=1_000_000, sleep_after_open=0)
    try:
        yield bus, port
    finally:
        bus.shutdown()
    assert port.closed


def test_unterminated_input_has_bounded_memory_and_work(endpoint):
    bus, port = endpoint
    port.incoming.extend(b"x" * 65536)
    try:
        bus.recv(timeout=0)
    except can.CanOperationError:
        pass
    assert len(bus._buffer) <= 143
    assert port.reads <= 4096
    before = port.reads
    assert bus.recv(timeout=0) is None
    assert 0 < port.reads - before <= 4096
    assert len(bus._buffer) <= 143


def test_overlong_line_discards_to_delimiter_then_recovers(endpoint):
    bus, port = endpoint
    port.incoming.extend(b"x" * 200 + b"t1231FF\rt1231AA\r")
    with pytest.raises(can.CanOperationError, match="line"):
        bus.recv(timeout=0)
    assert bus.recv(timeout=0) is None
    result = bus.recv(timeout=0)
    assert result is not None and bytes(result.data) == b"\xaa"


@pytest.mark.parametrize("bad", [b"\xff\r", b"\xff\a"])
def test_invalid_encoding_does_not_poison_next_frame(endpoint, bad):
    bus, port = endpoint
    port.incoming.extend(bad + b"t1231AA\r")
    with pytest.raises(can.CanOperationError):
        bus.recv(timeout=0)
    result = bus.recv(timeout=0)
    assert result is not None and bytes(result.data) == b"\xaa"


@pytest.mark.parametrize(
    "prefix",
    [b"t1231", b"T000001231", b"d1231", b"D000001231", b"b1231", b"B000001231"],
)
def test_fragmented_classic_and_fd_lines_keep_existing_parser(endpoint, prefix):
    bus, port = endpoint
    port.incoming.extend(prefix)
    assert bus.recv(timeout=0) is None
    port.incoming.extend(b"AA\r")
    result = bus.recv(timeout=0)
    assert result is not None and bytes(result.data) == b"\xaa"
    assert result.is_fd == (prefix[:1] in b"dDbB")


def test_largest_fd_line_and_version_response_still_work(endpoint):
    bus, port = endpoint
    port.incoming.extend(b"B1FFFFFFFf" + b"AA" * 64 + b"\r")
    result = bus.recv(timeout=0)
    assert result is not None and len(result.data) == 64 and result.bitrate_switch
    port.incoming.extend(b"V0102\r")
    assert bus.get_version(timeout=0.1) == (1, 2)


def test_flush_resets_discard_state(endpoint):
    bus, port = endpoint
    port.incoming.extend(b"x" * 200)
    with pytest.raises(can.CanOperationError):
        bus.recv(timeout=0)
    bus.flush()
    port.incoming.extend(b"t1231AA\r")
    assert bytes(bus.recv(timeout=0).data) == b"\xaa"
