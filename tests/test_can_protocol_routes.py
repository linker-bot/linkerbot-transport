"""Protocol addresses may occupy only part of an extended CAN identifier."""

import can
import pytest

from linkerbot_transport import CANRouteKey, ClosedError, ValidationError
from linkerbot_transport.can import CANTransport
from linkerbot_transport.testing import FakeDispatcher


def address(message):
    if message.is_extended_id:
        return CANRouteKey(message.arbitration_id & 0xFF00, True)
    return CANRouteKey(message.arbitration_id, False)


def test_protocol_address_preserves_original_frames_and_recovery_generations():
    dispatchers = []

    def create(on_error):
        dispatcher = FakeDispatcher(on_error)
        dispatchers.append(dispatcher)
        return dispatcher

    transport = CANTransport("virtual", dispatcher_factory=create, route_key_of=address)
    assert not dispatchers
    transport.open()
    route = transport.subscribe_route(CANRouteKey(0x3D00, True))
    try:
        for status in (0, 1, 0x7F, 0xFF):
            message = can.Message(
                arbitration_id=0x02003D00 | status << 16,
                is_extended_id=True,
                data=b"12345678",
            )
            dispatchers[-1].emit(message)
            received = route.receive(timeout_s=0)
            assert received.message is message
            assert received.message.arbitration_id == 0x02003D00 | status << 16
            assert received.generation == 1
        # Exact standard format and neighbouring protocol addresses stay distinct.
        dispatchers[-1].emit(can.Message(arbitration_id=0x3D, is_extended_id=False))
        dispatchers[-1].emit(
            can.Message(arbitration_id=0x02003E00, is_extended_id=True)
        )
        assert route.try_receive() is None
        old_callback = dispatchers[-1]._subscribers[0]
        transport.recover()
        with pytest.raises(ClosedError):
            route.receive(timeout_s=0)
        new_route = transport.subscribe_route(CANRouteKey(0x3D00, True))
        old_callback(message)
        assert new_route.try_receive() is None
        dispatchers[-1].emit(message)
        assert new_route.receive(timeout_s=0).generation == 2
    finally:
        transport.close()


def test_default_routes_remain_exact_and_custom_policy_has_a_distinct_key():
    created = []

    def factory(on_error):
        dispatcher = FakeDispatcher(on_error)
        created.append(dispatcher)
        return dispatcher

    transport = CANTransport("virtual", dispatcher_factory=factory)
    assert transport.pool_key == CANTransport("virtual").pool_key
    custom = CANTransport("virtual", route_key_of=address)
    assert custom.pool_key != transport.pool_key
    assert custom.pool_key == CANTransport("virtual", route_key_of=address).pool_key
    transport.open()
    try:
        route = transport.subscribe_route(CANRouteKey(0x02003D00, True))
        created[0].emit(can.Message(arbitration_id=0x02013D00, is_extended_id=True))
        assert route.try_receive() is None
    finally:
        transport.close()


@pytest.mark.parametrize("extractor", [False, 1, "mask"])
def test_invalid_extractor_fails_without_opening_channel(extractor):
    with pytest.raises(ValidationError, match="route_key_of"):
        CANTransport("not-opened", route_key_of=extractor)
