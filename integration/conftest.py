"""Explicitly selected, offline SDK integration tests on isolated CAN channels."""

from __future__ import annotations

import uuid

import pytest

from .devices import FourDevices


@pytest.fixture(params=["cantransport", "dispatcher"])
def arm_backend(request, monkeypatch):
    # Exercise both supported Arm adapters against the same Transport package.
    monkeypatch.setenv("LINKERBOT_ARM_TRANSPORT", request.param)


@pytest.fixture
def four_devices(request, arm_backend):
    prefix = "joint-" + uuid.uuid4().hex
    channels = tuple(f"{prefix}-{index}" for index in range(4))
    rig = FourDevices(
        "virtual", channels, duplicate_addresses=getattr(request, "param", False)
    )
    try:
        rig.open()
        yield rig
    finally:
        rig.close()
