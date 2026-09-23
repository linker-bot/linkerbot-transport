"""Run legacy Linux ABI fakes on every host; Windows tests select their ABI."""

import pytest

from linkerbot_transport.canfd import ctypes_backend


@pytest.fixture(autouse=True)
def default_vendor_test_platform(monkeypatch):
    monkeypatch.setattr(ctypes_backend.platform, "system", lambda: "Linux")
    if not hasattr(ctypes_backend.ctypes, "RTLD_GLOBAL"):
        monkeypatch.setattr(ctypes_backend.ctypes, "RTLD_GLOBAL", 0, raising=False)
