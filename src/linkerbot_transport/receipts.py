"""Observable completion of one accepted transport send."""

from __future__ import annotations

import threading
from concurrent.futures import Future


class SendReceipt(Future[None]):
    """Result completed only after the physical backend accepts one message.

    A receipt represents an already accepted send, so it cannot be cancelled by
    callers. Queue replacement and lifecycle shutdown are instead reported by
    completing the receipt with the corresponding stable exception.
    """

    def __init__(self) -> None:
        super().__init__()
        self.set_running_or_notify_cancel()

    @classmethod
    def completed(cls) -> SendReceipt:
        """Create a receipt for a message already sent successfully."""

        receipt = cls()
        receipt.set_result(None)
        return receipt

    @classmethod
    def failed(cls, error: BaseException) -> SendReceipt:
        """Create a receipt completed with ``error``."""

        receipt = cls()
        receipt.set_exception(error)
        return receipt


class _ReceiptTracker:
    """Thread-safe ownership of receipts not yet completed by a send loop."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: set[SendReceipt] = set()

    def register(self, receipt: SendReceipt) -> None:
        with self._lock:
            self._pending.add(receipt)

    def discard(self, receipt: SendReceipt) -> None:
        with self._lock:
            self._pending.discard(receipt)

    def succeed(self, receipt: SendReceipt) -> None:
        if self._take(receipt):
            receipt.set_result(None)

    def fail(self, receipt: SendReceipt, error: BaseException) -> None:
        if self._take(receipt):
            receipt.set_exception(error)

    def fail_all(self, error: BaseException) -> None:
        with self._lock:
            receipts = tuple(self._pending)
            self._pending.clear()
        for receipt in receipts:
            receipt.set_exception(error)

    def _take(self, receipt: SendReceipt) -> bool:
        with self._lock:
            if receipt not in self._pending:
                return False
            self._pending.remove(receipt)
            return True


__all__ = ["SendReceipt"]
