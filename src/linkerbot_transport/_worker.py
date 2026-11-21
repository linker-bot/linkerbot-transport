"""Worker completion independent of an interpreter thread join's bookkeeping."""

import threading


class WorkerThread(threading.Thread):
    """Signal only after the target and all of its cleanup have returned.

    CPython 3.10 can mark a running Thread stopped when join is interrupted.
    Resource owners wait on this handoff instead of inferring exit from that
    bookkeeping. An interrupted start retains this object until actual exit.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.exited = threading.Event()

    def run(self) -> None:
        try:
            super().run()
        finally:
            self.exited.set()
