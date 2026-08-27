# Changelog

This project follows Semantic Versioning.

## Unreleased

- Unify classic CAN, CAN FD, managed and fake transport sends on the observable
  `SendReceipt` contract.
- Report classic CAN background failures, shutdown drops and queue saturation
  without describing queue admission as physical transmission.
- Add generation-aware send outcomes and monotonic acceptance, claim and
  completion timing to successful and superseded send results.
- Add a lifecycle-managed shared TX arbiter with keyed latest-wins commands,
  an independent bounded FIFO, deadlines, priority aging and endpoint fairness.

## 0.1.0 - 2026-08-26

- Publish the independent classic CAN, vendor CAN FD, and SocketCAN FD
  transport package with canonical and compatibility import paths.
- Add explicit transport lifecycle management, health reporting, shared
  connection leases, and generation-safe recovery.
- Make blocking relay requests atomic so fast responses cannot be missed.
- Return observable receipts for queued CAN FD sends and propagate physical
  send failures to callers.
- Default CAN FD frames to FD without bit-rate switching while retaining an
  explicit BRS option and backend inheritance semantics.
- Cache vendor library handles so process-wide initialization and shutdown use
  the same loaded library instance.
