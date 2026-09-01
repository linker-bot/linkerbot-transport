# Changelog

This project follows Semantic Versioning.

## Unreleased

## 1.0.0 - 2026-09-01

- Publish the stable explicit-lifecycle transport API for classic CAN, vendor
  CAN FD and Linux SocketCAN FD.
- Provide exact receive routing, observable send receipts, shared connection
  leases and bounded latest/FIFO transmit arbitration without legacy aggregate
  SDK namespaces.
- Add edge-triggered receive readiness and caller-assisted transmit draining to
  reduce scheduling overhead in high-frequency device runtimes.
- Declare Python 3.10 through 3.13 support and keep `python-can` bounded to the
  compatible 4.x series.

## 1.0.0rc1 - 2026-08-28

- Freeze the public API around explicit-lifecycle classic CAN, vendor CAN FD,
  SocketCAN FD, exact receive routing, observable send receipts, shared leases
  and bounded transmit arbitration.
- Remove the historical `linkerbot.comm`, `linkerbot.exceptions`,
  `linkerbot.queue` and `linkerbot.relay` compatibility namespaces.
- Remove Hand-specific and legacy error aliases from the Transport API.
- Make auto-starting dispatcher classes internal implementation details.
- Remove the unused iterable queue and data relay primitives.
- Declare Python 3.10 through 3.13 support and bound the supported
  `python-can` dependency to the compatible 4.x series.
- Add complete English and Simplified Chinese usage documentation.

## 0.1.4 - 2026-08-28

- Report cumulative successful transmit frames, received frames and DLC data
  field bytes for each physical channel across connection generations.

## 0.1.3 - 2026-08-28

- Allow several exact receive routes to feed one atomic bounded subscription
  while preserving constant-time frame routing and combined-stream backpressure.

## 0.1.2 - 2026-08-28

- Add an atomic replace-if-pending operation for keyed latest sends so protocol
  runtimes never queue a replacement behind an already claimed transaction.

## 0.1.1 - 2026-08-28

- Unify classic CAN, CAN FD, managed and fake transport sends on the observable
  `SendReceipt` contract.
- Report classic CAN background failures, shutdown drops and queue saturation
  without describing queue admission as physical transmission.
- Add generation-aware send outcomes and monotonic acceptance, claim and
  completion timing to successful and superseded send results.
- Add a lifecycle-managed shared TX arbiter with keyed latest-wins commands,
  an independent bounded FIFO, deadlines, priority aging and endpoint fairness.
- Add generation-safe O(1) receive routes with bounded inboxes for managed CAN,
  CAN FD and fake transports, including miss, overflow and stale-frame health.
- Allow one endpoint to cancel and boundedly drain only its own pending shared
  transport work without interrupting other endpoint leases.

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
