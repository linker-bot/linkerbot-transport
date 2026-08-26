# Changelog

This project follows Semantic Versioning.

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
