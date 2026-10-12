# Changelog

This project follows Semantic Versioning.

## Unreleased

- Default Linux MiniCANFD enumeration to channel 0 on every scanned adapter,
  including the verified legacy device API. When channel 0 cannot be enumerated
  as FD, check channel 1 where the ABI supports it and report failures when
  neither channel is available. Derive adapter count from the vendor scan;
  retain strict explicit layouts, FD type checks and lifecycle diagnostics
  without an additional scan.
- Clarify how the hand SDK separately requests channel 1 enumeration after an
  unanswered channel 0 identity probe, retaining legacy channel restrictions.

- Retain failed final-lease cleanup for explicit `close()` retries without
  reopening channels or decrementing references twice. Serialize retries with
  pool ownership checks so an old retry never closes a recovered active owner
  or a replacement entry.

- Reorganize the current-version documentation: keep the root READMEs focused on
  installation, backend selection and a minimal lifecycle, and move runtime,
  platform, enumeration and release details into dedicated guides.

- Add public PCAN and SLCAN candidate enumeration with immutable native channel
  descriptors and explicit dependency, driver, busy and missing-port diagnostics.
  Query PCAN through python-can without initializing channels; select only known
  CANable USB candidates or explicitly requested serial ports without opening
  them. Keep SLCAN FD firmware capability unknown until protocol validation.

- Report MiniCANFD library Exit failures from close and retain explicit cleanup
  retries. Release each lifecycle reference once, so an old close retry cannot
  tear down a new connection or close an already released channel again.

- Record synchronous send claims before entering the backend. Preserve the same
  receipt through success or failure so replies received during a send remain
  newer than its claim timestamp.

- Reuse the MiniCANFD scan snapshot when opening additional channels, including
  while another channel is pending close. Reject new indices until all channels
  close, so opening a connection cannot bypass enumeration's rescan guard.

- Support the verified legacy Linux MiniCANFD device API used by HCANFD Mini-I
  libraries without Init/Exit exports. Enumerate explicitly confirmed channel 0
  with its single-argument device type query; reject partial lifecycle exports
  and unsupported legacy channel layouts.

- Add frame-free MiniCANFD channel enumeration for Windows and Linux. Return
  native FD channel coordinates and library paths with structured diagnostics;
  require confirmed Linux channel layouts where the vendor API only reports
  device counts. Protect live/pending-close channels from rescans.

- Share vendor library ownership across system-loader names and absolute paths.
  Retain failed Linux library cleanup for retry before another initialization
  and coordinate enumeration with channel opening and cleanup.

- Preserve valid frames received before a later error in python-can CAN FD
  batches, including SocketCAN FD. Return the valid prefix, then report the
  pending receive/conversion error once before reading further frames. Keep
  error causes, immediate interrupts, and existing consecutive-error handling;
  discard pending errors on successful close.

- Track PCAN initialization ownership before later driver configuration can fail.
  Roll back only channels acquired by that instance, check Uninitialize results,
  and release Windows receive events after native shutdown. Keep failed cleanup
  reachable through the transport/pool for explicit recovery.

- Release the local SLCAN serial handle even when the adapter's close command
  fails after USB disconnection. Retain partial initialization and failed serial
  cleanup for close/recover retries in classic CAN and python-can CAN FD.

- Add `PythonCANFDTransport` for python-can CAN FD backends and CANable 2.0
  SLCAN FD. Snapshot serial/bit-timing configuration, include pyserial in FD
  extras, and share DLC/FD/BRS conversion with Linux SocketCAN FD. Keep remote
  and error frames out of device payloads and preserve receive timestamps in
  microseconds. Roll back FD worker startup failures with retryable cleanup.

- Configure classic CAN bitrate, local echoes and immutable python-can backend
  options, including Windows PCAN. Ignore ambient driver configuration and
  reject conflicting live channel settings before opening another handle.
  Retain channel ownership until failed shutdown has been cleaned up.

- Retain shared-pool ownership when the first transport open and its cleanup
  both fail. Subsequent acquisition recovers the same zero-lease transport
  before granting access, preventing duplicate physical connections. Remove
  failed entries only after successful cleanup and preserve lifecycle interrupts.

- Roll back classic CAN bus and worker ownership when thread startup or managed
  opening is interrupted. Preserve KeyboardInterrupt/SystemExit, retain failed
  cleanup for explicit close retry, and keep interrupted close faulted.

- Add `TransmitArbiter.submit_fifo_batch()` with all-or-none, frame-counted
  admission and non-interleaved dispatch. `BatchSendReceipt` exposes successful
  frame progress even after failure. Optional pending-age admission and
  pre-dispatch deadlines bound stale targets without enlarging queues.
- Expose expired/rejected-stale counts, oldest pending or in-flight age and
  in-flight batch size in zero-I/O transmit health. Outcome counters count jobs;
  capacity and pending sizes count frames.

- Avoid starting the unused legacy CAN send worker in `CANTransport`.
  Direct dispatcher users retain eager startup by default; lazy dispatchers
  start that worker only if their asynchronous `send()` is actually used.

- Add the repository gates this package was missing entirely. It is a dependency of both `linkerbot-hand` and `linkerbot-hand-ros` and has six versions in the package registry, yet it had no CI, no pre-commit hooks and no release script — the only one of the three repositories with nothing but a `MANIFEST.in`. `.gitea/workflows/ci.yml` runs `ruff check` and `ruff format --check` on pushes to `main` and on pull requests, plus one pull-request check: changing `src/` without touching `CHANGELOG.md` fails, with a `[skip changelog]` escape in the pull-request title for a genuine internal refactor. `.pre-commit-config.yaml` uses `repo: local` to call the ruff already in the environment rather than pulling `ruff-pre-commit`, which ships its own pinned version, so the hook, a manual run and CI stay on one ruff and one rule set; each clone needs `pre-commit install` once, and with no Gitea runner online the push hook is the gate that actually takes effect. The rule set is unchanged (`E4`/`E7`/`E9`/`F`/`I`/`UP`). `pre-commit` and `twine` joined the dev dependency group — the latter was already required by the release path and undeclared.

- Add a release flow: `scripts/release.py` (`check` / `tag` / `publish`) and `.gitea/workflows/release.yml`, documented in [scripts/README.md](scripts/README.md). A release here is **six** things staying consistent: the version in `pyproject.toml`, a dated `CHANGELOG.md` section, the pinned version in both READMEs, a signed tag, the registry artifacts, and a Gitea Release carrying those artifacts. The 2.0.0 releases of hand and ROS got the first two right, missed the next three — producing a version nobody could install — and then missed the sixth even after the rest was repaired, leaving the only tag in either repository without a Release. `check` gates all of it: clean tracked tree, `main` equal to `origin/main`, a dated section with an empty `Unreleased`, both READMEs pinning exactly this version and no other (the `[can]`/`[canfd]` extras forms as well as the bare one), an unused tag, `ruff` and `pytest`, and the version not already published. An unreachable registry reports "not checked" rather than reading as "nothing is published", so one network failure cannot wave through a re-upload. `publish` additionally verifies the artifacts rather than just the build: `twine check`, the wheel metadata version, the version pinned in the wheel's long description (`readme` puts README inside the artifact, so a stale pin is permanent), and that the sdist contains no untracked file. It then uploads and creates the Gitea Release with the CHANGELOG section as its body and both artifacts attached. Idempotent throughout: the upload is skipped when the registry already has the version and the Release when one already exists, so a local run and a CI run cannot double-publish.

- `publish` requires `v<version>` to exist **and to point at HEAD** — the inverse of the gate `tag` uses. This comes from a real defect in the hand script, which skipped the working-tree and tag gates on the publish path to accommodate CI's detached tag checkout: the effect was that nothing related the built source to the tag at all. Reproduced with `HEAD` one commit ahead of the tag and a modified tracked file, all four gates passed, which would have uploaded source present in no commit. Registry versions cannot be overwritten, so such an artifact is permanent and anyone checking out the tag later to reproduce or audit it gets different source. In CI the workflow checks out the tag and the gate holds automatically; locally it forces `git checkout v<version>` before publishing.

- `scripts/release.py` reads the version from `pyproject.toml` with a regex instead of `tomllib`, which is not in the standard library on Python 3.10 — the minimum this package supports.

- Bind MiniCANFD's Windows `HCanbus.dll` with its documented single-channel
  stdcall API, without Linux-only Init/Exit calls. Validate channel selection,
  retain DLL dependency directories, and fail explicit library paths without
  falling back to another binary. Keep failed initialization cleanup reachable
  through managed close/recover and the shared pool.

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
