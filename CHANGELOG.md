# Changelog

This project follows Semantic Versioning.

## Unreleased

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
