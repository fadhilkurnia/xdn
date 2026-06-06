# Decoupling XDN from its in-tree gigapaxos copy

XDN is a monorepo that vendors gigapaxos as in-tree source (`src/edu/umass/cs/{gigapaxos,reconfiguration,nio,protocoltask,txn,chainreplication,utils}`) and builds it from source rather than depending on a published jar. Over time the two have drifted far apart: measured from their common ancestor, **upstream `origin/master` is 4 commits ahead while `fork/main` is 442 commits ahead**. Re-syncing is now expensive and conflict-prone, and the cost grows every time either side moves.

This doc records why the drift hurts, the direction we chose, and a phased plan to make future syncs cheap without giving up the monorepo developer experience.

## The problem, concretely

Three independent forces make each sync harder than it should be:

1. **Whole-file formatting drift.** XDN reformatted the vendored gigapaxos files (tabs → spaces, re-wrapping). A 21-line *semantic* upstream change to `PaxosManager.java` lands inside a file git sees as almost entirely rewritten, so 3-way merge degrades to manual hunk-porting. This is the single biggest tax — it converts small upstream diffs into whole-file conflicts.

2. **Parallel implementations of the same idea.** XDN and upstream independently solved the same problem with inverse flags: XDN's `ENABLE_STARTUP_LEADER_ELECTION` vs upstream's `BOOTSTRAP_COORD_DETERMINISTIC` (see the Phase-0 merge `4a0c089b`). Neither knew about the other, so reconciliation required *understanding both* and unioning the gates rather than taking either side. This is the cost of changes that should have been upstreamed but weren't.

3. **XDN-only subsystems woven into shared packages.** XDN adds whole top-level packages that upstream does not have — `xdn`, `primarybackup`, `causal`, `clientcentric`, `eventual`, `pram`, `sequential` — and these *call into and are referenced by* the vendored gigapaxos/reconfiguration code. The boundary between "gigapaxos library" and "XDN application" is not clean, which is what makes a true library split non-trivial.

## Direction chosen

Two decisions frame the plan (made 2026-06-04):

- **XDN owns a gigapaxos fork.** The canonical MobilityFirst/gigapaxos upstream is effectively dormant. Rather than depend on it, XDN maintains a gigapaxos repository it controls (`xdn-gigapaxos`), pushes XDN's generic improvements there, and treats *that* as the upstream of record. The dormant MobilityFirst repo becomes an occasional cherry-pick source, not a dependency.

- **Keep the monorepo.** We are not splitting XDN into a multi-repo build today. The goal is "gigapaxos as a *tracked* library inside the monorepo" — ideally a git subtree — so the day-to-day developer experience (single `ant` build, single checkout, cross-package refactors) is preserved while syncs become a mechanical subtree pull instead of a manual merge.

The end state is: **gigapaxos is a subtree-tracked library whose canonical home is XDN's own fork; XDN-specific code lives outside the subtree; generic fixes flow back to the fork.**

## Phased roadmap

### Phase 0 — Sync the outstanding upstream commits *(done — `4a0c089b`)*

Port the 4 outstanding `origin/master` commits onto `fork/main` so XDN is current before any restructuring: `BOOTSTRAP_COORD_DETERMINISTIC` + `RUN_IF_NOT_RUN_YET`, `RESEND_HIGHER_PREPARE`, `FORWARD_PREEMPTED_REQUESTS=false`, `scheduleSinglePoke`, and the upstream CI workflow. The `notRunYet()` gate was reconciled as a union of XDN's and upstream's flags. Low risk, immediate value, and it establishes a clean baseline for the steps below.

### Phase 1 — Kill the formatting tax (highest ROI)

Before anything structural, eliminate force #1. Agree on **one** formatter for the vendored gigapaxos packages and apply it identically on both sides:

- Adopt the same `google-java-format` config XDN already runs in CI (`.github/workflows/google-java-format.yml`) for the gigapaxos source tree, and apply the *identical* formatter to the XDN-owned gigapaxos fork.
- Land the reformat as a single, isolated commit on both sides so future `git diff`/subtree merges compare like-for-like and surface only semantic changes.
- Record the formatter version/config in the repo so it can't drift again.

This alone turns most future "whole-file conflict" syncs back into small semantic diffs.

### Phase 2 — Establish the XDN-owned gigapaxos fork as upstream-of-record

- Create/confirm `xdn-gigapaxos` (the repo XDN controls) seeded from the current reconciled gigapaxos source (post-Phase-1 formatting).
- Mirror the dormant MobilityFirst history into it as a base so future cherry-picks from MobilityFirst remain attributable.
- Document the contribution rule: **generic** paxos/reconfiguration/nio fixes are authored against (or promptly back-ported to) `xdn-gigapaxos`, never only in XDN.

### Phase 3 — Draw the library boundary

Separate "library" from "application" inside the monorepo so the subtree has a clean edge:

- **Library (subtree candidate):** `gigapaxos`, `reconfiguration`, `nio`, `protocoltask`, `txn`, `chainreplication`, `utils` — the packages that also exist upstream.
- **XDN application (stays in XDN, outside subtree):** `xdn`, `primarybackup`, `causal`, `clientcentric`, `eventual`, `pram`, `sequential`.
- Audit and remove inbound references from library → XDN packages (the library must not import XDN-only code). Where XDN needs to influence library behavior, do it through existing extension points (config keys, pluggable `*Coordinator`/`ReconfiguratorDB`/`PaxosLogger` interfaces) rather than edits to library internals. The SQLite/RocksDB backends merged in #53 are the model: they extend the JDBC/logger abstractions instead of forking the core.

### Phase 4 — Convert the library tree to a git subtree

- Replace `src/edu/umass/cs/{library packages}` with a `git subtree` mapped to `xdn-gigapaxos`.
- Syncing upstream becomes `git subtree pull`; contributing back becomes `git subtree push` (or PR to `xdn-gigapaxos`).
- Keep the single-`ant` build: the subtree is still plain source on disk, so the monorepo build is unchanged.

### Phase 5 — Steady state

- New generic fix → land in `xdn-gigapaxos` → `subtree pull` into XDN.
- New XDN feature → land in XDN application packages, using library extension points.
- Periodically cherry-pick anything worthwhile from dormant MobilityFirst upstream into `xdn-gigapaxos`.

## Guardrails / lessons from Phase 0

- **Reconcile, don't pick a side, when both implemented the same feature.** The `notRunYet()` union is the template: understand both flags, preserve both gates.
- **Resolve formatting once, then split semantics back out.** During Phase 0 we kept XDN's formatting (`checkout --ours`) and re-applied upstream hunks on top, rather than fighting whole-file conflicts — the same instinct Phase 1 makes permanent.
- **Prefer extension points over core edits.** Every time XDN edits library internals instead of extending an interface, it creates a future merge conflict. The DB-backend work (#53) is the example to follow.
