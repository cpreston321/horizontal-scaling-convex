# Upstream port plan

Bringing this fork up to current `get-convex/convex-backend`.

## Base commit

The fork began as a vendored snapshot, not a git fork, so it had no common
ancestor with upstream. The snapshot matches:

    e09cab675614d3f7b53b983afbea5892b076d879   2026-03-31 04:00 UTC
    "Update screenshots including `<DeploymentSummary />` (#49179)"

Verified by tree-diffing every upstream commit in the window against the
flatten commit `d7aa491084`. The only residual differences are this fork's own
first commit (`crates/database/src/commit_delta.rs`, +158) and a two-line
redaction of a sample API key in `crates/workos_client/src/lib.rs`.

## Restoring the graft

A `git replace` graft gives git a real merge base. The replace ref is pushed to
this repo, so a fresh clone picks it up with:

    git fetch origin '+refs/replace/*:refs/replace/*'

To recreate it from scratch:

    git remote add convex https://github.com/get-convex/convex-backend.git
    git fetch convex main
    git replace --graft d7aa491084 e09cab6756
    git merge-base dev convex/main    # -> e09cab675

## Divergence, as of convex/main 83119f930

| | |
| --- | --- |
| Upstream commits since base | 2074 |
| This fork's commits since base | 348 |
| Files this fork touches | 388 |
| Files upstream touched | 3940 |
| Overlap (conflict surface) | 278 |

A trial `git merge convex/main` produces **208 conflicted files, 396 hunks**,
and no delete/modify conflicts — upstream has not removed anything this fork
depends on.

| Conflict size | Files |
| --- | --- |
| >= 10 hunks | 6 |
| 3-9 hunks | 36 |
| 1-2 hunks | 166 |

The six large ones carry most of the risk:

| Hunks | File |
| --- | --- |
| 35 | `Cargo.lock` (regenerate, do not hand-merge) |
| 31 | `crates/database/src/committer.rs` |
| 16 | `crates/local_backend/src/public_api.rs` |
| 13 | `crates/database/src/database.rs` |
| 12 | `crates/database/src/subscription.rs` |
| 10 | `crates/local_backend/src/lib.rs` |

`committer.rs` is the fork's largest change (+2638/-72 against base) and one of
upstream's most active files (37 commits since the base). Resolve it last, with
the surrounding features already in place.

## Suggested order

1. Infrastructure: `Cargo.toml`, `Cargo.lock` (take upstream, regenerate),
   `rust-toolchain`, `.github/`, formatting configs.
2. The 166 one- and two-hunk files. Mostly upstream renames and signature
   changes; the fork has no stake in most of them.
3. Feature-sized groups, each compiling before the next:
   commit delta / distributed log -> partition map and placement ->
   NATS log and replica consumer -> Raft node -> two-phase commit ->
   route authority, mutation forwarding, owner reads.
4. `committer.rs`, `database.rs`, `subscription.rs`.

## Must not miss

- **Renumber the migration.** This fork's `DATABASE_VERSION = 125` collides
  with an unrelated upstream migration 125. Upstream is now at 132, so the
  `_catalog_versions` migration becomes **133** and `DATABASE_VERSION` follows.
  Leaving it at 125 makes every deployment skip a migration in silence.
- **The build system moved.** Upstream now drives the Docker build through
  pnpm, mise, and uv; this fork's `Dockerfile.backend` still uses npm/rush cache
  mounts. Take upstream's build stages wholesale rather than merging hunks.
- **Publish both architectures.** The current image is arm64-only. Add
  `--platform linux/amd64,linux/arm64` to the release workflow.

## Validating

    cargo build --release -p local_backend --bin convex-local-backend
    cargo test -p database
    cd self-hosted/docker && ./test.sh scaling && ./test.sh failover

Then rehearse a snapshot `export` / `import --replace-all` against a scratch
deployment before trusting the result with real data.
