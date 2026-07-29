# Design: TQ Flat Backend (Internal)

## Overview

> **Status (2026-06-11):** the TQ flat backend is **internal-only**. The standalone
> `TQ-FLAT` algorithm name that earlier drafts exposed on `FT.CREATE` was dropped per
> maintainer review. Users cannot create a TQ flat index; the only user-facing TQ
> surface is `HNSW ... COMPRESSION TQ8|TQ4|TQ2` (see [tq_hnsw.md](tq_hnsw.md)).

The TQ flat backend (`VecSimAlgo_TQ`) is an exhaustive-scan vector index inside the
vendored VecSim TurboQuant-style compressed implementation. It remains in the codebase
as a building block: it serves as the compressed brute-force component of the tiered
TQ-HNSW composition (tiered frontend role) and is exercised directly by VecSim-level
tests and benchmarks. The scan is exhaustive, but its scores are approximate because
stored vectors are compressed.

Engine behavior is unchanged by the API rework:

- each inserted vector is rotated and quantized once at insertion time
- queries scan every entry using the TQ distance kernels (asymmetric by default)
- VecSim owns the internal preprocessing and compressed storage

For an even dimension `d`, the current storage representation contains:

- `d / 2` FP32 radii, one for each pair of rotated coordinates
- two FP32 norm values
- a quantized polar-angle code for each coordinate pair
- one packed sign bit for each of `d / 2` residual projections

The compression name controls the polar resolution: TQ2, TQ4, and TQ8 use 1, 3,
and 7 polar bits respectively. TQ2 and TQ4 both use the current nibble-packed angle
layout, so they have the same byte footprint; TQ2 trades angle resolution for no
additional storage saving in this implementation.

## Internal Parameters

The backend is configured through `TQFlatParams` (VecSim-side), never through
`FT.CREATE`. The former user knobs were deliberately removed from the API per review
and are now fixed internal defaults:

- bits: taken from the compression name of the owning index (`TQ8`=8, `TQ4`=4, `TQ2`=2)
- projections: `max(1, DIM / 2)`
- seed: `7`
- rotation: always on (disabling exists VecSim-side for diagnostics/parity only)
- block size: fixed at 1024 vectors per block (`BLOCK_SIZE` / `INITIAL_CAP` are
  deprecated args and not part of the TQ surface)

## Current Constraints

- `FLOAT32` only
- even vector dimensions only
- single-value vectors only
- not available on disk-backed indexes (the owning `COMPRESSION` argument is rejected
  there at parse time)

These are enforced at schema validation time on the user-facing `HNSW` +
`COMPRESSION TQ<bits>` surface rather than being accepted and degraded later.

## Role in Query Semantics

The backend is never queried directly by users. Within a tiered TQ-compressed HNSW
index, it participates in the standard tiered query merge: results from the flat
frontend buffer are merged with the TQ-HNSW backend results, exactly as for plain
`HNSW`. Query vectors stay raw FP32; the index handles its own preprocessing
internally.

For cosine fields, storage and query vectors are normalized before estimation and the
reported distance is `1 - estimated_inner_product`. This keeps yielded distances and
`VECTOR_RANGE` radii on the standard approximate cosine-distance scale.

## Introspection

There is no user-visible TQ-flat introspection:

- `FT.INFO` never reports a `TQ-FLAT` algorithm; TQ-compressed fields report
  `algorithm=HNSW` plus a `compression` line (`TQ8`/`TQ4`/`TQ2`).
- `INFO MODULES` no longer has a `TQ_FLAT` counter (it was removed along with the
  standalone index); TQ-compressed HNSW fields count in the existing `HNSW` bucket.

## Persistence

There is no standalone TQ RDB branch anymore; the backend's state is persisted as
part of the tiered-`TQ_HNSW` branch in `VecSim_RdbLoad_v4` (see
[tq_hnsw.md](tq_hnsw.md)). RediSearch encoding version 28 marks introduction of this
layout. Older builds reject version-28 files; the new loader continues to accept
older non-TQ files.

## Testing Focus

Coverage for this backend now lives at two levels:

- VecSim-side: TurboQuant parity harness (Rust oracle), SIMD kernel parity, and
  backend unit tests
- RediSearch-side: indirectly via `tests/pytests/test_tq.py`, which exercises the
  tiered TQ-compressed HNSW path (create/info/KNN/range/persistence/negative cases)
