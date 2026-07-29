# Design: TQ-Compressed HNSW Vector Index

## Overview

> **Status (2026-06-11):** per maintainer review, TQ is exposed through the existing
> `COMPRESSION` argument on the `HNSW` algorithm (`COMPRESSION TQ8|TQ4|TQ2`), mirroring
> how `SVS-VAMANA` exposes LVQ/LeanVec compression. The standalone `TQ-HNSW` algorithm
> name on `FT.CREATE` was dropped. Internally the index is still backed by the
> `VecSimAlgo_TQ_HNSW` enum and the same tiered wrapping.

TQ-compressed HNSW is an approximate vector index mode backed by the vendored VecSim
TurboQuant-style compressed representation plus HNSW graph traversal.

At the RediSearch layer, the goal is the same as the existing vector algorithms:

- users create the index with `FT.CREATE` (as `HNSW` with `COMPRESSION TQ<bits>`)
- users write raw vectors into documents
- users query with raw vectors through `FT.SEARCH`
- VecSim owns the internal preprocessing, compressed storage, and HNSW traversal

## User-Facing Schema

TQ compression is accepted anywhere `VECTOR HNSW` fields are accepted in `FT.CREATE`.

Example:

```redis
FT.CREATE idx SCHEMA vec VECTOR HNSW 10 \
  TYPE FLOAT32 \
  DIM 768 \
  DISTANCE_METRIC COSINE \
  COMPRESSION TQ8 \
  M 16 EF_CONSTRUCTION 200 EF_RUNTIME 50 EPSILON 0.01
```

Supported parameters in v1:

- `TYPE`
- `DIM`
- `DISTANCE_METRIC`
- `COMPRESSION` — `TQ8` (recommended default, 8-bit), `TQ4`, or `TQ2`; the bit budget
  is encoded in the name (like `LVQ4`/`LVQ8` for SVS); case-insensitive
- `M`
- `EF_CONSTRUCTION`
- `EF_RUNTIME`
- `EPSILON`

The standard HNSW arguments (`M`, `EF_CONSTRUCTION`, `EF_RUNTIME`, `EPSILON`) are all
optional and keep their HNSW defaults:

- `M=16`
- `EF_CONSTRUCTION=200`
- `EF_RUNTIME=10`
- `EPSILON=0.01`

The former TQ knobs `BITS`, `PROJECTIONS`, `SEED`, `ROTATION` are **not user-exposed**;
they were deliberately removed from the API per review. Internally they are fixed:

- projections: `max(1, DIM / 2)`
- seed: `7`
- rotation: always on

The stored representation applies a full-dimensional orthogonal rotation, groups the
rotated coordinates into pairs, and stores an FP32 radius plus a quantized polar angle
per pair. It also stores two FP32 norms and packed signs from `DIM / 2` QJL projections
of the reconstruction residual. The projection count controls the residual sketch; it
does not reduce the rotation dimension.

TQ2, TQ4, and TQ8 use 1, 3, and 7 polar bits respectively. TQ2 and TQ4 both use the
current nibble-packed angle layout, so they have the same byte footprint in this
implementation; TQ2 has lower angular resolution without an additional memory saving.

`BLOCK_SIZE` / `INITIAL_CAP` are deprecated args and not part of the TQ surface
(block size is fixed at 1024 vectors per block internally).

Internally, when `COMPRESSION TQ<bits>` is present, the parser builds the same tiered
index as plain `HNSW` but with `VecSimAlgo_TQ_HNSW` as the primary algorithm and
`TQHNSWParams` (type, dim, metric, multi, `M`, `efConstruction`, `efRuntime`,
`epsilon` carried over from the parsed HNSW args; bits from the compression name;
projections/seed/rotation fixed defaults). Background indexing via tiered worker jobs
is identical to `HNSW`.

## Current Constraints

The initial RediSearch integration intentionally keeps the surface narrow. All are
parse-time validation errors:

- `TYPE FLOAT32` only — `"TQ compression only supports FLOAT32 vectors"`
- even vector dimensions only — `"TQ compression requires an even vector dimension"`
- `COSINE` / `IP` only — `"TQ compression with DISTANCE_METRIC L2 is not yet
  supported; use COSINE or IP"`
- single-value vectors only — `"TQ compression does not support multi-value vectors"`
- not supported for disk-backed indexes — `"Disk index does not support COMPRESSION"`
- an unknown `COMPRESSION` value on `HNSW` fails with the standard bad-argument error
  mentioning `COMPRESSION`

Unsupported forms fail during schema validation rather than being accepted and
degraded later.

## Query Semantics

TQ-compressed HNSW is exposed through the standard vector query syntax.

KNN example:

```redis
FT.SEARCH idx "*=>[KNN 10 @vec $blob AS dist]" PARAMS 2 blob <raw-f32-bytes> SORTBY dist DIALECT 2
```

Range query example:

```redis
FT.SEARCH idx "@vec:[VECTOR_RANGE 0.2 $blob]=>{$yield_distance_as: dist}" PARAMS 2 blob <raw-f32-bytes> SORTBY dist DIALECT 2
```

The query vector stays raw. The index handles its own preprocessing internally. For
cosine fields, both stored and query vectors are normalized and yielded distances are
`1 - estimated_inner_product`, so `VECTOR_RANGE` uses the standard approximate
cosine-distance scale.

## Introspection

`FT.INFO` reports the plain-HNSW block plus the compression — the algorithm is
reported as `HNSW`, not `TQ-HNSW` (mirroring how SVS reports its compressed variants):

- `algorithm` (`HNSW`)
- `data_type`
- `dim`
- `distance_metric`
- `M`
- `ef_construction`
- `compression` (`TQ8` / `TQ4` / `TQ2`)

It does **not** report `bits` / `projections` / `seed` / `rotation` /
`ef_runtime` / `epsilon`.

`INFO MODULES` counts TQ-compressed HNSW fields in the existing `HNSW` bucket; there
is no dedicated TQ counter (a compressed-HNSW counter is a possible follow-up).

## Persistence

TQ-compressed HNSW is supported by the current RDB save/load path via a single
tiered-`TQ_HNSW` branch in `VecSim_RdbLoad_v4`, which saves `swapJobThreshold` then type, dim, metric,
multi, bits, projections, seed, useRotation, `M`, `efConstruction`, `efRuntime`,
`epsilon`.

RediSearch encoding version 28 marks introduction of this layout. Older builds reject
version-28 files rather than partially loading an unknown algorithm; the new loader
continues to load older non-TQ files.

## Testing Focus

The expected coverage areas for the RediSearch layer (see
`tests/pytests/test_tq.py`) are:

- schema parsing and validation, including the negative cases above
- `FT.INFO` rendering (HNSW block + `compression`)
- KNN and `VECTOR_RANGE`
- JSON single-value vs multi-value behavior (multi-value rejected)
- RDB and AOF round-trips
- INFO MODULES accounting (counted under `HNSW`)
