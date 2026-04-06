# Design: TQ-FLAT Vector Index

## Overview

`TQ-FLAT` is a new exact flat vector index mode backed by the vendored VecSim TurboQuant-style compressed scan implementation.

At the RediSearch layer, the goal is to make it feel like the existing vector algorithms:

- users create the index with `FT.CREATE`
- users write raw vectors into documents
- users query with raw vectors through `FT.SEARCH`
- VecSim owns the internal preprocessing and compressed storage

## User-Facing Schema

`TQ-FLAT` is accepted anywhere `VECTOR` fields are accepted in `FT.CREATE`.

Example:

```redis
FT.CREATE idx SCHEMA vec VECTOR TQ-FLAT 14 \
  TYPE FLOAT32 \
  DIM 768 \
  DISTANCE_METRIC COSINE \
  BITS 8 \
  PROJECTIONS 384 \
  SEED 7 \
  ROTATION ON
```

Supported parameters in v1:

- `TYPE`
- `DIM`
- `DISTANCE_METRIC`
- `INITIAL_CAP`
- `BLOCK_SIZE`
- `BITS`
- `PROJECTIONS`
- `SEED`
- `ROTATION`

Defaults:

- `BITS=8`
- `PROJECTIONS=max(1, DIM / 2)`
- `SEED=7`
- `ROTATION=ON`

## Current Constraints

The initial RediSearch integration intentionally keeps the surface narrow:

- `TYPE FLOAT32` only
- single-value vectors only
- not supported for disk-backed indexes

Unsupported forms should fail during schema validation rather than being accepted and degraded later.

## Query Semantics

`TQ-FLAT` is exposed through the standard vector query syntax.

KNN example:

```redis
FT.SEARCH idx "*=>[KNN 10 @vec $blob AS dist]" PARAMS 2 blob <raw-f32-bytes> SORTBY dist DIALECT 2
```

Range query example:

```redis
FT.SEARCH idx "@vec:[VECTOR_RANGE 0.2 $blob]=>{$yield_distance_as: dist}" PARAMS 2 blob <raw-f32-bytes> SORTBY dist DIALECT 2
```

The query vector stays raw. The index handles its own preprocessing internally.

## Introspection

`FT.INFO` reports `TQ-FLAT` as the algorithm and exposes the TQ-specific configuration:

- `algorithm`
- `data_type`
- `dim`
- `distance_metric`
- `bits`
- `projections`
- `seed`
- `rotation`

`INFO MODULES` also counts `TQ_FLAT` vector fields separately from `FLAT`, `HNSW`, and `SVS-VAMANA`.

## Persistence

`TQ-FLAT` is supported by the current RDB save/load path.

Older encoding versions do not understand the new algorithm and are expected to reject it rather than load partially valid state.

## Testing Focus

The expected coverage areas for the RediSearch layer are:

- schema parsing and validation
- `FT.INFO` rendering
- KNN and `VECTOR_RANGE`
- JSON single-value vs multi-value behavior
- RDB and AOF round-trips
- INFO MODULES accounting
