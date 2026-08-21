# Design: Paper-Faithful TQ-Compressed HNSW

## Overview

TQ is exposed through the existing `COMPRESSION` argument on `HNSW`:

```redis
FT.CREATE idx SCHEMA vec VECTOR HNSW 10 \
  TYPE FLOAT32 DIM 768 DISTANCE_METRIC COSINE COMPRESSION TQ8 \
  M 16 EF_CONSTRUCTION 200 EF_RUNTIME 50 EPSILON 0.01
```

The public algorithm remains `HNSW`; internally RediSearch builds a tiered index whose
primary backend is `VecSimAlgo_TQ_HNSW` and whose frontend is the internal TQ flat
index. Raw FP32 document and query blobs use the normal RediSearch ingest/query paths.
VecSim owns compression and scoring.

## Algorithm and Storage

The implementation follows `TurboQuant_prod` (Algorithm 2): a deterministic random
orthogonal rotation, an exact sphere-distribution Lloyd-Max quantizer using `b - 1`
bits per coordinate, and one QJL residual-sign bit per coordinate. It stores the
residual norm and applies the unbiased correction `gamma * sqrt(pi / 2) / d`.
See [tq_flat.md](tq_flat.md) for the equations and packing details.

Per-vector payload is:

```text
ceil((b - 1) * DIM / 8) + ceil(DIM / 8) + 8 bytes
```

The final eight bytes are the FP32 source scale needed by non-unit IP vectors and the
FP32 residual norm. At dimension 1024 the exact payload is 264/520/1032 bytes for
TQ2/TQ4/TQ8. Model-wide rotation, codebook, and QJL matrices are not per-vector bytes.

## Parameters and Validation

Supported schema parameters are the standard HNSW parameters plus `COMPRESSION`:

- `COMPRESSION TQ2|TQ4|TQ8` (case-insensitive);
- `M`, default 16;
- `EF_CONSTRUCTION`, default 200;
- `EF_RUNTIME`, default 10;
- `EPSILON`, default 0.01.

The former `BITS`, `PROJECTIONS`, `SEED`, and `ROTATION` knobs are not public.
RediSearch derives total bits from the compression name, fixes projections to `DIM`,
uses seed 7, and always enables rotation.

Validation currently requires:

- `TYPE FLOAT32`;
- `DIM >= 2` (odd dimensions are valid);
- `DISTANCE_METRIC COSINE` or `IP`;
- a single-value vector path;
- an in-memory index (disk-backed compression is rejected).

## Search and Graph Construction

Query traversal is asymmetric. VecSim preprocesses each query once into `Pi y` and
`S y`, then evaluates candidates directly from their packed indices, packed signs,
source scale, and residual norm. Cosine normalizes both sides. IP leaves the query
unnormalized and restores every source vector's original magnitude.

The TurboQuant paper does not specify a compressed-code-to-compressed-code estimator.
HNSW construction and maintenance need stored-to-stored comparisons, so the current
correctness-first path decodes both Algorithm 2 approximations and computes their
ordinary metric distance. This is explicit and tested; it does not retain raw vectors
and does not pretend that a new symmetric estimator came from the paper. Query
traversal continues to use the asymmetric estimator.

Tiered background indexing and frontend/backend result merging are otherwise the same
as ordinary HNSW.

## Introspection

`FT.INFO` reports the ordinary HNSW block plus `compression: TQ2|TQ4|TQ8`. It does not
expose the fixed internal projection count, seed, or rotation flag. `INFO MODULES`
counts these fields in the existing HNSW bucket.

## Persistence

Encoding version 29 stores paper codec marker `2` before the tiered TQ parameters,
followed by the tiered swap threshold, vector type/dimension/metric/multi flag,
bit width, projection count, seed, rotation flag, and HNSW parameters.

Encoding version 28 identified the historical pairwise-polar prototype. Its parameter
shape looks similar but its vector bytes and equations are incompatible, so the v29
loader rejects it. Older non-TQ indexes remain loadable through their existing paths.

RDB tests compare complete search results and scores before and after reload, rather
than checking only that documents survive.

## Testing Focus

Coverage includes all three bit widths, HASH and JSON ingest, COSINE and non-unit IP,
odd dimensions, tiered insertion/query behavior, KNN/range paths, FT.INFO, exact RDB
score preservation, invalid types/metrics/multi-value/dimensions, and VecSim-level
HNSW construction tests against the explicit Algorithm 2 decoder.
