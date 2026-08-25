# Design: Paper-Faithful TQ-Compressed HNSW

## Overview

The dense marker-2 TQ reference profile is available through the existing `COMPRESSION`
argument on `HNSW` only when `ENABLE_UNSTABLE_FEATURES` is enabled (it is off by default):

```redis
FT.CONFIG SET ENABLE_UNSTABLE_FEATURES true
FT.CREATE idx SCHEMA vec VECTOR HNSW 10 \
  TYPE FLOAT32 DIM 768 DISTANCE_METRIC COSINE COMPRESSION TQ8 \
  M 16 EF_CONSTRUCTION 200 EF_RUNTIME 50 EPSILON 0.01
```

The schema algorithm remains `HNSW`; internally RediSearch builds a tiered index whose
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
TQ2/TQ4/TQ8. Rotation, codebook, and QJL data are allocator-accounted shared model
state, not per-vector bytes; only the dense reference represents both transforms as
matrices.

A zero source has a canonical all-zero payload and therefore inner product zero. For a
nonzero source whose residual is exactly zero, `alpha` remains nonzero, `gamma` is zero,
and the used QJL sign bits encode `sign(0) = +1`; the unused tail bits in both packed
streams remain zero. Because `gamma == 0`, those sign bits do not contribute to scoring.

## Parameters and Validation

Supported schema parameters are the standard HNSW parameters plus `COMPRESSION`:

- `COMPRESSION TQ2|TQ4|TQ8` (case-insensitive);
- `M`, default 16;
- `EF_CONSTRUCTION`, default 200;
- `EF_RUNTIME`, default 10;
- `EPSILON`, default 0.01.

New TQ schema creation requires `ENABLE_UNSTABLE_FEATURES`. The flag does not disable
loading the known dense marker-2 RDB profile. Deprecated `INITIAL_CAP` is ignored and
normalized to zero before validation.

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
HNSW construction and maintenance need stored-to-stored comparisons. The current dense
marker-2 profile uses the versioned `FullDecodeReferenceV1` default: it decodes both
Algorithm 2 approximations and computes their ordinary metric distance. The separately
versioned `CoarseMse` candidate computes the exact metric on Algorithm 1 coarse
reconstructions in O(DIM), with no heap allocation. It is not paper-defined residual
scoring and is not enabled for the persisted dense profile; promotion to the Stage-1
production profile requires reviewed production-dimension recall evidence. Neither path
retains raw vectors. Query traversal always uses the asymmetric estimator.

Tiered background indexing and frontend/backend result merging are otherwise the same
as ordinary HNSW.

## Introspection

`FT.INFO` reports the ordinary HNSW block plus `compression: TQ2|TQ4|TQ8`, exact encoded
payload bytes, projection count, seed, and the immutable RDB/codec/model/rotation/QJL/
construction/metric identities. These fields are diagnostic: there is no public backend
selector. `vector_index_sz_mb` and the per-field memory statistic include VecSim's
allocator-accounted shared model, container, graph, and vector allocations; the exact
payload field deliberately excludes container alignment and graph overhead. `INFO MODULES`
counts TQ fields in the existing HNSW bucket.

## Persistence

Encoding version 29 stores dense-reference marker `2` before the tiered TQ parameters,
followed by the tiered swap threshold, vector type/dimension/metric/multi flag, bit width,
projection count, seed, required-rotation flag, and HNSW parameters. Marker `2`
deterministically binds `PaperV1`, `DenseReferenceV1`, `DenseHaarV1`,
`DenseGaussianV1`, `FullDecodeReferenceV1`, and `CosineOrInnerProductV1`; those component
versions are therefore derived without changing the existing bytes. A fast profile gets
a new marker only after its rotation, QJL, construction, quality, and benchmark gates pass.
The dense reference uses independent Gaussian QJL rows. The internal candidates
`FastStructuredRotationV1` and `CirculantGaussianQjlV1` are separately versioned;
circulant QJL has correlated rows and therefore requires its own bias/variance/tail and
end-to-end quality evidence. Their existence does not imply acceptance or a default.

Encoding version 28 identified the historical pairwise-polar prototype. Its parameter
shape looks similar but its vector bytes and equations are incompatible, so the v29
loader rejects it. Unknown markers and corrupt/inconsistent dense fields fail before
model estimation or index construction. Older non-TQ indexes remain loadable through
their existing paths.

RDB tests compare complete search results and scores before and after reload, rather
than checking only that documents survive.
AOF rebuilds from the unchanged schema and source documents, deterministically selecting
marker-2-equivalent dense configuration and seed `7`; the unstable-feature flag must be
enabled while replaying that schema.

## Testing Focus

Coverage includes all three bit widths, HASH and JSON ingest/update/delete/reindex,
COSINE and non-unit IP (including zero vectors/residuals), odd dimensions, tiered
foreground/background transitions, repair and GC, KNN/range/hybrid paths, FT.INFO,
exact RDB score/config preservation, corrupt marker/field rejection, and VecSim-level
FullDecode/CoarseMse construction tests.
