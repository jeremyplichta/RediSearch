# Design: Paper-Faithful TQ Flat Backend (Internal)

## Overview

> **Status (2026-08-25):** the TQ flat backend is internal-only. RediSearch exposes immutable
> model bundles through `HNSW ... COMPRESSION TQ8|TQ4|TQ2 [TQ_PROFILE ...]`; all new TQ schema
> creation remains behind `ENABLE_UNSTABLE_FEATURES`. Omitting `TQ_PROFILE` preserves dense
> marker 2, while markers 3 and 4 are explicit fast-profile selections.
> See [tq_hnsw.md](tq_hnsw.md).

`VecSimAlgo_TQ` is the exhaustive-scan component used by tiered TQ-HNSW. Stored vectors
use Algorithm 2 (`TurboQuant_prod`) from the TurboQuant paper. Search is exhaustive but
scores remain approximate because the candidates are compressed.

For an input `x`, the storage preprocessor first records
`alpha = ||x||_2` for IP (`alpha = 1` for a nonzero cosine vector) and quantizes the
unit vector `u = x / ||x||_2`. Zero vectors use `alpha = gamma = 0`.

The model then:

1. applies the deterministic orthogonal rotation `Pi`;
2. quantizes each rotated coordinate with the shared Lloyd-Max codebook for the exact
   sphere-coordinate density, using `b - 1` bits per coordinate;
3. reconstructs the coarse unit vector `u_mse` and forms `r = u - u_mse`;
4. stores `gamma = ||r||_2`;
5. for nonzero `r`, stores `sign(S (r / gamma))` using `m = d` Gaussian QJL rows.

The codebook, rotation, and QJL state are deterministic shared model state derived from
the dimension, bit width, seed, and immutable component versions. They are allocated
through the VecSim allocator, shared by the codec users in an index, included in VecSim
memory accounting, and never repeated per vector.

## Stored Layout

For total advertised bit width `b` and dimension `d`, each vector stores, in order:

- `ceil((b - 1) * d / 8)` bytes of LSB-first packed Lloyd-Max indices;
- `ceil(d / 8)` bytes of LSB-first QJL signs;
- one FP32 source scale `alpha` (required to support non-unit IP vectors);
- one FP32 residual norm `gamma`.

The exact payload is therefore:

```text
ceil((b - 1) * d / 8) + ceil(d / 8) + 8 bytes
```

At dimension 1024 this is 264 bytes for TQ2, 520 bytes for TQ4, and 1032 bytes for
TQ8. The bitstreams themselves are exactly 2, 4, and 8 bits/dimension; the eight-byte
metadata overhead makes the physical costs 2.0625, 4.0625, and 8.0625 bits/dimension.
There are no per-coordinate or per-pair FP32 radii.

Zero encoding is canonical. A zero source writes an all-zero payload (`alpha = gamma = 0`)
and scores as inner product zero. If a nonzero source has an exactly zero residual, its
coarse code and `alpha` remain meaningful, `gamma` is zero, every used residual-sign bit
encodes `sign(0) = +1`, and every unused packed tail bit is zero. The sign stream is then
ignored by the estimator because the residual factor is zero.

## Asymmetric Scoring

For a query `y`, preprocessing computes `Pi y` and `S y`. The estimator is

```text
alpha * (
  <Pi y, c[q_mse]> +
  gamma * sqrt(pi / 2) / m * <S y, q_sign>
)
```

where `c[q_mse]` is the coarse Lloyd-Max reconstruction and each component of
`q_sign` is `+1` or `-1`. The residual factor is `sqrt(pi / 2) / m`, not
`pi / (2m)`. The residual term is skipped exactly when `gamma == 0`.

Cosine normalizes both source and query and returns `1 - estimate`. IP leaves the
query unnormalized, restores the source magnitude through `alpha`, and also returns
`1 - estimate`, matching VecSim's IP-distance convention.

The optimized scorer vectorizes the two candidate/query dot products with NEON on
ARM64 and SSE2 on x86. Packing, codebook lookup, tails, and model construction remain
scalar. Randomized tests require scalar/SIMD parity within floating-point tolerance
for every bit width, odd/tail dimensions, and unaligned candidate storage.

## Internal Parameters and Constraints

`TQFlatParams` is not user-facing. RediSearch fixes:

- total bits from the compression name (`TQ2`, `TQ4`, or `TQ8`);
- QJL projections to `DIM`, as required by Algorithm 2's `d` sign bits;
- seed to `7`;
- rotation on.

RediSearch also passes an explicit `VecSimTqProfile` selected by the optional `TQ_PROFILE`
schema token. VecSim keeps value 0 as a dense compatibility default for zero-initialized callers.

Current product constraints are `FLOAT32`, dimension at least 2, COSINE or IP,
single-value vectors, and no disk-backed index. Odd dimensions are supported.

`DenseReferenceV1` uses a dense Haar rotation and independent dense-Gaussian QJL rows.
`FastStructuredRotationV1` replaces only the rotation; `FastStructuredV1` also replaces QJL with
`CirculantGaussianQjlV1`. These component swaps do not change payload meaning. Circulant QJL rows
are correlated and retain separate statistical/quality gates. Dense remains the omitted default;
the fast profiles must be named explicitly while TQ creation is unstable-gated.

## Direct Flat Role and HNSW Maintenance

The TQ flat backend is an internal direct VecSim/conformance building block; it is not
the tiered TQ-HNSW staging frontend. Tiered TQ-HNSW uses the ordinary raw FP32
brute-force frontend, then compresses vectors as background jobs insert them into the
TQ-HNSW primary. The paper does not define a compressed-code-to-compressed-code
estimator for that primary, and this implementation does not invent one. Dense marker 2 uses
`FullDecodeReferenceV1`, which decodes both Algorithm 2 approximations. Fast markers 3 and 4 use
the separately versioned, allocation-free `CoarseMseV1` metric on the two Algorithm 1 coarse
reconstructions. Neither mode stores a raw-vector sidecar; see [tq_hnsw.md](tq_hnsw.md).

## Persistence and Tests

TQ state is persisted only as part of tiered TQ-HNSW. RediSearch encoding version 29
uses marker `2` for the immutable dense identity: `PaperV1` payload,
`DenseReferenceV1` model, `DenseHaarV1` rotation, `DenseGaussianV1` QJL,
`FullDecodeReferenceV1` construction score, and the cosine/IP metric contract. Marker 3 binds
fast rotation, dense QJL, and `CoarseMseV1`; marker 4 binds fast rotation,
`CirculantGaussianQjlV1`, and `CoarseMseV1`. The
dimension, bits, metric, projections, seed, and required-rotation flag are persisted and
validated before profile-aware model estimation. Unknown markers and inconsistent fields fail load.
Encoding version 28 was the incompatible pairwise-polar prototype and is rejected rather
than interpreted as the new format. Marker 2's existing parameter bytes are unchanged.

Known marker-2/3/4 RDB state remains loadable independently of the unstable-feature creation gate.
RediSearch requires Redis' AOF RDB preamble and disables module command rewriting, so AOF restart
also restores the profile through its marker rather than reconstructing schema arguments.

VecSim tests derive their reference formulas independently from the paper and cover
the Lloyd-Max density/codebooks, bit packing, exact byte cost, zero cases, QJL bias,
residual scaling, IP magnitude, cosine behavior, scalar/SIMD parity, FullDecode and
CoarseMse HNSW maintenance. RediSearch tests cover parsing, tiering, HASH/JSON
lifecycle, hybrid paths, GC, score/config-preserving marker-2/3/4 RDB round trips, AOF restart, exact
payload sizes, and corrupt identity validation.
