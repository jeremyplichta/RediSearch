# Design: Paper-Faithful TQ Flat Backend (Internal)

## Overview

> **Status (2026-08-21):** the TQ flat backend is internal-only. The public surface is
> `HNSW ... COMPRESSION TQ8|TQ4|TQ2`; see [tq_hnsw.md](tq_hnsw.md).

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

The codebook, rotation, and QJL matrix are deterministic model state derived from the
dimension, bit width, and seed. They are not repeated per vector.

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

Current product constraints are `FLOAT32`, dimension at least 2, COSINE or IP,
single-value vectors, and no disk-backed index. Odd dimensions are supported.

## Tiered Role and HNSW Maintenance

The flat backend is the tiered frontend and uses the same asymmetric query scorer as
the HNSW backend. The paper does not define a compressed-code-to-compressed-code
estimator. HNSW graph maintenance therefore decodes both stored Algorithm 2
approximations and compares those decoded vectors. This correctness-first strategy
stores no raw-vector sidecar; see [tq_hnsw.md](tq_hnsw.md).

## Persistence and Tests

TQ state is persisted only as part of tiered TQ-HNSW. RediSearch encoding version 29
adds paper codec marker 2. Encoding version 28 was the incompatible pairwise-polar
prototype and is rejected rather than interpreted as the new format.

VecSim tests derive their reference formulas independently from the paper and cover
the Lloyd-Max density/codebooks, bit packing, exact byte cost, zero cases, QJL bias,
residual scaling, IP magnitude, cosine behavior, scalar/SIMD parity, and explicit
decode-based HNSW maintenance. RediSearch tests cover parsing, tiering, HASH/JSON
ingest, score-preserving RDB round trips, and negative validation.
