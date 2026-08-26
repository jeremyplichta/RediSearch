# HLD: TurboQuant (TQ) Compression for HNSW Vector Indexes

| **PRD**             | TBD                                                            |
| ------------------- | -------------------------------------------------------------- |
| **Document owner**  | Jeremy Plichta                                                 |
| **Document status** | DRAFT v0.9 (selectable immutable profiles; production benchmark gates outstanding) |
| **RediSearch branch** | `codex/tq-production-20260824` (local review checkpoint) |
| **VecSim branch**   | `codex/tq-production-20260824` |

| Row | Version | Revision date | Revision Note |
| --- | ------- | ------------- | ------------- |
| 1   | 0.1     | 2026-05-12    | First draft, pre-review |
| 2   | 0.2     | 2026-05-13    | Clarify §5.6 dataset is the Cohere-embedded MS MARCO v2.1 variant (HF card linked) |
| 3   | 0.3     | 2026-05-13    | Link file references to the GitHub fork branch; surface branch links in the header |
| 4   | 0.4     | 2026-06-11    | API reworked per review: TQ exposed via HNSW `COMPRESSION` argument (`TQ2`/`TQ4`/`TQ8`); standalone `TQ-FLAT`/`TQ-HNSW` algorithm names and `BITS`/`PROJECTIONS`/`SEED`/`ROTATION` knobs removed |
| 5   | 0.5     | 2026-07-08    | Synced with branch state after merging latest master (twice); suites green (C 742+130, Python 2139/2140 — remaining failure is a local-env TLS port conflict, unrelated to TQ; VecSim TQ 14, bench harness 16); added Risk 9 (cosine distance scale) |
| 6   | 0.6     | 2026-07-28    | Corrected the algorithm and storage description, advanced the RDB encoding version to 28, added even-dimension validation and TQ2/IP/disk/cosine-scale coverage, removed the incorrect cosine-scale risk, and made the compressed-SVS comparison an explicit sign-off gate |
| 7   | 0.7     | 2026-08-21    | Replaced the historical pairwise-polar codec with paper-faithful `TurboQuant_prod`: exact-density Lloyd-Max quantization, normalized QJL residual signs, unbiased correction, exact bit budgets, odd dimensions, explicit decoded HNSW maintenance, and RDB codec versioning |
| 8   | 0.8     | 2026-08-24    | Added immutable component/persistence identity, allocator-backed shared model accounting, the unstable reference-profile creation gate, structured-profile rollout gates, safe query contexts, exact 8-byte metadata accounting, and the versioned FullDecode/CoarseMse construction policy |
| 9   | 0.9     | 2026-08-25    | Added the optional immutable `TQ_PROFILE` selector, assigned distinct markers 3 and 4 to the fast profiles without changing marker 2, and documented profile-aware estimation, introspection, RDB/AOF persistence, and tests |

## Table of Contents

1. Introduction
2. Goals and Objectives
3. Architecture Overview
4. Interfaces (APIs)
5. Performance, Scalability & Availability
6. Security and Permissions
7. Compatibility & Constraints
8. Testing Strategy
9. Open Issues & Risks
10. Related Documents and Tickets

---

## 1. Introduction

#### 1.1 Purpose

Add immutable TurboQuant (TQ) compression profiles to RediSearch's `HNSW` vector index
through the existing `COMPRESSION` argument on `FT.CREATE` (`COMPRESSION TQ8|TQ4|TQ2`)
and the optional `TQ_PROFILE` selector. New TQ schema creation is gated by
`ENABLE_UNSTABLE_FEATURES`, which is off by default. Omitting `TQ_PROFILE` preserves the
existing dense marker-2 `DenseReferenceV1` behavior; the fast profiles are explicit opt-ins.
All profiles retain the paper's payload and asymmetric estimator while swapping immutable,
versioned rotation, QJL, and HNSW construction-score components.

> **API note (v0.8).** TQ is **not exposed as standalone algorithms** `TQ-FLAT` /
> `TQ-HNSW` on `FT.CREATE`. The gated schema shape is `HNSW ... COMPRESSION TQ<bits>`.
> `TQ_PROFILE DenseReferenceV1|FastStructuredRotationV1|FastStructuredV1` optionally selects
> the immutable VecSim backend tuple at index creation; it cannot be changed afterward.
> The TQ flat backend still exists inside VecSim as a direct internal/conformance building
> block, but it is not user-creatable and is not the tiered staging frontend. Tiered TQ-HNSW
> stages raw FP32 vectors in the ordinary brute-force frontend. The internal enums (`VecSimAlgo_TQ`,
> `VecSimAlgo_TQ_HNSW`) and the tiered wrapping are unchanged.

This adds a new family of compression options to RediSearch's existing vector index surface, complementary to the SVS LVQ-based compression and the upcoming Scalar Quantization (SQ) for tiered HNSW described in the sibling HLD [HLD: Scalar Quantization (SQ) Compression for Tiered HNSW](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6153601069/HLD+Scalar+Quantization+SQ+Compression+for+Tiered+HNSW).

#### 1.2 Scope

* **VectorSimilarity**: new internal TQ flat and TQ-HNSW backends, TurboQuant preprocessor, SIMD distance kernels (x86_64 + ARM, including NEON), background-indexing integration for the TQ-HNSW backend.
* **RediSearch**: parsing and validation of `COMPRESSION TQ8|TQ4|TQ2` plus optional `TQ_PROFILE` on the `HNSW` algorithm, RDB/AOF-preamble persistence, `FT.INFO` and `INFO MODULES` reporting, and JSON ingest path coverage for TQ-compressed HNSW fields.

#### 1.3 Out of Scope

* A standalone user-creatable `TQ-FLAT` index. The TQ flat backend remains an internal direct/conformance building block but is not reachable from `FT.CREATE`.
* Disk-backed (Flex / ROF) variants. Disk indexes reject `COMPRESSION` at parse time.
* Data types other than `FLOAT32`. `FLOAT16`, `BFLOAT16`, `INT8`, etc. are reserved for a follow-up.
* Multi-value vectors with TQ compression.
* `L2` distance metric with TQ compression (only `COSINE` and `IP` are supported in this phase).
* User tuning of TQ internals other than the versioned profile (`projections`, `seed`, or individual rotation/QJL/construction components) — deliberately not exposed; see §3.4.
* Run-time conversion of an existing index to/from TQ compression.
* Cluster-mode resharding semantics specific to TQ (uses the same path as other vector indexes).

#### 1.4 Target version

Redis 8.12.

---

## 2. Goals and Objectives

#### 2.1 Primary Goals

* Preserve the reviewed `COMPRESSION TQ8|TQ4|TQ2` schema shape while allowing an immutable profile to be selected at index creation. Omitting `COMPRESSION` still creates ordinary HNSW; omitting `TQ_PROFILE` from a TQ schema selects `DenseReferenceV1` for compatibility.
* Reduce encoded-vector memory at production dimensions without excluding shared model, container, or graph allocations from reported memory.
* Preserve the existing `FT.SEARCH` / `FT.AGGREGATE` / `KNN` / `VECTOR_RANGE` query surface — query vectors remain raw FP32 on the wire; the index handles preprocessing internally.
* Full RDB save/load support so a TQ index survives restart/replication.
* `FT.INFO` reports the `compression` value (`TQ8`/`TQ4`/`TQ2`) on the standard HNSW info block so users and tooling can see what they have.

#### 2.2 Non-Goals

* Changing query semantics or result formatting.
* Exposing individual TQ tuning knobs. `BITS`, `PROJECTIONS`, `SEED`, and `ROTATION` remain absent; the bit budget is encoded in the compression name and component bundles are selected only by a versioned `TQ_PROFILE` (§3.4).
* Sharing model state across independently configured indexes. State is shared only by codec users within one index.

---

## 3. Architecture Overview

#### 3.1 TurboQuant Approach (recap)

Let `d` be the dimension, `b` the advertised total bits per coordinate, `m = d`, `Pi` a random orthogonal matrix, and `S` an `m × d` matrix with i.i.d. `N(0,1)` entries. Algorithm 1 uses the coordinate density

```text
f_d(t) = Gamma(d/2) / (sqrt(pi) Gamma((d-1)/2)) * (1 - t^2)^((d-3)/2)
```

on `[-1,1]` to compute a shared `2^(b-1)`-entry Lloyd-Max codebook. For a unit vector `u`, it stores the nearest-centroid indices for `Pi u`, reconstructs `u_mse = Pi^T c[idx]`, and forms `r = u - u_mse`.

Algorithm 2 stores `gamma = ||r||_2` and, when `gamma != 0`, the `d` signs `q_i = sign(S_i r/gamma)`. Its decoded approximation and asymmetric query estimator are:

```text
u_tilde = u_mse + gamma * sqrt(pi/2) / m * S^T q

IP_hat(y, x) = alpha * (
    <Pi y, c[idx]> + gamma * sqrt(pi/2) / m * <S y, q>
)
```

`alpha` is one for nonzero cosine vectors and `||x||_2` for IP, preserving non-unit source magnitude. Cosine normalizes the query; IP does not. Zero sources use `alpha = gamma = 0`, and zero residuals skip the correction. The correction is `sqrt(pi/2)/m`, not `pi/(2m)`.

Zero encoding is canonical: a zero source writes an all-zero payload. For a nonzero source with an exactly zero residual, `alpha` and the coarse code remain meaningful, `gamma` is zero, used QJL bits encode `sign(0) = +1`, and unused packed tail bits remain zero. The residual signs cannot affect the score when `gamma == 0`.

The paper defines the asymmetric estimator but no compressed-code-to-compressed-code estimator. Query traversal uses the formula above. Dense marker 2 uses `FullDecodeReferenceV1` for HNSW maintenance. Fast markers 3 and 4 use the separately versioned `CoarseMseV1`, which evaluates the exact metric on the two Algorithm 1 coarse reconstructions in O(d), without QJL residual scoring or allocation. Omitting `TQ_PROFILE` still selects dense/FullDecode; neither mode stores a raw-vector sidecar.

For full details see the TurboQuant paper and the VectorSimilarity-side `docs/turboquant-paper-faithful-pivot.md` design.

#### 3.2 System Architecture

Two new VecSim algorithm enums are introduced. Neither is a user-facing algorithm name: the only user-facing surface is `HNSW ... COMPRESSION TQ<bits>`.

| Algorithm enum         | User-facing surface | Index family | Tiered? |
| ---------------------- | ------------------- | ------------ | ------- |
| `VecSimAlgo_TQ`        | none — internal direct/conformance flat backend | exhaustive approximate scan | No      |
| `VecSimAlgo_TQ_HNSW`   | `HNSW` + `COMPRESSION TQ8\|TQ4\|TQ2` | HNSW graph   | Yes (wrapped in `VecSimAlgo_TIERED` from the RediSearch side, identically to how `HNSW` is wrapped) |

##### 3.2.1 TQ flat backend — internal compressed brute-force building block

The TQ flat backend (`VecSimAlgo_TQ`) is a flat (linear scan) index inside VecSim. Each inserted vector is rotated and quantized once at insertion time; queries scan every entry using the TQ distance kernels. Conceptually it is `FLAT` with a different storage and distance kernel.

It is **not user-creatable**: the standalone `TQ-FLAT` index type that earlier drafts of this HLD proposed was dropped per review. The backend remains as an internal direct/conformance component. Tiered TQ-HNSW instead stages raw FP32 vectors in its ordinary brute-force frontend, so the TQ flat backend's properties do not describe staging-buffer storage:

* No background indexing; insert is synchronous.
* Block-based storage; block size is capped internally at 1024 vectors and reduced when required
  by the configured memory limit.
* Single-value vectors only.

##### 3.2.2 TQ-HNSW — compressed graph index behind `COMPRESSION TQ<bits>`

The TQ-HNSW backend (`VecSimAlgo_TQ_HNSW`) reuses the HNSW graph layout (`M`,
`EF_CONSTRUCTION`, `EF_RUNTIME`, `EPSILON`) but stores nodes in TQ-compressed form. Query
traversal uses the paper's asymmetric estimator. HNSW construction is explicitly versioned:
dense marker 2 selects `FullDecodeReferenceV1`; fast markers 3 and 4 select `CoarseMseV1`.
With `ENABLE_UNSTABLE_FEATURES` enabled, `FT.CREATE ... VECTOR HNSW ... COMPRESSION
TQ<bits> [TQ_PROFILE ...]` creates the selected profile.

* Wrapped in a `VecSimAlgo_TIERED` index from the RediSearch side, with `TQ_HNSW` as the primary index — exactly the same wrapping used today for plain `HNSW`. The ordinary raw FP32 brute-force frontend stages new vectors; background jobs compress them when inserting into the TQ-HNSW primary. Queries use the standard tiered frontend/backend merge logic.
* `swapJobThreshold` is set to 0 (default tiered behavior).
* Distance metric is currently restricted to `COSINE` and `IP`. VecSim contains an L2 estimate, but RediSearch rejects `L2` until its recall and range semantics have been validated for release.

##### 3.2.3 Component Design

**VectorSimilarity ([`deps/VectorSimilarity`](https://github.com/jeremyplichta/VectorSimilarity/tree/feat/tq-vector-quantization)) — new and changed:**

* Paper-faithful codec with allocator-backed immutable model state shared by the index's codec users. Every payload/model/rotation/QJL/metric component has a versioned identity.
* Dense reference profile: dense Haar rotation and i.i.d. dense Gaussian QJL, retained for conformance and compatibility rather than production-dimension setup.
* Structured profiles: `FastStructuredRotationV1` uses deterministic three-stage sign/permutation/FWHT rotation; `FastStructuredV1` also uses the separately versioned `CirculantGaussianQjlV1`, whose correlated rows retain distinct statistical and quality gates. Markers 3 and 4 make these explicit opt-ins; the omitted default remains dense marker 2.
* TQ-HNSW backend: asymmetric query scoring with reusable prepared raw-query context, plus versioned `FullDecodeReferenceV1` and `CoarseMse` stored-distance modes.
* SIMD scoring: NEON and SSE2 implementations of the coarse and residual dot products, with scalar packing/lookups/tails and randomized scalar/SIMD parity coverage.
* API hardening (`tq: harden TQ-HNSW factory and reporting APIs`): `static_assert`s on `TQHNSWParams` prefix layout, `AsTQFlatParamsPrefix` helper, tiered-algo virtual dispatch fixes.
* Independent paper-derived C++ test reference; the historical Rust oracle was removed because it repeated the obsolete pairwise-polar equations.

**RediSearch ([`src/`](https://github.com/jeremyplichta/RediSearch/tree/feat/tq-vector-quantization/src)) — new and changed:**

* [`src/spec.c`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/spec.c):
    * New helper `parseVectorField_GetTqCompression`, which parses the `COMPRESSION` value (`TQ2` / `TQ4` / `TQ8`, case-insensitive) into a bit budget.
    * New `COMPRESSION` branch in `parseVectorField_hnsw`: when a TQ compression value is present, the parsed HNSW params are transformed to `VecSimAlgo_TQ_HNSW` / `TQHNSWParams` and validated via the new `parseVectorField_validate_tq_hnsw`.
    * New TQ schema creation requires the existing `ENABLE_UNSTABLE_FEATURES` gate. RDB
      loading of known markers 2/3/4 remains compatible regardless of that run-time flag.
    * The earlier standalone parsers `parseVectorField_tq` / `parseVectorField_tq_hnsw` and the `parseVectorField_GetRotation` helper were deleted — there is no standalone algorithm-name dispatch anymore.
    * `COMPRESSION` is rejected when `isSpecOnDiskForValidation(sp)` is true.
* [`src/vector_index.c`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/vector_index.c) / [`src/vector_index.h`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/vector_index.h):
    * New compression-name constants `VECSIM_TQ_2` / `VECSIM_TQ_4` / `VECSIM_TQ_8` (`"TQ2"` / `"TQ4"` / `"TQ8"`); the former `BITS` / `PROJECTIONS` / `SEED` / `ROTATION` keyword defines were removed.
    * New helper `VecSimTqCompression_ToString(bits)` in `vector_index.c` mapping the stored bit budget back to the compression name for reporting.
    * `VecSim_RdbSave` and `VecSim_RdbLoad_v4` cases for tiered-`TQ_HNSW` only. Encoding version 29 preserves dense-reference marker 2 and its existing field order, while markers 3 and 4 bind the fast tuples in §3.3. Unknown markers and inconsistent fields are rejected before profile-aware model estimation. Version 28 was the incompatible pairwise-polar layout and is rejected.
    * Older RDB versions (v2, v3) explicitly fail when they encounter the new algorithm enum (we don't pretend they were loadable).
    * `getVecSimMetricFromVectorField` extended.
* [`src/info/info_command.c`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/info/info_command.c): a TQ-compressed field renders the plain-HNSW block plus compression, exact payload bytes, fixed seed/projections, and immutable RDB/codec/profile/payload/rotation/QJL/construction/metric identities. These fields report the creation-time selection and are read-only diagnostics.
* [`src/info/global_stats.c`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/info/global_stats.c) / [`src/info/global_stats.h`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/info/global_stats.h): the earlier `numVectorFieldsTqFlat` counter was removed along with the standalone TQ-FLAT index. TQ-compressed HNSW fields are counted in the existing `HNSW` bucket in `INFO MODULES`.
* [`src/json.c`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/json.c): TQ-compressed schemas pass through the JSON ingest path so JSON-backed indexes work with `COMPRESSION TQ<bits>`.

#### 3.3 Data Model Changes

* **New VecSim enums (internal only):**
    * `VecSimAlgo_TQ`
    * `VecSimAlgo_TQ_HNSW`
* **New schema strings:** `VECSIM_TQ_2` (`TQ2`), `VECSIM_TQ_4` (`TQ4`), and `VECSIM_TQ_8` (`TQ8`) are values of `COMPRESSION`; `TQ_PROFILE` accepts the exact versioned profile names in the table below, case-insensitively. No new algorithm-name strings exist, and the former `BITS` / `PROJECTIONS` / `SEED` / `ROTATION` keyword defines remain removed.
* **New profile enum and parameter field:** VecSim exposes `VecSimTqProfile` (`Default=0`, `DenseReferenceV1=1`, `FastStructuredRotationV1=2`, `FastStructuredV1=3`) in the shared `TQFlatParams`/`TQHNSWParams` prefix. VecSim treats zero-initialized callers as dense for compatibility; RediSearch always passes an explicit value.
* **Global stats:** unchanged — TQ-compressed HNSW fields count in the existing `HNSW` bucket (the interim `numVectorFieldsTqFlat` counter was removed).
* **RDB:** RediSearch encoding version 29 uses marker 2 for the existing dense tuple, marker 3 for fast rotation plus dense QJL, and marker 4 for the fully structured tuple. The marker determines the profile without appending or reinterpreting the marker-2 parameter bytes. The incompatible version-28 pairwise-polar layout and unknown/corrupt identities are rejected.

| `TQ_PROFILE` value | RDB marker | rotation | QJL | HNSW construction score |
| ------------------ | ---------- | -------- | --- | ----------------------- |
| `DenseReferenceV1` (omitted default) | 2 | `DenseHaarV1` | `DenseGaussianV1` | `FullDecodeReferenceV1` |
| `FastStructuredRotationV1` | 3 | `FastStructuredRotationV1` | `DenseGaussianV1` | `CoarseMseV1` |
| `FastStructuredV1` | 4 | `FastStructuredRotationV1` | `CirculantGaussianQjlV1` | `CoarseMseV1` |

All three markers retain `PaperV1`, codec version 1, and
`CosineOrInnerProductV1`. A future component change requires another profile name and marker.

#### 3.4 Default Parameter Values

`COMPRESSION` and the optional immutable profile bundle are user-visible. Individual model
parameters remain internal fixed values.

| Parameter        | Value                        | User-exposed? | Notes                                                                |
| ---------------- | ---------------------------- | ------------- | -------------------------------------------------------------------- |
| bits             | from compression name (`TQ8`=8, `TQ4`=4, `TQ2`=2) | Yes, via `COMPRESSION` | Explicit and case-insensitive; no TQ value is implicit.        |
| projections      | `DIM`                         | No (internal) | Algorithm 2 uses one QJL sign bit per dimension.                      |
| seed             | 7                            | No (internal) | Plain integer seed for the random rotation.                          |
| rotation         | `ON`                         | No (internal) | Always on; disabling exists VecSim-side for diagnostics only.        |
| model profile    | `DenseReferenceV1` | Yes, via optional `TQ_PROFILE` | Also accepts `FastStructuredRotationV1` and `FastStructuredV1`; fixed for the index lifetime. |
| construction score | from profile | No (bundled) | Dense uses `FullDecodeReferenceV1`; both fast profiles use `CoarseMseV1`. |
| block size       | up to 1024 vectors per block | No (internal) | Reduced when required by the configured memory limit; `BLOCK_SIZE` / `INITIAL_CAP` are deprecated args and not part of the TQ surface. |
| `M`              | `HNSW_DEFAULT_M` (16)        | Yes (standard HNSW) |                                                                |
| `EF_CONSTRUCTION`| `HNSW_DEFAULT_EF_C` (200)    | Yes (standard HNSW) |                                                                |
| `EF_RUNTIME`     | `HNSW_DEFAULT_EF_RT` (10)    | Yes (standard HNSW) |                                                                |
| `EPSILON`        | `HNSW_DEFAULT_EPSILON` (0.01)| Yes (standard HNSW) |                                                                |

#### 3.5 New and Impacted Flows

* **Index creation:** with `ENABLE_UNSTABLE_FEATURES` enabled, `HNSW` with `COMPRESSION TQ8|TQ4|TQ2` and optional `TQ_PROFILE`. The parser builds the same tiered index as plain `HNSW` but with `VecSimAlgo_TQ_HNSW` as the primary algorithm and an explicit `VecSimTqProfile` in `TQHNSWParams`. VecSim creation and initial-memory estimation dispatch through the same profile. Deprecated `INITIAL_CAP` is normalized to zero before validation. Disk-backed indexes reject `COMPRESSION`.
* **Vector insertion:** raw FP32 vectors land in the ordinary tiered brute-force frontend first, are quantized in a background job, then inserted into the TQ-HNSW backend via the existing tiered job queue — identical to `HNSW` except for compression at primary insertion. WriteInPlace works exactly as for `HNSW`.
* **Vector query:** unchanged user-facing syntax. VecSim prepares the raw FP32 query once per index/query context and reuses it for candidate scoring, including hybrid/ad-hoc paths; the legacy per-candidate fallback remains correctness-safe. The tiered index merges frontend + backend results just like `HNSW`.
* **Vector deletion:** same as `HNSW` (tombstone + repair).
* **RDB save/load:** the tiered-`TQ_HNSW` branch derives marker 2/3/4 from the selected profile and restores the explicit VecSim enum from that marker before validation and estimation. Marker 2's parameter byte layout is unchanged.
* **FT.INFO:** plain-HNSW info block plus compression, immutable model identity, and exact payload bytes. Aggregate/per-field vector memory continues to include allocator-accounted shared model, vector container, and graph allocations.
* **INFO MODULES:** TQ-compressed HNSW fields counted in the existing `HNSW` bucket; no new counters.

---

## 4. Interfaces (APIs)

#### 4.1 Overview

No new commands and no new algorithm names. When `ENABLE_UNSTABLE_FEATURES` is enabled,
`FT.CREATE` accepts `TQ8`/`TQ4`/`TQ2` for the existing `COMPRESSION` argument on `HNSW`.
It also accepts one optional `TQ_PROFILE` token for TQ-compressed fields. `FT.INFO` reports
the compression and exact selected component identity on the standard HNSW info block.

#### 4.2 FT.CREATE — HNSW with COMPRESSION TQ

```redis
FT.CONFIG SET ENABLE_UNSTABLE_FEATURES true
FT.CREATE idx SCHEMA vec VECTOR HNSW 18
  TYPE FLOAT32
  DIM 768
  DISTANCE_METRIC COSINE
  COMPRESSION TQ8
  TQ_PROFILE FastStructuredV1
  M 16 EF_CONSTRUCTION 200 EF_RUNTIME 50 EPSILON 0.01
```

All standard HNSW arguments (`M`, `EF_CONSTRUCTION`, `EF_RUNTIME`, `EPSILON`) remain optional and keep their HNSW defaults.

| Attribute         | Description                                                                 | Default                       |
| ----------------- | --------------------------------------------------------------------------- | ----------------------------- |
| `TYPE`            | Vector component type. **FLOAT32 only** with TQ compression in this phase.  | (mandatory)                   |
| `DIM`             | Vector dimensionality, `DIM >= 2`; odd dimensions are supported.            | (mandatory)                   |
| `DISTANCE_METRIC` | `COSINE` or `IP` with TQ compression (`L2` rejected).                       | (mandatory)                   |
| `COMPRESSION`     | Explicit `TQ8`, `TQ4`, or `TQ2`. The bit budget is encoded in the name, like `LVQ4`/`LVQ8` for SVS. Case-insensitive. | (optional — omit for plain HNSW) |
| `TQ_PROFILE`      | `DenseReferenceV1`, `FastStructuredRotationV1`, or `FastStructuredV1`, case-insensitive. Valid only with TQ compression and immutable after creation. | `DenseReferenceV1` |
| `M` / `EF_CONSTRUCTION` / `EF_RUNTIME` / `EPSILON` | As `HNSW`.                                 | HNSW defaults                 |

The former individual TQ knobs `BITS`, `PROJECTIONS`, `SEED`, and `ROTATION` are **not
user-exposed**. A profile selects only one reviewed, versioned tuple; profiles cannot be
assembled piecemeal or changed after creation. Every profile fixes projections = `DIM`, seed = 7,
and rotation = ON. `BLOCK_SIZE` / `INITIAL_CAP` are deprecated args and are not part of the TQ
surface (block size is capped at 1024 vectors and may be reduced to honor the configured memory
limit).

**Validation errors (parse-time):**

* TQ creation while `ENABLE_UNSTABLE_FEATURES` is false → a deterministic unstable-feature
  error. This does not disable loading a known marker-2 RDB.
* `TYPE` other than `FLOAT32` → `"TQ compression only supports FLOAT32 vectors"`.
* `DIM < 2` → `"TQ compression requires vector dimension >= 2"`; odd dimensions are valid.
* Overflow in raw/payload/model size arithmetic → a deterministic supported-size error before model allocation.
* `DISTANCE_METRIC L2` → `"TQ compression with DISTANCE_METRIC L2 is not yet supported; use COSINE or IP"`.
* Multi-value vector (JSON `$.vecs[*]`) → `"TQ compression does not support multi-value vectors"`.
* `COMPRESSION` on a disk-backed index → `"Disk index does not support COMPRESSION"`.
* Unknown `COMPRESSION` value on `HNSW` → standard bad-argument error mentioning `COMPRESSION`.
* Unknown or missing `TQ_PROFILE` value → standard bad-argument error mentioning `TQ_PROFILE`.
* Duplicate `TQ_PROFILE` → `"Duplicate TQ_PROFILE parameter"`.
* `TQ_PROFILE` without `COMPRESSION TQ2|TQ4|TQ8` → `"TQ_PROFILE requires COMPRESSION TQ2, TQ4, or TQ8"`.
* Missing `TYPE` / `DIM` / `DISTANCE_METRIC` → standard "mandatory argument" error.

**Internal mapping.** When `COMPRESSION TQ<bits>` is present, the parser builds the same tiered index as plain `HNSW` but with `VecSimAlgo_TQ_HNSW` as the primary algorithm and `TQHNSWParams`: type, dim, metric, multi, `M`, `efConstruction`, `efRuntime`, `epsilon` carried over from the parsed HNSW args (`epsilon` defaults to `HNSW_DEFAULT_EPSILON`); bits from the compression name; an explicit profile enum from `TQ_PROFILE` or the dense omitted default; and projections/seed/rotation fixed internal values. Background indexing via tiered worker jobs is identical to `HNSW`.

#### 4.3 FT.SEARCH / FT.AGGREGATE / FT.HYBRID

Unchanged surface:

```redis
FT.SEARCH idx "*=>[KNN 10 @vec $blob AS dist]" PARAMS 2 blob <raw-f32-bytes> SORTBY dist DIALECT 2
FT.SEARCH idx "@vec:[VECTOR_RANGE 0.2 $blob]=>{$yield_distance_as: dist}" PARAMS 2 blob <raw-f32-bytes> SORTBY dist DIALECT 2
```

The query vector is raw FP32 of length `DIM * sizeof(float)`. The index applies its own preprocessing internally. Cosine fields normalize both sides and report `1 - estimated_inner_product`, so yielded distances and `VECTOR_RANGE` radii remain on the standard approximate cosine-distance scale.

#### 4.4 FT.INFO

A TQ-compressed field reports the plain-HNSW info block plus a `compression` line — the same shape SVS uses for its compressed variants. The algorithm is reported as `HNSW` (not `TQ-HNSW`):

```text
algorithm: HNSW
data_type: FLOAT32
dim: 768
distance_metric: COSINE
M: 16
ef_construction: 200
ef_runtime: 50
compression: TQ8
tq_rdb_marker: 2
tq_codec_version: 1
tq_profile: DenseReferenceV1
tq_payload_layout: PaperV1
tq_rotation: DenseHaarV1
tq_qjl: DenseGaussianV1
tq_construction_score: FullDecodeReferenceV1
tq_metric_contract: CosineOrInnerProductV1
tq_projections: 768
tq_seed: 7
tq_payload_bytes: 776
```

`compression` is one of `TQ8` / `TQ4` / `TQ2`. The `tq_*` fields expose the selected
immutable profile and exact encoded bytes; they cannot be mutated after creation. Markers 3 and 4
report the exact component strings in §3.3. `epsilon` remains omitted, matching the existing HNSW
block. Overall and per-field vector memory includes allocator-accounted shared model, container,
and graph memory; it must not be inferred from `tq_payload_bytes * num_vectors`.

#### 4.5 INFO MODULES

No changes. TQ-compressed HNSW fields are counted in the existing `HNSW` bucket of `search_fields_vector`; the earlier `TQ_FLAT` counter was removed along with the standalone index. A dedicated compressed-HNSW counter is a possible follow-up (see §9 Risks).

---

## 5. Performance, Scalability & Availability

#### 5.1 Memory

* For total width `b` and dimension `d`, stored-vector bytes are `ceil((b - 1)d / 8) + ceil(d / 8) + 8`: Lloyd-Max indices, QJL signs, FP32 source scale, and FP32 residual norm.
* At dimension 768 this is 200 bytes for TQ2, 392 bytes for TQ4, and 776 bytes for TQ8, versus 3,072 bytes for FP32 before allocator and graph metadata: approximately 93.5%, 87.2%, and 74.7% raw-vector savings. At dimension 1024 the exact payloads are 264, 520, and 1,032 bytes.
* The packed bitstreams are exactly 2, 4, and 8 bits/dimension. The eight metadata bytes are reported separately and amortize with dimension.
* Graph overhead (links, levels) is unchanged from `HNSW`.
* VecSim's initial allocation estimate and reported memory include encoded containers,
  graph/link/label data, and allocator-backed shared model state. Per-operation and per-query
  scratch are excluded from the initial estimate and are accounted only while the reusable
  context that owns them is alive. `tq_payload_bytes` reports only the exact encoded payload so
  those costs are not conflated.

#### 5.2 Throughput

* Asymmetric distance reads packed centroid indices and sign bits, then evaluates two candidate/query dot products. The current verified SIMD paths use NEON on ARM64 and SSE2 on x86, with scalar codebook lookup, packing, and tails.
* Marker 2 uses auditable dense Gaussian QR rotation and a dense Gaussian QJL matrix. Allocator-backed shared state makes this memory visible, but its O(d^2) memory and O(d^3) setup keep it a reference profile at production dimensions.
* Marker 3 replaces the dense Haar rotation with `FastStructuredRotationV1` while retaining dense Gaussian QJL state. Marker 4 also replaces QJL with `CirculantGaussianQjlV1`, reducing persistent model state to O(d) (including FFT workspace state) and transform work to O(d log d). VecSim's initial-size estimator dispatches on the selected profile and includes only its actual component allocations.
* Marker 2 uses `FullDecodeReferenceV1` for HNSW stored-to-stored maintenance. Markers 3 and 4 use allocation-free O(d) `CoarseMseV1`, which evaluates exact Algorithm 1 coarse geometry.

#### 5.3 Recall

* No bit width or structured profile is described as near-lossless without fixed-corpus, multi-seed HNSW construction/search evidence.
* TQ8 is the first rollout candidate; TQ4 and TQ2 require independent quality gates rather than inheriting TQ8 approval.
* Algorithm 2 fixes the QJL sketch at one sign per coordinate (`projections = DIM`); it does not reduce the full-dimensional rotation.

#### 5.4 Concurrency / Threading

* The internal TQ flat backend follows the same thread model as `FLAT`: writes go through the main thread; queries are read-mostly under the existing index lock.
* TQ-compressed HNSW follows the same thread model as `HNSW` via the tiered wrapper: frontend buffer + background worker jobs + thpool's existing query-priority handling. The "enable tiered tq hnsw worker indexing" change wires the TQ-HNSW backend through the same path as `HNSW` (see §3.2.2).

#### 5.5 Availability

Replication uses the standard RDB path. RediSearch requires Redis' AOF RDB preamble and disables
module command rewriting, so AOF restart also restores profile identity from the embedded marker
rather than regenerating `FT.CREATE` text. The VecSim v4 loader maps known markers 2/3/4 to an
explicit profile before validation and estimation, even when `ENABLE_UNSTABLE_FEATURES` is false.
It also validates the exact source-vector blob size and all model-affecting fields before index
construction. The incompatible version-28 pairwise-polar layout and unknown markers are rejected;
older non-TQ files remain loadable.

#### 5.6 Measured Benchmarks

> **Historical-only:** these runs predate both the current paper payload and the versioned
> dense/structured model split. The old pairwise-polar numbers and 4/8/16-bit lane names are
> not evidence for TQ2/TQ4/TQ8, `CoarseMse`, or any structured profile. They are retained only
> as project history and must not be used for release claims.

The numbers below are from the local benchmarking harness at `~/git/redis-turboquant-bench`, run against the patched RediSearch + VecSim from this branch. Two sets of results are included: (a) **single-shard local** runs that establish the quality / latency / memory shape, and (b) **multi-node Redis Enterprise** runs (`4 × e2-standard-16`, `all-master-shards`, sparse placement) that establish the throughput shape.

> **Benchmark sign-off gap.** These historical results include raw SVS-VAMANA only.
> They do not compare TQ against compressed SVS-VAMANA (`LVQ8`, `LVQ4`, or LeanVec)
> on the same corpus and hardware, as requested in review. That apples-to-apples matrix
> must be run on the final branch before approval; no conclusion about TQ versus
> compressed SVS should be drawn from the tables below.

**Dataset.** [`CohereLabs/msmarco-v2.1-embed-english-v3`](https://huggingface.co/datasets/CohereLabs/msmarco-v2.1-embed-english-v3) on Hugging Face — MS MARCO v2.1 passages with **pre-computed Cohere `embed-english-v3.0` embeddings** (FLOAT32, 1024-dim). Using the pre-embedded variant means the benchmark measures pure indexing / retrieval cost rather than embedding-generation cost, and every algorithm sees the exact same vector for the same passage. Prepared corpus: ~253K passages assembled from locality-aware contiguous blocks to keep labeled coverage intact while sitting close to the ~1 GiB raw-FLAT memory target. 214 evaluated annotated queries.

**F1@10 ceiling note.** MS MARCO labels only 2–3 relevant passages per query in this corpus, so precision@10 is capped by construction and `F1@10` sits in the low 0.3s even for ground-truth `FLAT`. `Recall@10` is the more informative quality metric for comparing algorithms — for the raw `FLAT` baseline it lands at `0.935`, which confirms the ground truth and the methodology are sound.

##### 5.6.1 Quality & memory (single-shard local, MS MARCO ~253K passages)

| Run                         | Algorithm      | Precision@10 | Recall@10 | F1@10  | Mean ms | P95 ms | Vector index MB | Indexing s |
| --------------------------- | -------------- | -----------: | --------: | -----: | ------: | -----: | --------------: | ---------: |
| `raw-flat`                  | `FLAT`         | 0.258        | **0.935** | 0.359  | 90.28   | 102.58 | 1003.7          | 313.9      |
| `raw-hnsw`                  | `HNSW`         | 0.254        | 0.904     | 0.351  |  42.34  |  44.03 | 1078.1          | 270.6      |
| `raw-svs-vamana`            | `SVS-VAMANA`   | 0.255        | 0.911     | 0.353  |  45.35  |  51.48 | 1024.0          | 294.3      |
| `tq-flat-4bit`              | `TQ-FLAT`      | 0.221        | 0.812     | 0.308  | 203.84  | 229.38 | **587.2**       | 257.4      |
| `tq-flat-8bit`              | `TQ-FLAT`      | 0.196        | 0.734     | 0.276  | 220.81  | 410.38 | 649.2           | 276.5      |
| `tq-flat-16bit`             | `TQ-FLAT`      | 0.192        | 0.729     | 0.271  | 225.14  | 244.60 | 773.2           | 284.4      |
| `tq-hnsw-8bit` (defaults)   | `TQ-HNSW`      | 0.234        | 0.853     | 0.325  |  51.48  |  81.75 | 724.6           | 586.3      |
| `tq-hnsw-8bit-ef200`        | `TQ-HNSW`      | 0.236        | 0.852     | 0.327  |   4.11  |   4.99 | 728.2           | 886.9      |
| `tq-hnsw-8bit-m32-ef200`    | `TQ-HNSW`      | 0.237        | 0.865     | 0.331  |   5.86  |   7.13 | 760.0           | 1948.6     |
| **`tq-hnsw-8bit-m32-ef300`**| **`TQ-HNSW`**  | **0.239**    | **0.880** | **0.333** | **5.12** | **6.58** | **763.1** | 319.1 |
| `tq-hnsw-8bit-m32-ef400`    | `TQ-HNSW`      | 0.239        | 0.879     | 0.333  |   6.09  |   8.63 | 760.0           | 1087.4     |
| `tq-hnsw-16bit-m32-ef200`   | `TQ-HNSW`      | 0.214        | 0.787     | 0.299  |   7.57  |   9.95 | 882.1           | 2778.4     |

Headline takeaways from this matrix:

* **`tq-hnsw-8bit-m32-ef300` is the best-balanced lane.** Within ~5% of raw HNSW's recall@10, an order of magnitude better mean latency than raw HNSW (5.1 ms vs 42.3 ms — note the raw `HNSW` lane here was not running with `WORKERS`, so this comparison shows the codec helping latency, not raw-vs-tiered), and ~29% smaller vector index (763 MB vs 1078 MB).
* **`tq-flat-4bit` is the smallest index by far** at 587 MB — ~42% smaller than raw `FLAT` — at a recall@10 cost of ~13 points. It is still a full scan, so latency stays well above ANN.
* **`tq-hnsw-16bit` did not pay off** in this matrix: it cost more memory than 8-bit and recall got worse. The 4-bit `TQ-HNSW` lane was not yet stable enough to include here.
* **TQ-HNSW build cost is real.** Indexing time for the best lane is comparable to raw `HNSW` (with concurrent `WORKERS` enabled), but smaller `EF_CONSTRUCTION` / `M` settings without WORKERS can be many times slower. See §5.6.3.

##### 5.6.2 Throughput (Redis Enterprise, 4 × e2-standard-16, all-master-shards, `WORKERS` via DB-level QPF)

Same corpus and queries, run against the corrected multi-node setup with explicit client-side endpoint fanout. `QPF off` = `query_performance_factor.active=false`; `QPF 4x` = `scaling_factor=4`.

| Run                | QPF off QPS | QPF 4x QPS | QPF off P95 | QPF 4x P95 | Build (QPF off) | Build (QPF 4x) |
| ------------------ | ----------: | ---------: | ----------: | ---------: | --------------: | -------------: |
| `raw-flat`         |     148.56  |    156.98  | 132.31 ms   | 124.02 ms  |        7.08 s   |         8.08 s |
| `raw-hnsw`         |     890.84  |    951.41  |  36.74 ms   |  35.65 ms  |       15.74 s   |        15.01 s |
| `raw-svs-vamana`   |     961.07  |    926.13  |  34.65 ms   |  33.84 ms  |       45.93 s   |        35.82 s |
| `tq-flat-4bit`     |      78.69  |     78.18  | 224.01 ms   | 226.31 ms  |       58.27 s   |        55.92 s |
| `tq-flat-8bit`     |      75.82  |     70.23  | 252.84 ms   | 305.11 ms  |          —      |          56.21 s |
| `tq-flat-16bit`    |      64.82  |     54.92  | 285.98 ms   | 370.10 ms  |          —      |          62.62 s |
| `tq-hnsw-8bit`     |     931.54  |    942.03  |  33.85 ms   |  28.72 ms  |      119.49 s   |       145.86 s |

Headline takeaways from the cluster runs:

* **`tq-hnsw-8bit` matches `raw-hnsw` QPS** under both QPF settings (942 vs 951 QPS at QPF 4x) and *beats* it on P95 (28.7 ms vs 35.7 ms). On this cluster shape, swapping `HNSW` for `TQ-HNSW` is approximately a free trade on read-side throughput in exchange for the memory savings.
* **TQ-FLAT lanes do not benefit from QPF** at all and several regress. This is consistent with TQ-FLAT being a true full scan: more concurrent workers do not get you fewer comparisons per query. We should not recommend QPF as a default for TQ-FLAT.
* **TQ-HNSW build cost is the trade-off you pay.** The `tq-hnsw-8bit` build was 119 s vs 16 s for raw HNSW on the same shape — about 7× — even at QPF 4x. The benchmark was a bulk re-ingest; steady-state writes are much closer (see §5.6.3).

##### 5.6.3 RediSearch `WORKERS` benefit on TQ-HNSW (single-shard local)

Same `tq-hnsw-8bit-m32-ef300` lane, with `WORKERS=1` vs `WORKERS=4` to validate that the tiered background-indexing path actually engages for `TQ-HNSW`:

| Metric                                | WORKERS=1 | WORKERS=4 | Delta              |
| ------------------------------------- | --------: | --------: | -----------------: |
| Single-query mean latency (ms)        |     5.84  |     5.84  |  +0.0%             |
| Single-query P95 latency (ms)         |     8.93  |     8.83  |  −1.1%             |
| F1@10                                 |    0.333  |    0.332  |  −0.3% (noise)     |
| Concurrent read throughput (QPS)      |   275.8   |   601.5   | **+118.1%**        |
| Concurrent read P95 (ms)              |    17.66  |     8.68  | **−50.9%**         |
| Concurrent write enqueue (ops/s)      |   631.0   |   876.5   | **+38.9%**         |
| Concurrent write fully-indexed (ops/s)|   559.9   |   839.6   | **+50.0%**         |
| Concurrent write P95 (ms)             |     9.40  |     5.35  |  −43.1%            |
| Bulk indexing time (s)                |   624.4   |   309.6   | **−50.4%**         |
| Vector index size (MB)                |   760.6   |   762.1   |  +0.2% (noise)     |

This confirms that the tier wrapping for `TQ-HNSW` correctly engages the background indexing path, that bulk ingest scales roughly linearly with `WORKERS`, and that read concurrency benefits substantially from moving query work off the main thread.

##### 5.6.4 Caveats on these numbers

* The MS MARCO sample is locality-aware (contiguous 1024-row blocks) rather than uniformly random across the full ~113M-passage corpus. Absolute recall numbers will move when re-run at full scale; **relative ordering between algorithms is what these benchmarks are good for**, not absolute recall.
* `F1@10 ≈ 0.36` is the corpus ceiling, not an algorithm ceiling. Recall@10 is the metric to trust for quality comparisons.
* The single-shard local runs were on Apple Silicon (NEON) with the `WORKERS`-aware writer paths; the Enterprise runs were on `e2-standard-16` GCP nodes (AVX2 + FMA, AVX-512 not used).
* The Enterprise `raw-hnsw` lane there was *also* using tiered HNSW with workers — it is not a "single-threaded raw HNSW" baseline. Treat that table as "raw vs TQ at the same cluster shape," not "raw single-threaded vs TQ tiered."
* These numbers are from `~/git/redis-turboquant-bench` notes `benchmark-findings.md` (single-shard, 2026-04-15) and `jp-tq-bench-enterprise-report-2026-04-22.md` (cluster, 2026-04-22), and from `results/latest/comparison.md` + `workers-comparison.md`. They should be re-run on the final branch and re-pinned in this doc before sign-off.
* Several lanes use the pre-v0.4 standalone prototype names and bit settings that are no longer user-creatable. Treat them as historical implementation evidence, not a benchmark of the final public API.

---

## 6. Security and Permissions

No new security considerations. TQ-compressed indexes use the same ACL surface and the same data-path as existing vector indexes. The internal model seed is a plain integer used by deterministic transform generation; it is not a cryptographic seed and is not security-relevant.

---

## 7. Compatibility & Constraints

* **No standalone TQ-FLAT:** `TQ-FLAT` is no longer a creatable index type. The only way to get TQ on a vector field is `HNSW ... COMPRESSION TQ<bits>`. The TQ flat backend remains internal to VecSim.
* **Backward compatibility:** Existing indexes without TQ are unaffected. RediSearch encoding version 29 plus marker 2 continues to identify the exact dense component tuple and remains loadable independently of the unstable-feature creation gate. New markers 3 and 4 identify different component tuples without changing marker 2's bytes or meaning. Version-28 pairwise-polar TQ files, unknown markers, mismatched raw-vector blob sizes, and inconsistent model fields are rejected; new builds still load older non-TQ files.
* **Profile immutability:** `TQ_PROFILE` is accepted only by `FT.CREATE` together with TQ compression. There is no `FT.ALTER` or runtime switch for a field's profile.
* **Data types:** `FLOAT32` only with TQ compression in this phase. Adding `FLOAT16` (and integer types) requires new VecSim SIMD kernel variants — explicitly out of scope.
* **Distance metrics:** `COSINE`, `IP` only with TQ compression. `L2` returns a parse error.
* **Disk (Flex / ROF):** unsupported. `COMPRESSION` is rejected when `isSpecOnDiskForValidation(sp) == true`.
* **Multi-value:** unsupported with TQ compression. JSON `$.vecs[*]` is rejected at parse time.
* **Dimensions:** TQ requires `DIM >= 2`; odd dimensions are supported and packing tests cover byte tails.
* **VecSim version:** requires the reviewed production-story commit range. The RediSearch submodule pointer is pinned only after the final accepted VecSim SHA is known.
* **Cluster mode:** TQ-compressed HNSW is a local-shard index type and uses the existing coordinator and resharding paths. No coordinator changes are required.

---

## 8. Testing Strategy

| Level                | Type      | Description                                                                                                                |
| -------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------- |
| Unit (VecSim)        | Automated | Independent paper-derived reference; exact-density Lloyd-Max, packing/byte budgets, QJL bias and residual scaling, TQ2/TQ4/TQ8 scalar/SIMD parity, IP/cosine, and FullDecode/CoarseMse HNSW maintenance. |
| Unit (RediSearch)    | Automated | Parser coverage for the omitted default, all three exact profile names, case-insensitivity, unknown/missing/duplicate/irrelevant profiles, plus existing compression/type/dimension/metric/multi/disk errors. |
| Component tests      | Automated | Profile-aware estimation, marker 2/3/4 mapping, `FT.INFO` component identity, RDB/AOF-preamble score/config round trips, and corrupt/unknown loaded identity rejection. |
| E2E functional       | Automated | KNN/range/hybrid correctness, HASH/JSON update/delete/reindex, tiered foreground/background transitions, repair, and GC. |
| Recall / quality     | Automated | Recall@K vs FP32 `FLAT` ground truth across at least one open embedding dataset, for TQ-compressed HNSW with default params. |
| Micro benchmarks     | Automated | `add_label`, `TopK`, `Range` benchmarks vs `FLAT` / `HNSW` for memory, throughput, and latency.                            |
| SIMD path coverage   | Automated | VecSim NEON / SSE2 / scalar scorer parity across bit widths, randomized inputs, tails, and unaligned storage.              |
| Persistence          | Partial   | Automated RDB round-trip of markers 2/3/4 and AOF RDB-preamble restart for every profile; mixed-algorithm persistence and replica failover remain required. |
| Enterprise sanity    | Planned   | Rolling upgrade (pre-TQ → post-TQ build and vice versa where applicable), resharding, replica failover, and backup/restore remain pre-merge requirements. |

Existing test artifacts on the branch:

* [`tests/pytests/test_tq.py`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/tests/pytests/test_tq.py) — covers `FT.CREATE` with `COMPRESSION TQ2`/`TQ4`/`TQ8`, all three profiles and the omitted default, HASH/JSON lifecycle, immutable `FT.INFO` identity and payload bytes, cosine KNN/range, non-unit IP, hybrid frontend/backend scoring, tiered repair/GC, exact score/config RDB round trips, odd dimensions, and negative cases. Flex-mode disk rejection is covered in `tests/pytests/test_flex_validation.py`.

Test gaps to close before merge (see §9):

* End-to-end recall test for TQ-compressed HNSW on a real dataset.
* Final-branch comparison against compressed SVS-VAMANA (`LVQ8`, `LVQ4`, and applicable LeanVec variants) on the same corpus and hardware.
* Replica-failover persistence coverage for all accepted TQ profile markers.
* Mixed-algorithm persistence coverage with TQ fields alongside FLAT, HNSW, and SVS fields.
* Enterprise rolling-upgrade/downgrade, resharding, and backup/restore validation.

---

## 9. Open Issues & Risks

| ID     | Description                                                                                                                                                                          | Impact                                                                                  | Mitigation                                                                                                                                                                                |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Risk 1 | **No `L2` support for TQ compression yet.** The decoded stored-to-stored path can evaluate L2, but an asymmetric L2 product contract, recall, and range behavior have not been validated for the public surface. | Users with L2 embeddings cannot use `COMPRESSION TQ<bits>`. | Reject at parser with a user-readable error pointing at COSINE/IP. Follow-up: enable L2 only after end-to-end validation. |
| Risk 2 | **The omitted profile remains marker 2.** Dense Gaussian QR, dense QJL state, and FullDecode maintenance match the reviewed equations but are too expensive at production dimensions. | Users who omit `TQ_PROFILE` may see unacceptable setup cost and O(d^2) model memory. | Retain marker 2 for compatibility/conformance, expose markers 3/4 as explicit immutable alternatives, document their exact identities, and never reinterpret marker 2. |
| Risk 3 | **No data type other than `FLOAT32`.** Many of our users send `FLOAT16` / `BFLOAT16` embeddings.                                                                                       | Coverage gap relative to `FLAT` / `HNSW`.                                               | Explicitly call out in docs; track follow-up to add FP16 SIMD kernel variants (mirrors the SQ HLD §7 SIMD matrix).                                                                        |
| Risk 4 | **No disk-backed variant.** Disk-backed RediSearch (Flex / ROF) cannot use `COMPRESSION` / TQ.                                                                                          | Tiered-memory deployments cannot benefit from TQ in this phase.                         | Reject at parser; design integration with `vecsim_disk` as a follow-up HLD.                                                                                                               |
| Risk 5 | **No dedicated compressed-HNSW aggregate counter.** TQ-compressed fields remain in the existing `HNSW` bucket of `INFO MODULES`. | Operators cannot count compressed fields from `INFO MODULES` alone. | `FT.INFO` exposes per-field compression, exact payload bytes, and immutable component identity; a dedicated aggregate counter remains a possible follow-up. |
| Risk 6 | **VecSim API surface widened.** New factory paths, virtual dispatch in tiered algos, `static_assert`s on TQ param prefixes, and a profile enum in the shared prefix can affect other consumers. | Build break or accidental profile changes in `vecsim_disk` / micro-benchmarks. | Enum value 0 resolves to dense for zero-initialized callers; RediSearch always passes an explicit value; unknown values fail creation and estimation. Coordinate the VecSim bump with consumers. |
| Risk 7 | **Final benchmark comparison is incomplete.** Existing results compare TQ with FP32 FLAT/HNSW and raw SVS-VAMANA, but not compressed SVS-VAMANA. | We could recommend a new codec without showing whether it improves on existing LVQ/LeanVec choices. | Block approval on a same-corpus, same-hardware recall/latency/memory/build-time matrix against `LVQ8`, `LVQ4`, and applicable LeanVec variants; pin results in this HLD. |
| Risk 8 | **Branch hygiene.** The historical `feat/tq-vector-quantization` branch accumulated unrelated drift while the paper-faithful work was evolving. | Hard to review TQ in isolation. | The production follow-up is checkpointed locally on `codex/tq-production-20260824` with explicit RediSearch commits and a separately reviewable VecSim submodule bump. No branch is pushed until the rollout evidence is accepted. |

#### 9.1 Rollout gates

* **Stage 0 — reference development:** marker 2 remains the known dense omitted default and can be
  loaded for compatibility. All new TQ schemas remain rejected unless
  `ENABLE_UNSTABLE_FEATURES` is explicitly enabled.
* **Stage 1 — selectable immutable profiles:** markers 3 and 4 are assigned to the exact fast
  tuples in §3.3 and can be selected only with `TQ_PROFILE`. Collect same-corpus build latency,
  query latency, memory, recall, fallback-context, and load-failure diagnostics for each profile;
  exposure under the unstable gate is not production endorsement.
* **Stage 2 — TQ4:** repeat all quality, error, storage, construction, and persistence gates; do not
  inherit TQ8 approval.
* **Stage 3 — experimental TQ2:** require multi-seed production-dimension graph construction and
  search recall. Quantizer-only error is not sufficient because construction changes graph topology.

Rollback never changes the meaning of a marker. Unknown/unavailable profiles fail explicitly and
are never silently mapped to dense. Changing the omitted default would require a separately
reviewed compatibility decision; this design keeps it `DenseReferenceV1`.

---

## 10. Related Documents and Tickets

| Type                       | Link                                                                                                                                                                                | Owner          |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| Reference HLD (SQ)         | [HLD: Scalar Quantization (SQ) Compression for Tiered HNSW](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6153601069/HLD+Scalar+Quantization+SQ+Compression+for+Tiered+HNSW) | Dor Forer      |
| Primary algorithm source   | [TurboQuant: Online Vector Quantization with Near-optimal Distortion Rate](https://arxiv.org/abs/2504.19874) | n/a |
| External background        | [Google Research blog — TurboQuant: redefining AI efficiency with extreme compression](https://research.google/blog/turboquant-redefining-ai-efficiency-with-extreme-compression/) | n/a            |
| Local design notes         | [docs/design/tq_flat.md](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/docs/design/tq_flat.md), [docs/design/tq_hnsw.md](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/docs/design/tq_hnsw.md) | Jeremy Plichta |
| Benchmark dataset          | [CohereLabs/msmarco-v2.1-embed-english-v3](https://huggingface.co/datasets/CohereLabs/msmarco-v2.1-embed-english-v3) — MS MARCO v2.1 passages with pre-computed Cohere `embed-english-v3.0` 1024-dim embeddings | n/a |
| RediSearch review branch   | `codex/tq-production-20260824` (local; not pushed)                                                                                                                          | Jeremy Plichta |
| VecSim production branch   | `codex/tq-production-20260824`                                                                                                                                             | Jeremy Plichta |
| Benchmark harness          | `~/git/redis-turboquant-bench` (local; MS MARCO v2.1 corpus prep + comparison harness used for §5.6)                                                                                 | Jeremy Plichta |
| Tickets                    | TBD — open MOD-* tickets per section of work once this HLD is reviewed (parser + validation, RDB, FT.INFO + INFO MODULES, JSON ingest, VecSim TQ-FLAT, VecSim TQ-HNSW, SIMD kernels, benchmarks, persistence tests, enterprise sanity). |                |
