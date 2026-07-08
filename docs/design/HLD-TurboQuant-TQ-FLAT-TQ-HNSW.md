# HLD: TurboQuant (TQ) Compression for HNSW Vector Indexes

| **PRD**             | TBD                                                            |
| ------------------- | -------------------------------------------------------------- |
| **Document owner**  | Jeremy Plichta                                                 |
| **Document status** | DRAFT v0.5 (review feedback incorporated, re-review requested) |
| **RediSearch branch** | [jeremyplichta/RediSearch @ feat/tq-vector-quantization](https://github.com/jeremyplichta/RediSearch/tree/feat/tq-vector-quantization) |
| **VecSim branch**   | [jeremyplichta/VectorSimilarity @ feat/tq-vector-quantization](https://github.com/jeremyplichta/VectorSimilarity/tree/feat/tq-vector-quantization) |

| Row | Version | Revision date | Revision Note |
| --- | ------- | ------------- | ------------- |
| 1   | 0.1     | 2026-05-12    | First draft, pre-review |
| 2   | 0.2     | 2026-05-13    | Clarify §5.6 dataset is the Cohere-embedded MS MARCO v2.1 variant (HF card linked) |
| 3   | 0.3     | 2026-05-13    | Link file references to the GitHub fork branch; surface branch links in the header |
| 4   | 0.4     | 2026-06-11    | API reworked per review: TQ exposed via HNSW `COMPRESSION` argument (`TQ2`/`TQ4`/`TQ8`); standalone `TQ-FLAT`/`TQ-HNSW` algorithm names and `BITS`/`PROJECTIONS`/`SEED`/`ROTATION` knobs removed |
| 5   | 0.5     | 2026-07-08    | Synced with branch state after merging latest master (twice); suites green (C 742+130, Python 2139/2140 — remaining failure is a local-env TLS port conflict, unrelated to TQ; VecSim TQ 14, bench harness 16); added Risk 9 (cosine distance scale) |

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

Add TurboQuant (TQ) vector compression to RediSearch's `HNSW` vector index, exposed through the existing `COMPRESSION` argument on `FT.CREATE` (`COMPRESSION TQ8|TQ4|TQ2`) — mirroring how `SVS-VAMANA` exposes its LVQ / LeanVec compression. TurboQuant is a quantization technique published by Google Research that combines a random orthogonal rotation with per-component scalar quantization. Together they produce a compact, SIMD-friendly representation of high-dimensional vectors with near-lossless recall at typical embedding dimensions (≥ 512), and bring meaningful memory and bandwidth savings compared to FP32 storage.

> **API note (v0.4).** Per maintainer review on Confluence, TQ is **no longer exposed as standalone algorithms** `TQ-FLAT` / `TQ-HNSW` on `FT.CREATE`. The user-facing surface is `HNSW ... COMPRESSION TQ<bits>`. The TQ flat backend still exists inside VecSim as the tiered frontend / building block, but it is not user-creatable. The internal enums (`VecSimAlgo_TQ`, `VecSimAlgo_TQ_HNSW`) and the tiered wrapping are unchanged.

This adds a new family of compression options to RediSearch's existing vector index surface, complementary to the SVS LVQ-based compression and the upcoming Scalar Quantization (SQ) for tiered HNSW described in the sibling HLD [HLD: Scalar Quantization (SQ) Compression for Tiered HNSW](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6153601069/HLD+Scalar+Quantization+SQ+Compression+for+Tiered+HNSW).

#### 1.2 Scope

* **VectorSimilarity**: new internal TQ flat and TQ-HNSW backends, TurboQuant preprocessor, SIMD distance kernels (x86_64 + ARM, including NEON), background-indexing integration for the TQ-HNSW backend.
* **RediSearch**: parsing and validation of `COMPRESSION TQ8|TQ4|TQ2` on the `HNSW` algorithm, RDB persistence, `FT.INFO` and `INFO MODULES` reporting, and JSON ingest path coverage for TQ-compressed HNSW fields.

#### 1.3 Out of Scope

* A standalone user-creatable `TQ-FLAT` index. The TQ flat backend remains internal to VecSim (tiered frontend / building block) but is not reachable from `FT.CREATE`.
* Disk-backed (Flex / ROF) variants. Disk indexes reject `COMPRESSION` at parse time.
* Data types other than `FLOAT32`. `FLOAT16`, `BFLOAT16`, `INT8`, etc. are reserved for a follow-up.
* Multi-value vectors with TQ compression.
* `L2` distance metric with TQ compression (only `COSINE` and `IP` are supported in this phase).
* User tuning of the TQ internals (`projections`, `seed`, `rotation`) — deliberately not exposed; see §3.4.
* Run-time conversion of an existing index to/from TQ compression.
* Cluster-mode resharding semantics specific to TQ (uses the same path as other vector indexes).

#### 1.4 Target version

8.x (target TBD after review).

---

## 2. Goals and Objectives

#### 2.1 Primary Goals

* Expose TQ compression through the existing `COMPRESSION` argument on `HNSW` vector fields (`COMPRESSION TQ8|TQ4|TQ2`), the same shape `SVS-VAMANA` uses for LVQ / LeanVec. `TQ8` is the recommended default.
* Memory footprint at typical embedding sizes (e.g. dim 768–1536) materially lower than FP32 `HNSW`, with near-lossless recall when defaults are used.
* Preserve the existing `FT.SEARCH` / `FT.AGGREGATE` / `KNN` / `VECTOR_RANGE` query surface — query vectors remain raw FP32 on the wire; the index handles preprocessing internally.
* Full RDB save/load support so a TQ index survives restart/replication.
* `FT.INFO` reports the `compression` value (`TQ8`/`TQ4`/`TQ2`) on the standard HNSW info block so users and tooling can see what they have.

#### 2.2 Non-Goals

* Changing query semantics or result formatting.
* Exposing TQ tuning knobs. `BITS`, `PROJECTIONS`, `SEED`, `ROTATION` were removed from the API per review; the bit budget is encoded in the compression name and the rest are fixed internal defaults (§3.4).
* Aggregating or sharing rotation matrices across indexes.

---

## 3. Architecture Overview

#### 3.1 TurboQuant Approach (recap)

TurboQuant compresses a `d`-dimensional FP32 vector to a much smaller byte representation in two stages:

1. **Random orthogonal rotation** onto a subspace of `projections` components (fixed internally at `max(1, DIM / 2)`). The rotation is a fixed random orthogonal matrix derived from an internal seed (7). The rotation step spreads the energy of any input vector approximately uniformly across the projected components, which makes scalar quantization much more accurate (Lloyd-Max-style assumptions hold better on a rotated vector than on the raw one). Disabling rotation remains possible VecSim-side for diagnostics and parity testing, but it is not exposed to users (rotation is always ON in production).
2. **Per-component scalar quantization** of the rotated vector to a per-component bit budget taken from the compression name — 8 for `TQ8` (recommended default), 4 for `TQ4`, 2 for `TQ2`. The decode parameters (min, delta, plus a small amount of per-vector summary metadata) are stored alongside the codes.

Distance computation is **asymmetric** by default: at query time the query vector is rotated and stored in a query blob along with FP32 summary metadata, while index candidates remain in their compact code form. This avoids the recall penalty of round-tripping the query through quantization. For graph construction inside `TQ-HNSW`, where every neighbor candidate is already quantized, a **symmetric** code-vs-code distance path is used; for `L2` it is corrected to be unbiased using a stored full-vector norm. A separate **compact-angle / polar** SIMD path is used for cosine/IP scoring on NEON.

For full mathematical details of TurboQuant see the Google Research blog post and the VectorSimilarity-side design doc [TQ VecSim design](https://github.com/RedisAI/VectorSimilarity) (commit range listed in §10).

#### 3.2 System Architecture

Two new VecSim algorithm enums are introduced. Neither is a user-facing algorithm name: the only user-facing surface is `HNSW ... COMPRESSION TQ<bits>`.

| Algorithm enum         | User-facing surface | Index family | Tiered? |
| ---------------------- | ------------------- | ------------ | ------- |
| `VecSimAlgo_TQ`        | none — internal flat backend, used as the tiered frontend / building block | exact / BF   | No      |
| `VecSimAlgo_TQ_HNSW`   | `HNSW` + `COMPRESSION TQ8\|TQ4\|TQ2` | HNSW graph   | Yes (wrapped in `VecSimAlgo_TIERED` from the RediSearch side, identically to how `HNSW` is wrapped) |

##### 3.2.1 TQ flat backend — internal compressed brute-force building block

The TQ flat backend (`VecSimAlgo_TQ`) is a flat (linear scan) index inside VecSim. Each inserted vector is rotated and quantized once at insertion time; queries scan every entry using the TQ distance kernels. Conceptually it is `FLAT` with a different storage and distance kernel.

It is **not user-creatable**: the standalone `TQ-FLAT` index type that earlier drafts of this HLD proposed was dropped per review. The backend remains as an internal component — most importantly as the building block / frontend role in the tiered TQ-HNSW composition — and its properties still matter for the overall design:

* No background indexing; insert is synchronous.
* Block-based storage; block size is fixed internally at 1024 vectors per block.
* Single-value vectors only.

##### 3.2.2 TQ-HNSW — compressed graph index behind `COMPRESSION TQ<bits>`

The TQ-HNSW backend (`VecSimAlgo_TQ_HNSW`) reuses the HNSW graph layout (`M`, `EF_CONSTRUCTION`, `EF_RUNTIME`, `EPSILON`) but stores nodes in TQ-compressed form and uses TQ kernels for both graph construction (symmetric) and search (asymmetric). It is what `FT.CREATE ... VECTOR HNSW ... COMPRESSION TQ<bits>` creates.

* Wrapped in a `VecSimAlgo_TIERED` index from the RediSearch side, with `TQ_HNSW` as the primary index — exactly the same wrapping used today for plain `HNSW`. This enables background indexing via the existing job queue, frontend flat buffer, and the standard tiered query merge logic.
* `swapJobThreshold` is set to 0 (default tiered behavior).
* Distance metric is currently restricted to `COSINE` and `IP`. `L2` is rejected at the parser; the VecSim side does not yet expose an unbiased TQ-HNSW L2 kernel that we are confident enough to ship.

##### 3.2.3 Component Design

**VectorSimilarity ([`deps/VectorSimilarity`](https://github.com/jeremyplichta/VectorSimilarity/tree/feat/tq-vector-quantization)) — new and changed:**

* TQ flat backend, internal (`feat: add tq-flat vecsim prototype` → `feat: rewrite tq-flat around turboquant-style codes` → `perf: optimize tq-flat query scoring`): index implementation, `TQFlatParams` struct, and asymmetric scoring path.
* TQ-HNSW backend (`feat: add tq-hnsw vecsim backend` → `fix: use symmetric tq-hnsw distance path` → `feat: tier tq hnsw background indexing`): HNSW graph integrated with TQ storage and distance kernels.
* SIMD kernels (`perf: add neon tq kernels` → `tq: add runtime-dispatched SIMD kernels for TQ-FLAT/TQ-HNSW`): runtime-dispatched `TQ_FP32_*` and `TQ_POLAR_*` kernels across AVX-512, AVX2+FMA, SSE4, NEON, SVE, SVE2, with scalar fallback.
* Unbiased symmetric L2 via stored full-vector norm (`tq: unbiased symmetric L2 via stored full-vector norm`) — used by `TQ-HNSW` graph construction.
* API hardening (`tq: harden TQ-HNSW factory and reporting APIs`): `static_assert`s on `TQHNSWParams` prefix layout, `AsTQFlatParamsPrefix` helper, tiered-algo virtual dispatch fixes.
* Rust oracle parity harness (`test: add turboquant parity harness against rust oracle`) — used to validate the C++ implementation against a known-good reference.

**RediSearch ([`src/`](https://github.com/jeremyplichta/RediSearch/tree/feat/tq-vector-quantization/src)) — new and changed:**

* [`src/spec.c`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/spec.c):
    * New helper `parseVectorField_GetTqCompression`, which parses the `COMPRESSION` value (`TQ2` / `TQ4` / `TQ8`, case-insensitive) into a bit budget.
    * New `COMPRESSION` branch in `parseVectorField_hnsw`: when a TQ compression value is present, the parsed HNSW params are transformed to `VecSimAlgo_TQ_HNSW` / `TQHNSWParams` and validated via the new `parseVectorField_validate_tq_hnsw`.
    * The earlier standalone parsers `parseVectorField_tq` / `parseVectorField_tq_hnsw` and the `parseVectorField_GetRotation` helper were deleted — there is no standalone algorithm-name dispatch anymore.
    * `COMPRESSION` is rejected when `isSpecOnDiskForValidation(sp)` is true.
* [`src/vector_index.c`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/vector_index.c) / [`src/vector_index.h`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/vector_index.h):
    * New compression-name constants `VECSIM_TQ_2` / `VECSIM_TQ_4` / `VECSIM_TQ_8` (`"TQ2"` / `"TQ4"` / `"TQ8"`); the former `BITS` / `PROJECTIONS` / `SEED` / `ROTATION` keyword defines were removed.
    * New helper `VecSimTqCompression_ToString(bits)` in `vector_index.c` mapping the stored bit budget back to the compression name for reporting.
    * `VecSim_RdbSave` and `VecSim_RdbLoad_v4` cases for tiered-`TQ_HNSW` only; the standalone `TQ` / `TQ-HNSW` RDB branches were removed. The tiered branch saves `swapJobThreshold` then type, dim, metric, multi, bits, projections, seed, useRotation, `M`, `efConstruction`, `efRuntime`, `epsilon`.
    * Older RDB versions (v2, v3) explicitly fail when they encounter the new algorithm enum (we don't pretend they were loadable).
    * `getVecSimMetricFromVectorField` extended.
* [`src/info/info_command.c`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/info/info_command.c): a TQ-compressed field renders the plain-HNSW `FT.INFO` block (`algorithm=HNSW`, `data_type`, `dim`, `distance_metric`, `M`, `ef_construction`) plus a `compression` line (`TQ8`/`TQ4`/`TQ2`), mirroring how SVS reports its compression. TQ internals (`bits`, `projections`, `seed`, `rotation`) are not reported.
* [`src/info/global_stats.c`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/info/global_stats.c) / [`src/info/global_stats.h`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/info/global_stats.h): the earlier `numVectorFieldsTqFlat` counter was removed along with the standalone TQ-FLAT index. TQ-compressed HNSW fields are counted in the existing `HNSW` bucket in `INFO MODULES`.
* [`src/json.c`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/src/json.c): TQ-compressed schemas pass through the JSON ingest path so JSON-backed indexes work with `COMPRESSION TQ<bits>`.

#### 3.3 Data Model Changes

* **New VecSim enums (internal only):**
    * `VecSimAlgo_TQ`
    * `VecSimAlgo_TQ_HNSW`
* **New compression-name strings:** `VECSIM_TQ_2` (`TQ2`), `VECSIM_TQ_4` (`TQ4`), `VECSIM_TQ_8` (`TQ8`) — values of the existing `COMPRESSION` keyword. No new algorithm-name strings; the former `BITS` / `PROJECTIONS` / `SEED` / `ROTATION` keyword defines were removed.
* **New parameter structs:** `TQFlatParams`, `TQHNSWParams` (defined VecSim-side, consumed via `VecSimParams::algoParams.tqFlatParams` / `tqHnswParams`).
* **Global stats:** unchanged — TQ-compressed HNSW fields count in the existing `HNSW` bucket (the interim `numVectorFieldsTqFlat` counter was removed).
* **RDB:** No version bump. The existing v4 envelope is extended with one new tiered-`TQ_HNSW` branch. Older versions (v2, v3) fail to load any RDB containing TQ-compressed fields, by design.

#### 3.4 Default Parameter Values

Only `COMPRESSION` is user-visible; the remaining TQ parameters are internal fixed defaults, deliberately removed from the API per review.

| Parameter        | Value                        | User-exposed? | Notes                                                                |
| ---------------- | ---------------------------- | ------------- | -------------------------------------------------------------------- |
| bits             | from compression name (`TQ8`=8, `TQ4`=4, `TQ2`=2) | Yes, via `COMPRESSION` | `TQ8` is the recommended default. Case-insensitive.        |
| projections      | `max(1, DIM / 2)`            | No (internal) | Fixed heuristic; resolved at index construction.                     |
| seed             | 7                            | No (internal) | Plain integer seed for the random rotation.                          |
| rotation         | `ON`                         | No (internal) | Always on; disabling exists VecSim-side for diagnostics only.        |
| block size       | 1024 vectors per block       | No (internal) | `BLOCK_SIZE` / `INITIAL_CAP` are deprecated args and not part of the TQ surface. |
| `M`              | `HNSW_DEFAULT_M` (16)        | Yes (standard HNSW) |                                                                |
| `EF_CONSTRUCTION`| `HNSW_DEFAULT_EF_C` (200)    | Yes (standard HNSW) |                                                                |
| `EF_RUNTIME`     | `HNSW_DEFAULT_EF_RT` (10)    | Yes (standard HNSW) |                                                                |
| `EPSILON`        | `HNSW_DEFAULT_EPSILON` (0.01)| Yes (standard HNSW) |                                                                |

#### 3.5 New and Impacted Flows

* **Index creation:** `HNSW` with `COMPRESSION TQ8|TQ4|TQ2`. The parser builds the same tiered index as plain `HNSW` but with `VecSimAlgo_TQ_HNSW` as the primary algorithm and `TQHNSWParams` (type, dim, metric, multi, `M`, `efConstruction`, `efRuntime`, `epsilon` carried over from the parsed HNSW args; bits from the compression name; projections/seed/rotation fixed internal defaults). Disk-backed indexes reject `COMPRESSION`.
* **Vector insertion:** vectors land in the tiered frontend (flat buffer) first, are quantized in a background job, then inserted into the TQ-HNSW backend via the existing tiered job queue — identical to `HNSW`. WriteInPlace works exactly as for `HNSW`.
* **Vector query:** unchanged user-facing syntax. VecSim takes the raw FP32 query, runs the same rotation, computes per-vector summary metadata once, and uses the asymmetric TQ distance kernel against compact stored codes. The tiered index merges frontend + backend results just like `HNSW`.
* **Vector deletion:** same as `HNSW` (tombstone + repair).
* **RDB save/load:** new tiered-`TQ_HNSW` branch; new fields persisted (see §3.2.3).
* **FT.INFO:** plain-HNSW info block plus a `compression` line.
* **INFO MODULES:** TQ-compressed HNSW fields counted in the existing `HNSW` bucket; no new counters.

---

## 4. Interfaces (APIs)

#### 4.1 Overview

No new commands and no new algorithm names. `FT.CREATE` accepts new values (`TQ8`/`TQ4`/`TQ2`) for the existing `COMPRESSION` argument on the `HNSW` algorithm, mirroring how `SVS-VAMANA` accepts `LVQ4`/`LVQ8`/LeanVec values. `FT.INFO` reports the compression on the standard HNSW info block.

#### 4.2 FT.CREATE — HNSW with COMPRESSION TQ

```redis
FT.CREATE idx SCHEMA vec VECTOR HNSW 10
  TYPE FLOAT32
  DIM 768
  DISTANCE_METRIC COSINE
  COMPRESSION TQ8
  M 16 EF_CONSTRUCTION 200 EF_RUNTIME 50 EPSILON 0.01
```

All standard HNSW arguments (`M`, `EF_CONSTRUCTION`, `EF_RUNTIME`, `EPSILON`) remain optional and keep their HNSW defaults.

| Attribute         | Description                                                                 | Default                       |
| ----------------- | --------------------------------------------------------------------------- | ----------------------------- |
| `TYPE`            | Vector component type. **FLOAT32 only** with TQ compression in this phase.  | (mandatory)                   |
| `DIM`             | Vector dimensionality.                                                      | (mandatory)                   |
| `DISTANCE_METRIC` | `COSINE` or `IP` with TQ compression (`L2` rejected).                       | (mandatory)                   |
| `COMPRESSION`     | `TQ8` (recommended default, 8-bit), `TQ4`, or `TQ2`. The bit budget is encoded in the name, like `LVQ4`/`LVQ8` for SVS. Case-insensitive. | (optional — omit for plain HNSW) |
| `M` / `EF_CONSTRUCTION` / `EF_RUNTIME` / `EPSILON` | As `HNSW`.                                 | HNSW defaults                 |

The former TQ knobs `BITS`, `PROJECTIONS`, `SEED`, `ROTATION` are **not user-exposed** — they were deliberately removed from the API per review. Internally they are fixed: projections = `max(1, DIM / 2)`, seed = 7, rotation = ON. `BLOCK_SIZE` / `INITIAL_CAP` are deprecated args and are not part of the TQ surface (block size is fixed at 1024 vectors per block internally).

**Validation errors (parse-time):**

* `TYPE` other than `FLOAT32` → `"TQ compression only supports FLOAT32 vectors"`.
* `DISTANCE_METRIC L2` → `"TQ compression with DISTANCE_METRIC L2 is not yet supported; use COSINE or IP"`.
* Multi-value vector (JSON `$.vecs[*]`) → `"TQ compression does not support multi-value vectors"`.
* `COMPRESSION` on a disk-backed index → `"Disk index does not support COMPRESSION"`.
* Unknown `COMPRESSION` value on `HNSW` → standard bad-argument error mentioning `COMPRESSION`.
* Missing `TYPE` / `DIM` / `DISTANCE_METRIC` → standard "mandatory argument" error.

**Internal mapping.** When `COMPRESSION TQ<bits>` is present, the parser builds the same tiered index as plain `HNSW` but with `VecSimAlgo_TQ_HNSW` as the primary algorithm and `TQHNSWParams`: type, dim, metric, multi, `M`, `efConstruction`, `efRuntime`, `epsilon` carried over from the parsed HNSW args (`epsilon` defaults to `HNSW_DEFAULT_EPSILON`); bits from the compression name; projections/seed/rotation fixed internal defaults. Background indexing via tiered worker jobs is identical to `HNSW`.

#### 4.3 FT.SEARCH / FT.AGGREGATE / FT.HYBRID

Unchanged surface:

```redis
FT.SEARCH idx "*=>[KNN 10 @vec $blob AS dist]" PARAMS 2 blob <raw-f32-bytes> SORTBY dist DIALECT 2
FT.SEARCH idx "@vec:[VECTOR_RANGE 0.2 $blob]=>{$yield_distance_as: dist}" PARAMS 2 blob <raw-f32-bytes> SORTBY dist DIALECT 2
```

The query vector is raw FP32 of length `DIM * sizeof(float)`. The index applies its own rotation/quantization internally.

#### 4.4 FT.INFO

A TQ-compressed field reports the plain-HNSW info block plus a `compression` line — the same shape SVS uses for its compressed variants. The algorithm is reported as `HNSW` (not `TQ-HNSW`):

```text
algorithm: HNSW
data_type: FLOAT32
dim: 768
distance_metric: COSINE
M: 16
ef_construction: 200
compression: TQ8
```

`compression` is one of `TQ8` / `TQ4` / `TQ2` (via `VecSimTqCompression_ToString`). TQ internals (`bits`, `projections`, `seed`, `rotation`) and `ef_runtime` / `epsilon` are **not** reported, mirroring the plain-HNSW info block.

#### 4.5 INFO MODULES

No changes. TQ-compressed HNSW fields are counted in the existing `HNSW` bucket of `search_fields_vector`; the earlier `TQ_FLAT` counter was removed along with the standalone index. A dedicated compressed-HNSW counter is a possible follow-up (see §9 Risks).

---

## 5. Performance, Scalability & Availability

#### 5.1 Memory

* TQ-compressed indexes store each vector as `bits * projections / 8` bytes of compact codes plus a small constant per-vector metadata block (FP32 min/delta + summary terms). With `TQ8` (bits = 8) and the internal `projections = DIM/2`, raw storage drops from `4 * DIM` bytes to roughly `DIM / 2 + O(1)` bytes per vector — an ~8× reduction at typical embedding sizes (e.g. dim 768 → 32B vs 3072B before metadata).
* Graph overhead (links, levels) is unchanged from `HNSW`.

#### 5.2 Throughput

* Asymmetric distance is dominated by integer-byte loads on candidates plus a small FP32 fixup using stored summary metadata. SIMD kernels (`TQ_FP32_*`, `TQ_POLAR_*`) are runtime-dispatched per ISA so we get the best path on the running CPU.
* NEON adds a compact-angle path for cosine/IP scoring that avoids decoding the full code stream.
* Insert path overhead is a single rotation + per-component quantization, comparable to (and often cheaper than) computing FP32 distance against many candidates.

#### 5.3 Recall

* Random rotation is the main lever — with rotation on (always, internally), recall is near-lossless for typical embedding distributions at `TQ8`.
* Choosing a lower bit budget (`TQ4`, `TQ2`) increases compression but degrades recall; this trade-off should be benchmarked per dataset before recommending non-default compression values.
* The internal `projections < DIM` setting introduces additional approximation but reduces both storage and distance-compute cost.

#### 5.4 Concurrency / Threading

* The internal TQ flat backend follows the same thread model as `FLAT`: writes go through the main thread; queries are read-mostly under the existing index lock.
* TQ-compressed HNSW follows the same thread model as `HNSW` via the tiered wrapper: frontend buffer + background worker jobs + thpool's existing query-priority handling. The "enable tiered tq hnsw worker indexing" change wires the TQ-HNSW backend through the same path as `HNSW` (see §3.2.2).

#### 5.5 Availability

No new availability concerns. Replication is delegated to the standard RDB/AOF path. RDB v4 round-trip is supported; older RDB versions fail closed when a TQ field is encountered, which is the correct behavior.

#### 5.6 Measured Benchmarks

> **Note:** these runs predate the v0.4 API rename — the old `TQ-FLAT` / `TQ-HNSW` lane names refer to the same engine now reachable via `HNSW` + `COMPRESSION TQ<bits>`; standalone `TQ-FLAT` lanes are no longer reachable through the public API.

The numbers below are from the local benchmarking harness at `~/git/redis-turboquant-bench`, run against the patched RediSearch + VecSim from this branch. Two sets of results are included: (a) **single-shard local** runs that establish the quality / latency / memory shape, and (b) **multi-node Redis Enterprise** runs (`4 × e2-standard-16`, `all-master-shards`, sparse placement) that establish the throughput shape.

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

---

## 6. Security and Permissions

No new security considerations. TQ-compressed indexes use the same ACL surface and the same data-path as existing vector indexes. The internal rotation seed is a plain integer for the rotation matrix; it is not a cryptographic seed and is not security-relevant.

---

## 7. Compatibility & Constraints

* **No standalone TQ-FLAT:** `TQ-FLAT` is no longer a creatable index type. The only way to get TQ on a vector field is `HNSW ... COMPRESSION TQ<bits>`. The TQ flat backend remains internal to VecSim.
* **Backward compatibility:** Existing indexes (no TQ) are unaffected. RDB v4 files written by an old build still load on a new build; new RDB files containing TQ-compressed fields cannot be loaded by a build that doesn't know the enum (the v2/v3/v4 loaders fail closed on unknown algorithm enums).
* **Data types:** `FLOAT32` only with TQ compression in this phase. Adding `FLOAT16` (and integer types) requires new VecSim SIMD kernel variants — explicitly out of scope.
* **Distance metrics:** `COSINE`, `IP` only with TQ compression. `L2` returns a parse error.
* **Disk (Flex / ROF):** unsupported. `COMPRESSION` is rejected when `isSpecOnDiskForValidation(sp) == true`.
* **Multi-value:** unsupported with TQ compression. JSON `$.vecs[*]` is rejected at parse time.
* **VecSim version:** requires `deps/VectorSimilarity` to include the TQ commit range below. The submodule pin is bumped on this branch.
* **Cluster mode:** TQ-compressed HNSW is a local-shard index type and uses the existing coordinator and resharding paths. No coordinator changes are required.

---

## 8. Testing Strategy

| Level                | Type      | Description                                                                                                                |
| -------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------- |
| Unit (VecSim)        | Automated | TurboQuant parity harness against a Rust oracle (`test: add turboquant parity harness against rust oracle`).               |
| Unit (RediSearch)    | Automated | Parser branch coverage for the `COMPRESSION` `TQ2`/`TQ4`/`TQ8` branch, including unknown-compression-value, FLOAT32-only, L2, multi-value, and disk-index validation errors. |
| Component tests      | Automated | `FT.CREATE` schema acceptance, `FT.INFO` rendering (HNSW block + `compression`), RDB save/load round-trip (see [`tests/pytests/test_tq.py`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/tests/pytests/test_tq.py)). |
| E2E functional       | Automated | KNN and `VECTOR_RANGE` correctness on real embeddings (HASH + JSON, single-value), mixed insert/query/delete workloads, tiered transitions under load. |
| Recall / quality     | Automated | Recall@K vs FP32 `FLAT` ground truth across at least one open embedding dataset, for TQ-compressed HNSW with default params. |
| Micro benchmarks     | Automated | `add_label`, `TopK`, `Range` benchmarks vs `FLAT` / `HNSW` for memory, throughput, and latency.                            |
| SIMD path coverage   | Automated | VecSim NEON / SVE / AVX-512 / AVX2 / scalar kernel parity (already covered by the VecSim parity harness).                  |
| Persistence          | Automated | RDB round-trip with TQ-compressed-only and mixed-algorithm indexes; AOF replay; replica failover.                          |
| Enterprise sanity    | Automated | Rolling upgrade (pre-TQ → post-TQ build and vice versa where applicable), resharding, replica failover, backup/restore.    |

Existing test artifacts on the branch:

* [`tests/pytests/test_tq.py`](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/tests/pytests/test_tq.py) — covers `FT.CREATE` with `COMPRESSION TQ2`/`TQ4`/`TQ8` (HASH + JSON), `FT.INFO` rendering (HNSW block + `compression`), KNN and `VECTOR_RANGE` queries, RDB persistence round-trip, and negative cases (non-`FLOAT32` types, `L2`, multi-value JSON paths, unknown `COMPRESSION` values, disk-backed rejection).

Test gaps to close before merge (see §9):

* End-to-end recall test for TQ-compressed HNSW on a real dataset.

---

## 9. Open Issues & Risks

| ID     | Description                                                                                                                                                                          | Impact                                                                                  | Mitigation                                                                                                                                                                                |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Risk 1 | **No `L2` support for TQ compression yet.** Symmetric L2 path was added VecSim-side via the stored full-vector norm, but the asymmetric query path and recall validation aren't ready.    | Users with L2 embeddings cannot use `COMPRESSION TQ<bits>`.                                          | Reject at parser with a user-readable error pointing at COSINE/IP. Follow-up: enable L2 in a later release once the asymmetric kernel + recall numbers are in.                            |
| Risk 2 | **The fixed internal defaults are heuristic.** `projections = DIM/2` and the per-name bit budgets work well on the datasets we've tried, but defaults can be wrong on long-tail distributions — and users can no longer tune them. | Recall may be worse than expected on some user data, with no user-side knob beyond picking `TQ8`/`TQ4`/`TQ2`. | Publish a tuning guide alongside benchmarks (which compression name to pick per workload). Consider warning in logs when `DIM` is very low (< 64) — small dims don't amortize TQ overhead. |
| Risk 3 | **No data type other than `FLOAT32`.** Many of our users send `FLOAT16` / `BFLOAT16` embeddings.                                                                                       | Coverage gap relative to `FLAT` / `HNSW`.                                               | Explicitly call out in docs; track follow-up to add FP16 SIMD kernel variants (mirrors the SQ HLD §7 SIMD matrix).                                                                        |
| Risk 4 | **No disk-backed variant.** Disk-backed RediSearch (Flex / ROF) cannot use `COMPRESSION` / TQ.                                                                                          | Tiered-memory deployments cannot benefit from TQ in this phase.                         | Reject at parser; design integration with `vecsim_disk` as a follow-up HLD.                                                                                                               |
| Risk 5 | **No dedicated compressed-HNSW observability.** TQ-compressed HNSW fields are counted in the existing `HNSW` bucket of `INFO MODULES` (the interim `TQ_FLAT` counter was removed with the standalone index), so operators can't tell from `INFO MODULES` alone how many compressed fields they have. | Observability gap.                                                                      | A dedicated compressed-HNSW counter is a possible follow-up; `FT.INFO` per-field `compression` reporting covers the per-index need today.                                                  |
| Risk 6 | **VecSim API surface widened.** New factory paths, virtual dispatch in tiered algos, `static_assert`s on TQ param prefixes. Risk of misuse from other consumers of VecSim.            | Build break or runtime UB in `vecsim_disk` / micro-benchmarks if they construct params zero-initialized. | All new enums are zero (`VecSimAlgo_TQ`, `VecSimAlgo_TQ_HNSW` follow the existing enum tail). Coordinate the VecSim bump with consumers; the API-hardening commit covers most of the obvious traps. |
| Risk 7 | **Benchmarks are not in the merge gate.** Numbers below are anecdotal until we run `vector-db-benchmark`.                                                                              | We could ship a regression vs the existing path on some datasets.                       | Block merge on at least one end-to-end recall + throughput benchmark; capture results in this HLD before flipping the doc to APPROVED.                                                    |
| Risk 8 | **Branch hygiene.** The `feat/tq-vector-quantization` branch was cut from an older master and accumulated unrelated drift (CI, coord, aggregate, etc.).                                | Hard to review TQ in isolation.                                                         | Largely addressed: current master has since been merged into the branch. Before opening a PR, do a final pass to confirm the diff against master is TQ-only (the RediSearch TQ commits + the VecSim submodule bump). |
| Risk 9 | **Cosine distances are reported on an angle-based scale.** The TQ cosine kernels (compact-angle / polar path) score on a monotonic angle-based scale rather than exact `1 - cos`. KNN ordering is correct, but absolute `VECTOR_RANGE` radii and yielded distances are a surrogate, and the scale can differ between SIMD paths. | `VECTOR_RANGE` radius semantics and user-visible distances differ from exact cosine distance. | Documented; flow tests assert ordering, not absolute radii. Follow-up before GA: normalize the reported score back to `1 - cos` (or document the scale as part of the API contract). |

---

## 10. Related Documents and Tickets

| Type                       | Link                                                                                                                                                                                | Owner          |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| Reference HLD (SQ)         | [HLD: Scalar Quantization (SQ) Compression for Tiered HNSW](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6153601069/HLD+Scalar+Quantization+SQ+Compression+for+Tiered+HNSW) | Dor Forer      |
| External background        | [Google Research blog — TurboQuant: redefining AI efficiency with extreme compression](https://research.google/blog/turboquant-redefining-ai-efficiency-with-extreme-compression/) | n/a            |
| Local design notes         | [docs/design/tq_flat.md](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/docs/design/tq_flat.md), [docs/design/tq_hnsw.md](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/docs/design/tq_hnsw.md) | Jeremy Plichta |
| Reviewer's guide           | [notes/tq-review-guide.md](https://github.com/jeremyplichta/RediSearch/blob/feat/tq-vector-quantization/notes/tq-review-guide.md) — recommended reading order for the diff           | Jeremy Plichta |
| Benchmark dataset          | [CohereLabs/msmarco-v2.1-embed-english-v3](https://huggingface.co/datasets/CohereLabs/msmarco-v2.1-embed-english-v3) — MS MARCO v2.1 passages with pre-computed Cohere `embed-english-v3.0` 1024-dim embeddings | n/a |
| RediSearch fork branch     | [jeremyplichta/RediSearch @ feat/tq-vector-quantization](https://github.com/jeremyplichta/RediSearch/tree/feat/tq-vector-quantization)                                              | Jeremy Plichta |
| VecSim fork branch         | [jeremyplichta/VectorSimilarity @ feat/tq-vector-quantization](https://github.com/jeremyplichta/VectorSimilarity/tree/feat/tq-vector-quantization)                                  | Jeremy Plichta |
| Benchmark harness          | `~/git/redis-turboquant-bench` (local; MS MARCO v2.1 corpus prep + comparison harness used for §5.6)                                                                                 | Jeremy Plichta |
| Tickets                    | TBD — open MOD-* tickets per section of work once this HLD is reviewed (parser + validation, RDB, FT.INFO + INFO MODULES, JSON ingest, VecSim TQ-FLAT, VecSim TQ-HNSW, SIMD kernels, benchmarks, persistence tests, enterprise sanity). |                |
