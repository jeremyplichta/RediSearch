# TurboQuant production handoff for Joo Bin

**Status date:** 2026-08-27

**Primary implementation branch:** `codex/tq-production-20260824`

**Purpose:** give Joo Bin enough technical and repository context to reproduce, review, benchmark, and continue the paper-faithful TurboQuant work without relying on the original Codex conversation.

## Read this first

This work spans three repositories and several generations of TurboQuant experiments. The current implementation is the **paper-faithful asymmetric estimator** described in this document and in the linked design materials. Older documents that discuss a pairwise-polar or compressed-to-compressed estimator are historical context only; they are not the current mathematical contract.

The current code is locally committed on dedicated branches and tagged checkpoints. The implementation branches should be consumed together because RediSearch pins a specific VectorSimilarity commit, and the benchmark harness expects the corresponding RediSearch module behavior.

The smallest useful reading order is:

1. This handoff.
2. [`HLD-turboquant-vector-quantization.md`](../HLD-turboquant-vector-quantization.md).
3. [`turboquant-paper-faithful-pivot.md`](../../deps/VectorSimilarity/docs/turboquant-paper-faithful-pivot.md).
4. The benchmark report and reproduction guide in the benchmark repository:
   `reports/msmarco-local-amd64-emulated-profiles-20260826/{comparison.md,README.md}`.
5. The nine internal production specs in the separately distributed context archive. Those specs were intentionally kept out of Git and are identified by name later in this document.

## Executive summary

TurboQuant (TQ) is being evaluated as a compressed vector representation and asymmetric scoring backend for Redis vector search. The production effort replaced an earlier, non-paper-faithful prototype with an implementation based on the paper's two-stage construction:

- Algorithm 1 rotates the stored vector, quantizes each rotated scalar with a Lloyd-Max scalar quantizer, and stores packed centroid indices.
- Algorithm 2 sketches the normalized quantization residual with Gaussian random projections and stores the signs plus two FP32 scalars, `alpha` and `gamma`.
- Query scoring is asymmetric: the stored compressed representation is compared with a preprocessed raw query. There is no raw-vector sidecar in the TQ payload.

The intended inner-product estimate is:

```text
alpha * (
  <Pi*y, centroid[index]>
  + gamma * sqrt(pi/2) / d * <S*y, residual_signs>
)
```

For cosine search, distance is `1 - estimated_inner_product`. The current public contract is deliberately narrow: FP32 cosine and inner product, exact sketch dimension `m = d`, dimensions `d >= 2`, mandatory rotation, and TQ2/TQ4/TQ8 bit widths. L2 and a public arbitrary sketch dimension are not part of the current contract.

Two structured profiles were added so the dense paper-reference model can be swapped at index creation time:

| Profile | Rotation | QJL sketch | Main purpose |
| --- | --- | --- | --- |
| `DenseReferenceV1` | Dense Gaussian-derived orthogonal matrix | Dense Gaussian matrix | Independent/reference behavior and small-scale conformance |
| `FastStructuredRotationV1` | Structured signed/permuted FWHT | Dense Gaussian matrix | Isolate rotation memory and construction cost |
| `FastStructuredV1` | Structured signed/permuted FWHT | Circulant structured Gaussian projection | O(d) model state with O(d log d) transforms |

If `TQ_PROFILE` is omitted, the current implementation selects `DenseReferenceV1`. That is useful for compatibility and reference testing but is not a safe production default at large dimensions or scale. Resolving the product default and rollout policy is an explicit next-step decision.

The bounded MS MARCO experiment shows that the fully structured profile is the only current TQ profile that materially reduces Redis vector index bytes at `d=1024`, while SVS VAMANA LVQ8 provides stronger compression and much faster construction in this small run. Absolute throughput and latency numbers from that experiment are **not capacity results** because Redis ran as linux/amd64 under emulation on an Apple M3 Pro. Native AMD64 testing is the immediate priority.

## Repository and revision map

Use these exact branches together.

| Repository | Required branch | Exact handoff revision | Review tag | Role |
| --- | --- | --- | --- | --- |
| `jeremyplichta/VectorSimilarity` | `codex/tq-production-20260824` | `17d366571f637b1ed80612efc32bf0df3db627ea` | `codex/tq-production-20260826-swappable-profiles-vecsim` | TQ encoding, scoring, models, HNSW construction metric, persistence, tests, and microbenchmarks |
| `jeremyplichta/RediSearch` | `codex/tq-production-20260824` | The handoff commit that contains this document; it descends from `645ec77284c408c83caddaf03bb31a6d94c836c2` | `codex/tq-production-20260827-joo-bin-handoff-redisearch` | `FT.CREATE`/`FT.INFO`, RDB integration, hybrid query lifecycle, validation, and integration tests |
| `jeremyplichta/redis-turboquant-bench` | `codex/msmarco-compressed-ann-20260825` | `97bb4412e1b469891972d0732ca689c2dbad3a1b` | `codex/msmarco-compressed-ann-20260826-profile-results` | Reproducible MS MARCO loading, evaluation, result capture, and comparison reporting |

The benchmark repository is intentionally independent from the product repositories. It contains committed small-run results and test coverage for the harness, but not the approximately 1 GiB prepared corpus cache.

### Clone and initialize RediSearch correctly

RediSearch's `.gitmodules` file points to the upstream VectorSimilarity repository. The required TQ submodule commit lives on Jeremy's fork, so override the URL **before** initializing the submodule:

```bash
git clone git@github.com:jeremyplichta/RediSearch.git
cd RediSearch
git checkout codex/tq-production-20260824
git config submodule.deps/VectorSimilarity.url \
  git@github.com:jeremyplichta/VectorSimilarity.git
git submodule update --init --recursive

git -C deps/VectorSimilarity rev-parse HEAD
# Expected: 17d366571f637b1ed80612efc32bf0df3db627ea
```

Do not run `git clone --recursive` before configuring that override unless the required commit has also landed upstream.

### Clone the benchmark harness

```bash
git clone git@github.com:jeremyplichta/redis-turboquant-bench.git
cd redis-turboquant-bench
git checkout codex/msmarco-compressed-ann-20260825
git rev-parse HEAD
# Expected: 97bb4412e1b469891972d0732ca689c2dbad3a1b
```

The repositories and the separately distributed internal context archive are private handoff material. Joo Bin needs access to all three forks or must fork them into a location where he has write access. Do not assume upstream Redis repositories contain these branches.

### Redis runtime

A separate source checkout of Redis is not required. The benchmark workflow uses Redis 8.8.0 in Docker. The host Homebrew Redis found during development was 7.4.1 and is too old for the required module/testing path. Use the pinned container workflow from the benchmark repository or an equivalent Redis 8 build.

## How the work evolved

### 1. Initial prototype and the paper-faithful pivot

The earliest prototype used pairwise-polar compression and a compressed-to-compressed scoring idea. A detailed audit found that this was not the algorithm in the TurboQuant paper. The implementation was therefore pivoted rather than incrementally patched.

The current authority is:

- the global paper-faithful contract;
- the independent dense oracle;
- the current VectorSimilarity implementation and tests;
- the current RediSearch HLD and integration behavior.

Historical pairwise-polar notes are included in the context ZIP only to explain why certain choices were rejected. They must not be used to infer current persistence, payload, or scoring behavior.

### 2. Safe asymmetric query scoring

The query side now preprocesses a raw FP32 query into a reusable context. This is important because a hybrid/HNSW query may score many stored vectors. Query preprocessing must happen once per query, not once per candidate.

The work also hardened ownership, error propagation, and ad-hoc scoring so malformed or allocation-failing preprocessing cannot escape as an exception across C boundaries. The context lifetime is tied to the query rather than to a stored vector.

### 3. Independent conformance oracle

An independent dense oracle was added for deterministic conformance. It reconstructs the paper estimator separately from the production implementation and checks:

- packed centroid decoding;
- residual sign decoding;
- `alpha` and `gamma` handling;
- asymmetric raw-query scoring;
- deterministic seed behavior;
- distance conversion for cosine/IP.

This oracle is the primary guard against accidentally making an implementation and its test agree on the same wrong formula.

### 4. HNSW construction without a compressed-to-compressed paper claim

The paper defines asymmetric stored-to-raw-query scoring; it does not define a compressed-to-compressed estimator for graph construction. For HNSW construction and repair, the implementation uses a separate `CoarseMse` construction metric: decode the Algorithm 1 coarse reconstructions and evaluate the exact configured metric between those reconstructions.

This separation is deliberate:

- search uses the paper estimator;
- graph construction uses a documented engineering metric;
- neither path silently depends on a raw sidecar.

### 5. Allocator-backed, versioned model state

The model and vector allocations use VecSim/Redis-compatible allocators and are included in memory accounting. Model format, vector payload format, and backend/profile identity are versioned independently so incompatible changes fail closed instead of being misinterpreted.

Persistence tests cover valid round trips and rejection of unsupported or historical layouts. If an optimization changes floating-point output, serialized model state, or query semantics, add a new version/profile rather than changing `V1` behavior in place.

### 6. Fast structured transforms

The dense model stores O(d squared) matrices. At `d=1024`, this fixed model memory can exceed the entire compressed vector payload in a small index. Two structured replacements were therefore implemented:

- signed/permuted normalized FWHT rotation;
- a circulant Gaussian QJL projection evaluated with a portable scalar FP64 FFT.

These reduce model state to O(d) and transform work to O(d log d). The structured QJL profile is an engineering approximation to the dense independent-Gaussian reference: its circulant rows are correlated. It needs additional bias, variance, tail, and recall characterization before production rollout.

### 7. Swappable index-creation profiles

Later product direction requested that the model choices be selectable at index creation. RediSearch now parses an optional, case-insensitive `TQ_PROFILE` argument and forwards the selected profile into VectorSimilarity. `FT.INFO`, persistence, and tests report/preserve the selection.

This intentionally supersedes an earlier rollout-spec recommendation not to expose a public backend selector initially. The public surface now exists on the feature branch, but naming, default behavior, and rollout eligibility still need maintainer/product review.

## Current implementation inventory

### VectorSimilarity

The VectorSimilarity branch contains:

- paper-faithful TQ2/TQ4/TQ8 payloads and asymmetric scoring;
- safe query preprocessing and reusable contexts;
- the independent dense oracle;
- `CoarseMse` HNSW construction and repair scoring;
- allocator-backed/versioned models and payloads;
- dense, structured-rotation, and fully structured profiles;
- RDB/model serialization support and rejection tests;
- TQ flat, HNSW, tiered, estimator, exception-safety, and accounting tests;
- production-dimension microbenchmark coverage;
- bundled SVS compression-header detection for VAMANA LVQ benchmarking.

Key implementation history after the paper-pivot base:

```text
13ea9e9e fix: preprocess TQ-HNSW ad-hoc queries safely
dc177e79 test: add independent dense TurboQuant paper oracle
602f4bdc perf: add allocation-free coarse TQ stored distance
ceb899e3 refactor: version and account TurboQuant model state
a98820cd feat: add scalar versioned structured TurboQuant rotation
c432b03c feat: add versioned circulant Gaussian TurboQuant QJL
154e2ffe bench: add production-dimension TurboQuant quality matrix
ef247645 fix: contain VecSim estimator exceptions
fc97c225 fix: contain TurboQuant query preprocessing failures
e94c5642 fix: make TurboQuant benchmark metadata truthful
4cdefa98 test: account for allocator diagnostics in baseline
0cfb40b9 build: detect compression headers in bundled SVS
17d36657 feat: expose versioned TurboQuant profiles
```

Three older stacked branches were used during the initial work:

```text
codex/tq-vecsim-01-core-flat
codex/tq-vecsim-02-hnsw-tiered
codex/tq-vecsim-03-simd-benchmarks
```

Treat those as historical review checkpoints. Do not amend, rebase, or force-push them. Continue from `codex/tq-production-20260824`.

### RediSearch

The RediSearch branch contains:

- `FT.CREATE` parsing and validation for TQ and `TQ_PROFILE`;
- profile propagation into VectorSimilarity;
- `FT.INFO` reporting;
- hybrid-query reuse of the preprocessed query context;
- RDB validation and persistence integration;
- Python/integration tests for accepted and rejected configurations;
- architecture and rollout documentation;
- the pinned VectorSimilarity submodule revision.

Key implementation history after the original feature branch:

```text
5b1bdfb87 perf: reuse TQ query preprocessing in hybrid scoring
a2edfbc04 feat: harden versioned TurboQuant persistence and rollout
9748360e7 docs: define TurboQuant reference rollout contract
cf85fe9b2 build: advance VectorSimilarity TurboQuant production stack
e782a984c build: enable bundled SVS compression detection
c35450c8b fix: scope TQ RDB loader variables for GCC
645ec7728 feat: select versioned TurboQuant profiles
```

### Benchmark harness

The benchmark branch contains:

- MS MARCO corpus/query preparation and caching;
- exact relevance/ground-truth evaluation for the bounded run;
- Redis loading with fail-closed index-count checks;
- raw FLAT and HNSW baselines;
- a matched raw HNSW control;
- all three TQ profiles;
- raw SVS VAMANA and VAMANA LVQ8;
- accuracy, precision, recall, F1, latency, request rate, vector index bytes, total index bytes, build time, and compression reporting;
- completeness/censoring metadata so a timed-out build cannot be reported as complete;
- 33 passing harness tests at the last checkpoint;
- committed small-run results and logs.

Key history:

```text
564e746 bench: compare compressed TQ and SVS on MS MARCO
86f087b bench: require trained SVS for bounded runs
b487799 bench: fail closed on incomplete vector indexing
0a7c1d6 bench: pair TQ with matched raw HNSW
ac9d1ad bench: disclose mixed result provenance
35650ea bench: explain bounded TQ model overhead
78d48ea bench: record local MS MARCO compressed ANN results
d55460f bench: compare selectable TurboQuant profiles
08acfe7 bench: report censored indexing results
97bb441 bench: record selectable backend MS MARCO results
```

## Benchmark results available now

The current comparison is a **correctness and workflow smoke test**, not a native performance result:

- dataset slice: 1,024 corpus vectors and 32 queries;
- dimension: 1,024 FP32;
- seed: 7;
- server: Redis 8.8.0 linux/amd64 Docker image;
- host: Apple M3 Pro running amd64 under emulation;
- all eight lanes produced the same bounded quality result: accuracy `0.9688`, precision `0.3719`, recall `0.9455`, F1 `0.4620`.

| Backend | p50 ms | p95 ms | Serial req/s | Concurrent req/s | Vector bytes MiB | Total index MiB | Vector reduction vs matched raw HNSW | Build |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Raw FLAT | 2.34 | 3.26 | 396.34 | 1,028.88 | 4.048 | 4.453 | n/a | 0.07 s |
| Raw HNSW, default | 3.51 | 4.44 | 279.56 | 970.01 | 4.329 | 4.735 | n/a | 1.11 s |
| Raw HNSW, matched M/EF | 4.23 | 6.61 | 223.29 | 800.70 | 4.447 | 4.853 | baseline | 1.11 s |
| TQ `DenseReferenceV1` | 14.81 | 17.79 | 65.99 | 248.01 | 13.443 | 13.849 | -202.3% | 713.48 s |
| TQ `FastStructuredRotationV1` | 13.59 | 17.28 | 71.71 | 244.98 | 5.493 | 5.898 | -23.5% | 2.94 s |
| TQ `FastStructuredV1` | 12.72 | 13.98 | 78.18 | 269.14 | 1.573 | 1.978 | 64.6% / 2.83x | censored at 1,800 s; later drained |
| SVS VAMANA raw | 6.49 | 27.36 | 78.16 | 589.19 | 4.132 | 4.537 | n/a | 4.16 s |
| SVS VAMANA LVQ8 | 4.99 | 8.77 | 176.44 | 439.76 | 1.141 | 1.546 | 72.4% / 3.62x | 4.13 s |

Interpretation:

- Fixed dense paper-model memory dominates the compressed payload at this tiny vector count. That cost is roughly fixed per index, while payload bytes grow per vector, so it amortizes as `N` increases. It is still O(d squared), however, and therefore remains operationally undesirable at production dimensions even if its per-vector share falls.
- `FastStructuredRotationV1` removes dense rotation state but retains dense QJL state, so it still exceeds matched raw-HNSW bytes here.
- `FastStructuredV1` produces material TQ byte reduction but has an unresolved graph-construction performance problem in this emulated run.
- VAMANA LVQ8 has the strongest size reduction and build behavior in this bounded comparison.
- Equal metrics on 32 queries are not evidence that the methods have equal quality. Full-corpus, multi-seed native evaluation is required.

## Prepared MS MARCO cache

The prepared cache was not committed because it is approximately 1 GiB. On the originating machine it is at:

```text
/Users/jeremy.plichta/git/redis-turboquant-bench/data/cache/prepared
```

Expected artifacts:

| Artifact | Shape/type | SHA-256 |
| --- | --- | --- |
| corpus vectors | `(253030, 1024)` FP32, about 988 MiB | `a17edb068e98d390deb9300ffcb3f0222b3f0338e8ed1b587ec0ff7b4e73a08c` |
| query vectors | `(214, 1024)` FP32, about 860 KiB | `b11e9f3122d6cafbb776c699c2159b2e802c7f475338c9fb390fffe1bcda5a4e` |
| metadata | JSON/metadata | `7a9ec3fd26a3c880b1ffe87afbcd7d2434dc7e99dfd937d037c60e6e9d0ef137` |
| flat calibration | cached calibration | `a42a645bc6de9414e3cff83e089aa9155ab99b05337a2921914832d774eaf7f0` |
| shards manifest | shard metadata | `a1b27e123b3ed35b9b63eea5e00c1eabb8bd979dce89154f24bd6ab543a66bbf` |

Use the benchmark repository's preparation command to reproduce the cache, or transfer the cache privately and verify every hash before using it. Do not put the cache into a Git repository or the documentation ZIP.

## Verification completed at the handoff checkpoint

The last implementation/evaluation checkpoint recorded:

- linux/amd64 release build with `HAVE_SVS_LVQ=1`;
- focused VectorSimilarity profile/factory tests: 6/6 passing;
- focused RediSearch persistence-marker tests: 8/8 passing;
- benchmark harness tests: 33/33 passing;
- Redis 8 index-creation and `FT.INFO` smoke coverage for all TQ profiles;
- all eight bounded benchmark lanes completed or were truthfully marked as censored.

Native host RLTest was blocked by the local Redis 7.4.1 runtime; the Redis 8 Docker path worked. Re-run the relevant build/test suites on native AMD64 before treating the branch as merge-ready.

## Known design tensions and open decisions

These are important because the internal specs and later product requests are not perfectly aligned.

1. **Public selector versus staged rollout.** The initial rollout spec recommended no public backend selector. A later explicit requirement added `TQ_PROFILE`. Current code follows the later requirement. Maintainers must decide whether the names/default are acceptable before upstreaming.
2. **Metadata-byte typo in one rollout spec.** One section says four metadata bytes and a 1,028-byte TQ8 payload at `d=1024`. The global contract and implementation store both `alpha` and `gamma` as FP32: eight metadata bytes and a 1,032-byte TQ8 payload. Treat the global contract/current code as canonical and correct the stale spec.
3. **Forward versus adjoint structured QJL notation.** A story document contains `S^T q` prose. The global estimator and implementation use the forward row projection `S q` paired with `sign(Sr)`; the adjoint is used for decode/reconstruction helpers, not query projection. Reconcile the prose before design approval.
4. **Default profile.** `DenseReferenceV1` is the current omitted-argument default, but dense O(d squared) state and construction cost are unsuitable as a general production default at 1,024+ dimensions. Decide whether the feature remains reference-only, changes default in a new contract, or gates structured profiles explicitly.
5. **Structured-QJL statistics.** Circulant Gaussian rows are correlated. Validate estimator bias, variance, tails, and recall against the independent dense oracle and raw baselines across seeds/dimensions.
6. **Construction time.** `FastStructuredV1` compressed well but did not finish its bounded graph build within the 1,800-second measurement window under emulation. Profile before optimizing; the likely hot path is scalar `CoarseMse`/structured transform work during graph construction.
7. **V1 compatibility.** Do not silently change persisted V1 model/payload behavior to gain speed. Any optimization that changes serialized state or numerical contract needs a V2 profile/version and cross-version fixtures.

## Recommended next steps

### Phase 1: establish native AMD64 truth

Use a native x86-64 GCP host before optimizing:

- primary: `c4d-highcpu-32` (AMD Turin) for the requested native AMD baseline;
- secondary: `c4-highcpu-32` (Intel) to compare ISA/runtime dispatch behavior, especially AVX-512 where available;
- run the load generator on a separate same-zone VM when measuring service throughput, or pin server/client CPUs if colocated;
- record CPU model, microcode, kernel, compiler, Docker/native mode, Redis/module SHAs, CPU governor, NUMA placement, and exact benchmark command.

Run the existing 1,024-vector/32-query matrix first to validate parity. Repeat each lane at least five times, then scale `N` through approximately 1K, 4K, 16K, 64K, and the full 253,030-vector corpus. Cover production dimensions including 768, 1,000, 1,024, 1,536, 3,000, and 3,072 where compatible datasets/synthetic controls exist.

Keep these measurements separate:

- index build/load wall time;
- peak and steady RSS;
- Redis vector-index bytes and total index bytes;
- p50/p95/p99 latency;
- serial and concurrent requests/second;
- accuracy, precision, recall, F1, and recall@K against exact raw ground truth;
- result completeness, timeouts, and censored runs.

### Phase 2: profile isolated VecSim kernels

Use the existing native benchmark `bm_tq_production_fp32` to isolate:

- model creation;
- vector encoding;
- query preprocessing;
- stored-to-query score;
- construction score;
- HNSW build/search/repair.

Relevant benchmark filters include `fp32`, `dense-full`, `dense-coarse`, `fast-dense`, and `fast-circulant`. Capture cycles, instructions, IPC, cache misses, branch misses, and flamegraphs. Do not run cargo/make/build benchmarks concurrently because they share build artifacts and skew timing.

### Phase 3: optimize in measured priority order

Likely candidates, subject to native profiles:

1. Runtime-dispatched AVX2/AVX-512 packed-centroid and sign-dot query scoring. The new paper path currently has only limited x86 SIMD coverage, so this is the clearest query-throughput opportunity.
2. SIMD `CoarseMse` scoring over packed graph-construction payloads.
3. SIMD butterflies/permutation for the structured FWHT rotation.
4. Precomputed bit-reversal/twiddle tables and vectorized circulant FFT; consider real/batched transforms after the simpler changes are measured.
5. Build-path caching or batching that preserves determinism, allocator accounting, and V1 persistence.

For every optimization, compare both microbenchmarks and end-to-end Redis results, and re-run the independent oracle/quality matrix. Do not trade away quality silently.

### Phase 4: production hardening

- run full multi-seed MS MARCO and synthetic quality matrices;
- add/expand RDB and AOF cross-version fixtures for every profile;
- run sanitizer, C/C++, Rust, Python, formatting, and lint suites;
- audit allocation accounting, exception containment, concurrency, and cancellation paths;
- resolve `TQ_PROFILE` naming/default/rollout with product and maintainers;
- stage TQ8 first, gate TQ4 behind evidence, and leave TQ2 experimental unless quality results justify it;
- convert the internal specs into the repository's proposal/design/tasks/spec-delta workflow before an upstream PR.

## Internal context archive

The separately supplied ZIP is intentionally not checked into any repository. It contains:

- the nine production roadmap/story specs (`00` through `08`);
- a manifest explaining which document is canonical and which is historical;
- selected untracked review/audit/walkthrough notes;
- a copy of this handoff.

The ZIP must pass an archive-integrity test and credential-pattern scan before distribution. Its manifest includes its SHA-256. Historical pairwise-polar documents are kept in a clearly marked directory and must not override the current paper-faithful contract.

## Working rules for the next developer or agent

- Start from the exact branches/revisions above; do not work in Jeremy's dirty primary checkouts.
- Create a dedicated worktree and a new `codex/` or developer-prefixed branch for each coherent continuation.
- Preserve commit history. The historical stacked branches are review checkpoints; do not rewrite them.
- Keep VectorSimilarity changes separate from the RediSearch submodule bump so they can be reviewed in dependency order.
- Do not push datasets, credentials, internal specs, or ignored notes into public repositories.
- Keep benchmark outputs explicit about platform, emulation, sample size, incomplete indexing, and result provenance.
- Any new public command option or persistence change requires the spec-driven workflow described by RediSearch's `AGENTS.md`.
- A behavior change is not done until C/C++, Rust, Python/integration, persistence, and quality coverage appropriate to the change are green.

## Ready-to-paste agent bootstrap prompt

```text
Continue the paper-faithful TurboQuant production work for RediSearch.

First read, in order:
1. docs/design/turboquant-joo-bin-handoff-20260827.md
2. docs/HLD-turboquant-vector-quantization.md
3. deps/VectorSimilarity/docs/turboquant-paper-faithful-pivot.md
4. redis-turboquant-bench/reports/msmarco-local-amd64-emulated-profiles-20260826/comparison.md
5. the 00-08 specs in the private handoff ZIP, observing its canonical/historical labels

Verify the checkouts before changing code:
- RediSearch branch codex/tq-production-20260824
- VectorSimilarity commit 17d366571f637b1ed80612efc32bf0df3db627ea
- benchmark branch codex/msmarco-compressed-ann-20260825 at
  97bb4412e1b469891972d0732ca689c2dbad3a1b

The current contract is asymmetric raw-query scoring with packed Lloyd-Max indices,
packed QJL residual signs, FP32 alpha/gamma, mandatory rotation, and m=d. Historical
pairwise-polar/compressed-to-compressed notes are not authoritative. HNSW construction
uses the separate CoarseMse engineering metric.

Your immediate objective is to reproduce the eight-lane MS MARCO smoke matrix on
native AMD64, establish a trustworthy baseline, profile the VecSim kernels, and then
implement only optimizations justified by profiles. Preserve V1 persistence/numerical
behavior; create a V2 profile if serialized state or floating output changes. Measure
quality, recall/F1, latency, throughput, build time, memory, vector-index bytes, and
total index bytes. Keep TQ, matched raw HNSW, raw VAMANA, and VAMANA LVQ8 controls.

Follow AGENTS.md, use dedicated worktrees/branches, keep repositories clean, commit
reviewable units in dependency order, do not force-push protected history, and do not
publish the internal ZIP or prepared corpus.
```

## Handoff acceptance checklist

- [ ] Joo Bin can clone all three repositories using the commands above.
- [ ] RediSearch initializes VectorSimilarity at exactly `17d366571f637b1ed80612efc32bf0df3db627ea`.
- [ ] Joo Bin has received the private internal-context ZIP and verified its SHA-256.
- [ ] The 1,024-vector/32-query smoke matrix reproduces on native AMD64 before scaling.
- [ ] Native results clearly replace, rather than silently mix with, the emulated numbers.
- [ ] Any code continuation is on new reviewable branches with no history rewriting.
