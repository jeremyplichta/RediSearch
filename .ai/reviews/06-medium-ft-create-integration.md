# Medium: FT.CREATE Integration Review

**Priority: P2 - Medium**
**Category: Integration/Correctness**
**Files:** `src/spec.c`

## Integration Design

### 1. Key Index Scan Path - WELL DESIGNED ✓

The `Indexes_TryScanWithKeyIndex()` function (lines 2856-2988) implements a clean integration:

```
FT.CREATE
    ↓
Indexes_TryScanWithKeyIndex()
    ↓ (if DONE)         ↓ (if FALLBACK)      ↓ (if CANCELLED)
Complete             → RedisModule_Scan()  → Abort
```

### 2. Fallback Accounting - CORRECTLY HANDLED ✓

```c
if (iterRc != REDISMODULE_OK) {
  *fallbackReason = "iterator failure";
  // KeyIndex_IterPrefix error paths may already account for this fallback.
  // Signal the caller to avoid a second increment for the same FT.CREATE attempt.
  *fallbackAlreadyRecorded = true;
  ...
}
```

The double-accounting prevention is correct and well-documented.

### 3. Deduplication for Overlapping Prefixes ✓

Lines 2933-2939:
```c
if (numPrefixes > 1) {
  dedupe = NewTrieMap();
  ...
}
```

When multiple prefixes are specified, a dedupe TrieMap prevents processing the same key multiple times.

### 4. Yield During Iteration - GOOD ✓

Lines 2804-2816:
```c
static int Indexes_KeyIndexMaybeYield(KeyIndexIterCtx *iterCtx) {
  iterCtx->keysSinceYield++;
  if (iterCtx->keysSinceYield < KEY_INDEX_ITER_YIELD_INTERVAL) {
    return REDISMODULE_OK;
  }
  
  iterCtx->keysSinceYield = 0;
  RedisModule_ThreadSafeContextUnlock(iterCtx->ctx);
  sched_yield();
  RedisModule_ThreadSafeContextLock(iterCtx->ctx);
  ...
}
```

Periodic yields prevent blocking the Redis main thread during large iterations.

### 5. CONCERN: Debug Mode Bypass

```c
if (globalDebugCtx.debugMode || scanner->isDebug) {
  *fallbackReason = "debug scanner mode";
  return KEY_INDEX_SCAN_RESULT_FALLBACK;
}
```

Debug mode always falls back to SCAN. This is intentional for testing purposes.

## Implementation Status

- **Triage outcome:** Acceptable.
- **Code change required:** No code change required.
- **Rationale:** FT.CREATE integration already includes robust fallback, dedupe, and yield safeguards.
