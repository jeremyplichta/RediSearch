# High: State Machine Correctness

**Priority: P1 - High**
**Category: Correctness**
**Files:** `src/key_index.c`

## State Transitions

The key index implements a 4-state machine:
```
OFF → WARMING → READY
         ↓
       ERROR
```

### 1. State Transitions Are Well-Guarded ✓

- `OFF → WARMING`: Only via `KeyIndex_EnableLocked()` (line 491)
- `WARMING → READY`: Only when scan completes AND queue is empty (lines 330-332, 449-451)
- `WARMING → ERROR`: On allocation failure during reset (lines 260-263)
- `READY → WARMING`: Via overflow restart (line 316) or manual disable/enable

### 2. CONCERN: No ERROR → OFF Recovery Path

Once in ERROR state, the only way out is `KeyIndex_DisableLocked()`. This is actually **correct behavior** - requires explicit administrator intervention.

### 3. Queue Overflow Handling - GOOD ✓

Lines 310-317:
```c
if (g_keyIndex.queueDepth + deferredDepth > KEY_INDEX_QUEUE_MAX_DEPTH) {
  // Overflow policy: drop queued/deferred updates and force warmup rebuild.
  g_keyIndex.stats.droppedUpdates += KeyIndex_FreeOpList(orderedHead);
  KeyIndex_RestartWarmupLocked(true);
}
```

Overflow triggers a full rebuild rather than data loss. This is the correct policy.

### 4. Fallback Path Correctness ✓

The integration in `spec.c` correctly falls back to SCAN when:
- State is not READY
- Iterator fails
- Global scan mode

### 5. Bootstrap Duration Tracking ✓

Lines 444-447:
```c
uint64_t nowNs = KeyIndex_NowNs();
if (nowNs >= g_keyIndex.bootstrapStartNs) {
  g_keyIndex.stats.bootstrapDurationMs = (nowNs - g_keyIndex.bootstrapStartNs) / 1000000ULL;
}
```

Handles clock monotonicity edge cases correctly.

## Implementation Status

- **Triage outcome:** Acceptable.
- **Code change required:** No code change required.
- **Rationale:** State transitions, overflow handling, and SCAN fallback behavior are intentional and correct.
