# High: Memory Safety and Resource Management

**Priority: P1 - High**
**Category: Memory Safety**
**Files:** `src/key_index.c`, `src/spec.c`

## Findings

### 1. Snapshot Memory Bounds Checking - GOOD ✓

Lines 108-111 in `key_index.c`:
```c
if (newCapacity > SIZE_MAX / sizeof(*snapshot->keyOffsets) ||
    newCapacity > SIZE_MAX / sizeof(*snapshot->keyLens)) {
  return false;
}
```

Integer overflow protection is correctly implemented before allocation.

### 2. Blob Capacity Overflow Check - GOOD ✓

Lines 162-171:
```c
if (requiredBlobLen < snapshot->blobLen) {
  return false;  // Overflow check
}
```

Correctly detects size_t overflow when adding key lengths.

### 3. Partial Allocation Recovery (Lines 117-122) - ACCEPTABLE

```c
size_t *newLens = rm_realloc(snapshot->keyLens, newCapacity * sizeof(*newLens));
if (!newLens) {
  // Keep the successful resize for proper cleanup on the failure path.
  snapshot->keyOffsets = newOffsets;
  return false;
}
```

This is correct - the comment explains the partial allocation is intentionally preserved for cleanup by `KeyIndexSnapshot_Free()`.

### 4. Key Length Validation (Line 78) - GOOD ✓

```c
if (len > UINT16_MAX) {
  return false;
}
```

Keys longer than 65535 bytes are rejected, preventing potential buffer issues with `tm_len_t`.

### 5. CONCERN: Missing Null Check After `rm_calloc` (Line 593)

```c
KeyIndexOp *op = rm_calloc(1, sizeof(*op));
if (!op) {
  return;  // Silent failure
}
```

Silent failure on OOM. While defensive, consider incrementing a dropped counter for observability.

### 6. CONCERN: Snapshot Peak Bytes Tracking

The `snapshotPeakBytes` statistic only updates on success. Consider tracking attempted allocations for memory pressure diagnostics.

## Recommendations

1. Consider adding `droppedUpdates++` when allocation fails in Queue functions
2. Add comment documenting expected failure mode for OOM scenarios

## Status: ACCEPTABLE WITH MINOR SUGGESTIONS

## Implementation Status

- Implemented: OOM observability for op allocation failures in `KeyIndex_QueueUpsert`, `KeyIndex_QueueRemove`, and `KeyIndex_QueueRename`.
- Details: Added a synchronized helper in `src/key_index.c` that increments `stats.droppedUpdates` under `g_keyIndex.lock` when `rm_calloc` fails.
- Current status: Allocation-failure paths are now measurable instead of silent.
