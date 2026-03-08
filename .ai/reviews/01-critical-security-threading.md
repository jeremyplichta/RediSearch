# Critical: Thread Safety and Race Conditions

**Priority: P0 - Critical**
**Category: Concurrency/Security**
**Files:** `src/key_index.c`

## Issue

The key index implementation uses a lock-free deferred queue with potential ABA problems and memory ordering concerns.

### 1. Potential ABA Problem in Deferred Queue (Line 384-390)

```c
KeyIndexOp *head = atomic_load_explicit(&g_keyIndex.deferredHead, memory_order_acquire);
do {
  op->next = head;
} while (!atomic_compare_exchange_weak_explicit(
    &g_keyIndex.deferredHead, &head, op, memory_order_acq_rel, memory_order_acquire));
```

While this pattern is generally correct for a lock-free push, the ABA problem isn't a concern here because nodes are never recycled - they're freed after being popped. **This is acceptable.**

### 2. Double-Check Pattern on `acceptsUpdates` (Lines 356-358, 361-365)

```c
if (!atomic_load_explicit(&g_keyIndex.acceptsUpdates, memory_order_acquire)) {
  KeyIndex_FreeOp(op);
  return;
}

if (pthread_mutex_trylock(&g_keyIndex.lock) != 0) {
  if (!atomic_load_explicit(&g_keyIndex.acceptsUpdates, memory_order_acquire)) {
    KeyIndex_FreeOp(op);
    return;
  }
```

The second check after `trylock` failure is correct - prevents enqueuing after disable starts. **Well designed.**

### 3. Lock Ordering with Atomic Operations

The design correctly uses:
- Lock-free deferred queue for notification path (low latency critical)
- Mutex-protected main queue for timer drain
- Atomic `acceptsUpdates` flag for fast rejection

**Assessment:** The threading model is sound. The separation of deferred (lock-free) and main (mutex-protected) queues is a good architectural choice.

## Recommendation

**No immediate action required** - the implementation is thread-safe. Consider adding documentation comments explaining the memory ordering choices for future maintainers.

## Status: ACCEPTABLE ✓

## Implementation Status

- Implemented: Added concise memory-ordering rationale comments in enqueue/deferred and timer-drain paths (`acceptsUpdates`, `deferredHead`, `deferredDepth`) in `src/key_index.c`.
- Current status: No behavioral locking changes were required; review recommendation to document atomic ordering is completed.
