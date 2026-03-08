# Information: Architecture Summary

**Priority: Informational**
**Category: Architecture Documentation**

## Feature Summary

The commit `77b11b36e` adds an **optional in-memory key index** using a TrieMap for accelerating `FT.CREATE` prefix-based key discovery.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                   Key Index Service                      │
├─────────────────────────────────────────────────────────┤
│  State Machine: OFF ─► WARMING ─► READY ─► (ERROR)      │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐     ┌─────────────┐     ┌───────────┐  │
│  │ Deferred Q  │ ──► │   Main Q    │ ──► │  TrieMap  │  │
│  │ (lock-free) │     │ (mutex)     │     │  (keys)   │  │
│  └─────────────┘     └─────────────┘     └───────────┘  │
├─────────────────────────────────────────────────────────┤
│  Timer (5ms): Drains queue, advances warmup scan        │
└─────────────────────────────────────────────────────────┘
          ▲                                    │
          │                                    ▼
    Keyspace Events                    FT.CREATE Integration
    (notifications.c)                     (spec.c)
```

## Key Design Decisions

1. **Lock-free notification path**: Keyspace events use atomic CAS to enqueue without blocking
2. **Snapshot-based iteration**: Lock held briefly to copy keys, released before callbacks
3. **Graceful fallback**: Any non-ready state falls back to full Redis SCAN
4. **Queue overflow protection**: >500K pending ops triggers rebuild instead of data loss

## Performance Characteristics

- **Warmup**: Full keyspace SCAN on enable
- **Steady-state**: O(log n) updates via TrieMap
- **Query**: O(m) prefix iteration where m = matching keys
- **Memory**: Approximately 50-100 bytes per key + TrieMap overhead

## Files Changed

| File | Lines | Purpose |
|------|-------|---------|
| `src/key_index.c` | +883 | Core implementation |
| `src/key_index.h` | +61 | Public API |
| `src/spec.c` | +238 | FT.CREATE integration |
| `src/notifications.c` | +127/-80 | Event wiring |
| `src/config.c` | +75/-69 | Config registration |
| `src/module.c` | +56/-6 | Lifecycle management |
| Tests | +400 | Feature + regression tests |

## Implementation Status

- **Triage outcome:** Informational/acceptable.
- **Code change required:** No code change required.
- **Rationale:** This document summarizes intended architecture; triage found no actionable implementation defects.
