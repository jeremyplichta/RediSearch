# Medium: API Design and Interface Review

**Priority: P2 - Medium**
**Category: Architecture**
**Files:** `src/key_index.h`, `src/key_index.c`

## Public API Review

### Exposed Functions

```c
int KeyIndex_Init(void);
void KeyIndex_Shutdown(void);
void KeyIndex_SetEnabled(bool enabled);
bool KeyIndex_IsEnabled(void);
KeyIndexState KeyIndex_GetState(void);
const char *KeyIndex_StateToString(KeyIndexState state);
void KeyIndex_QueueUpsert(RedisModuleString *key);
void KeyIndex_QueueRemove(RedisModuleString *key);
void KeyIndex_QueueRename(RedisModuleString *from, RedisModuleString *to);
int KeyIndex_IterPrefix(const char *prefix, size_t len, KeyIndexIterCb cb, void *ctx);
void KeyIndex_RecordFallback(void);
void KeyIndex_GetStats(KeyIndexStats *stats);
void KeyIndex_DebugTriggerNextIterError(void);
```

### 1. Good: Non-Reentrant Contract Documented ✓

Line 41-42 in header:
```c
// Callback must not call back into key-index APIs (non-reentrant contract).
typedef int (*KeyIndexIterCb)(const char *key, size_t keyLen, void *ctx);
```

### 2. Good: Clear Callback Return Semantics ✓

`KeyIndexIterCb` returns `REDISMODULE_OK` to continue, `REDISMODULE_ERR` to stop.

### 3. SUGGESTION: Debug Function Access Control

`KeyIndex_DebugTriggerNextIterError()` is exposed without debug checks. The actual protection is in `debug_commands.c` which checks `debugCommandsEnabled()`. This is acceptable but could use a comment.

### 4. CONCERN: Global State Pattern

The service uses a global `g_keyIndex` singleton. This is consistent with Redis module patterns but limits testability. Consider documenting why this approach was chosen.

### 5. Good: Stats Structure Completeness ✓

```c
typedef struct {
  KeyIndexState state;
  size_t numEntries;
  size_t queueDepth;
  uint64_t fallbackCount;
  uint64_t bootstrapDurationMs;
  uint64_t snapshotPeakBytes;
  uint64_t queuedUpdates;
  uint64_t droppedUpdates;
  uint64_t appliedUpdates;
} KeyIndexStats;
```

All important metrics are exposed for observability.

## Status: ACCEPTABLE ✓

## Implementation Status

- Implemented: Added API/header comments in `src/key_index.h` documenting the singleton service rationale.
- Implemented: Added API/header comment clarifying `KeyIndex_DebugTriggerNextIterError()` is debug-only and reachable through the FT.DEBUG gate in `debug_commands`.
- Current status: Interface intent and debug-path constraints are now explicitly documented in the public header.
