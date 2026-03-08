/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "redismodule.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  KEY_INDEX_STATE_OFF = 0,
  KEY_INDEX_STATE_WARMING = 1,
  KEY_INDEX_STATE_READY = 2,
  KEY_INDEX_STATE_ERROR = 3,
} KeyIndexState;

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

// Callback is invoked against a snapshot collected from the key index.
// Callback must not call back into key-index APIs (non-reentrant contract).
typedef int (*KeyIndexIterCb)(const char *key, size_t keyLen, void *ctx);

int KeyIndex_Init(void);
void KeyIndex_Shutdown(void);
void KeyIndex_SetEnabled(bool enabled);
bool KeyIndex_IsEnabled(void);
KeyIndexState KeyIndex_GetState(void);
const char *KeyIndex_StateToString(KeyIndexState state);
void KeyIndex_QueueUpsert(RedisModuleString *key);
void KeyIndex_QueueRemove(RedisModuleString *key);
void KeyIndex_QueueRename(RedisModuleString *from, RedisModuleString *to);
// Returns REDISMODULE_OK only when iteration runs while state is `ready`.
// Any non-ready state or iterator setup failure returns REDISMODULE_ERR.
int KeyIndex_IterPrefix(const char *prefix, size_t len, KeyIndexIterCb cb, void *ctx);
void KeyIndex_RecordFallback(void);
void KeyIndex_GetStats(KeyIndexStats *stats);
void KeyIndex_DebugTriggerNextIterError(void);

#ifdef __cplusplus
}
#endif
