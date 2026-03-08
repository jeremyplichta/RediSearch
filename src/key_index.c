/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "key_index.h"

#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <string.h>
#include <time.h>

#include "triemap.h"
#include "rmalloc.h"

extern RedisModuleCtx *RSDummyContext;

#define KEY_INDEX_TIMER_PERIOD_MS 5
#define KEY_INDEX_QUEUE_BATCH_SIZE 4096
#define KEY_INDEX_QUEUE_BATCH_SIZE_WARMING_HIGH 32768
#define KEY_INDEX_SCAN_STEPS_PER_TICK 32
#define KEY_INDEX_SCAN_STEPS_IDLE_PER_TICK 2048
#define KEY_INDEX_QUEUE_MAX_DEPTH 500000
#define KEY_INDEX_DRAIN_BUDGET_READY_NS (2ULL * 1000ULL * 1000ULL)
#define KEY_INDEX_DRAIN_BUDGET_WARMING_NS (12ULL * 1000ULL * 1000ULL)
#define KEY_INDEX_SNAPSHOT_PRERESERVE_MAX_ENTRIES 1000000

typedef enum {
  KEY_INDEX_OP_UPSERT = 0,
  KEY_INDEX_OP_REMOVE = 1,
  KEY_INDEX_OP_RENAME = 2,
} KeyIndexOpType;

typedef struct KeyIndexOp {
  KeyIndexOpType type;
  char *key;
  size_t keyLen;
  char *renameTo;
  size_t renameToLen;
  struct KeyIndexOp *next;
} KeyIndexOp;

typedef struct {
  bool initialized;
  bool enabled;
  bool timerScheduled;
  RedisModuleTimerID timerId;
  KeyIndexState state;
  TrieMap *keys;
  KeyIndexOp *queueHead;
  KeyIndexOp *queueTail;
  size_t queueDepth;
  _Atomic(KeyIndexOp *) deferredHead;
  _Atomic size_t deferredDepth;
  _Atomic bool acceptsUpdates;
  RedisModuleScanCursor *scanCursor;
  uint64_t bootstrapStartNs;
  KeyIndexStats stats;
  bool debugFailNextIter;
  pthread_mutex_t lock;
} KeyIndexService;

static KeyIndexService g_keyIndex = {
  .state = KEY_INDEX_STATE_OFF,
};

static uint64_t KeyIndex_NowNs(void) {
  struct timespec ts = {0};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static bool KeyIndex_ToTmLen(size_t len, tm_len_t *outLen) {
  if (len > UINT16_MAX) {
    return false;
  }
  *outLen = (tm_len_t)len;
  return true;
}

typedef struct {
  size_t *keyOffsets;
  size_t *keyLens;
  size_t count;
  size_t capacity;
  char *blob;
  size_t blobLen;
  size_t blobCapacity;
  uint64_t bytes;
} KeyIndexSnapshot;

static void KeyIndexSnapshot_Init(KeyIndexSnapshot *snapshot) {
  if (!snapshot) {
    return;
  }
  memset(snapshot, 0, sizeof(*snapshot));
}

static bool KeyIndexSnapshot_ReserveEntries(KeyIndexSnapshot *snapshot, size_t newCapacity) {
  if (!snapshot || newCapacity <= snapshot->capacity) {
    return true;
  }

  if (newCapacity > SIZE_MAX / sizeof(*snapshot->keyOffsets) ||
      newCapacity > SIZE_MAX / sizeof(*snapshot->keyLens)) {
    return false;
  }

  size_t *newOffsets = rm_realloc(snapshot->keyOffsets, newCapacity * sizeof(*newOffsets));
  if (!newOffsets) {
    return false;
  }
  size_t *newLens = rm_realloc(snapshot->keyLens, newCapacity * sizeof(*newLens));
  if (!newLens) {
    // Keep the successful resize for proper cleanup on the failure path.
    snapshot->keyOffsets = newOffsets;
    return false;
  }
  snapshot->keyOffsets = newOffsets;
  snapshot->keyLens = newLens;

  size_t oldCapacity = snapshot->capacity;
  snapshot->capacity = newCapacity;
  uint64_t capacityBytesDelta = (uint64_t)(newCapacity - oldCapacity) *
                                (uint64_t)(sizeof(*snapshot->keyOffsets) + sizeof(*snapshot->keyLens));
  if (UINT64_MAX - snapshot->bytes < capacityBytesDelta) {
    snapshot->bytes = UINT64_MAX;
  } else {
    snapshot->bytes += capacityBytesDelta;
  }
  return true;
}

static bool KeyIndexSnapshot_ReserveBlob(KeyIndexSnapshot *snapshot, size_t newCapacity) {
  if (!snapshot || newCapacity <= snapshot->blobCapacity) {
    return true;
  }

  char *newBlob = rm_realloc(snapshot->blob, newCapacity);
  if (!newBlob) {
    return false;
  }
  snapshot->blob = newBlob;

  uint64_t delta = (uint64_t)(newCapacity - snapshot->blobCapacity);
  snapshot->blobCapacity = newCapacity;
  if (UINT64_MAX - snapshot->bytes < delta) {
    snapshot->bytes = UINT64_MAX;
  } else {
    snapshot->bytes += delta;
  }
  return true;
}

static bool KeyIndexSnapshot_Add(KeyIndexSnapshot *snapshot, const char *key, size_t keyLen) {
  if (!snapshot || !key) {
    return false;
  }

  if (snapshot->count == snapshot->capacity) {
    size_t newCapacity = snapshot->capacity ? snapshot->capacity * 2 : 1024;
    if (newCapacity < snapshot->capacity) {
      return false;
    }
    if (!KeyIndexSnapshot_ReserveEntries(snapshot, newCapacity)) {
      return false;
    }
  }

  size_t requiredBlobLen = snapshot->blobLen + keyLen;
  if (requiredBlobLen < snapshot->blobLen) {
    return false;
  }

  if (requiredBlobLen > snapshot->blobCapacity) {
    size_t newBlobCapacity = snapshot->blobCapacity ? snapshot->blobCapacity : 4096;
    while (newBlobCapacity < requiredBlobLen) {
      size_t nextCapacity = newBlobCapacity * 2;
      if (nextCapacity <= newBlobCapacity) {
        newBlobCapacity = requiredBlobLen;
        break;
      }
      newBlobCapacity = nextCapacity;
    }
    if (!KeyIndexSnapshot_ReserveBlob(snapshot, newBlobCapacity)) {
      return false;
    }
  }

  size_t keyOffset = snapshot->blobLen;
  if (keyLen > 0) {
    memcpy(snapshot->blob + keyOffset, key, keyLen);
  }
  snapshot->keyOffsets[snapshot->count] = keyOffset;
  snapshot->keyLens[snapshot->count] = keyLen;
  snapshot->blobLen = requiredBlobLen;
  snapshot->count++;
  return true;
}

static void KeyIndexSnapshot_Free(KeyIndexSnapshot *snapshot) {
  if (!snapshot) {
    return;
  }
  rm_free(snapshot->keyOffsets);
  rm_free(snapshot->keyLens);
  rm_free(snapshot->blob);
  memset(snapshot, 0, sizeof(*snapshot));
}

static void KeyIndex_FreeOp(KeyIndexOp *op) {
  if (!op) {
    return;
  }
  rm_free(op->key);
  rm_free(op->renameTo);
  rm_free(op);
}

static size_t KeyIndex_FreeOpList(KeyIndexOp *head) {
  size_t count = 0;
  while (head) {
    KeyIndexOp *next = head->next;
    KeyIndex_FreeOp(head);
    head = next;
    ++count;
  }
  return count;
}

static size_t KeyIndex_ClearQueueLocked(void) {
  KeyIndexOp *op = g_keyIndex.queueHead;
  size_t cleared = KeyIndex_FreeOpList(op);
  g_keyIndex.queueHead = NULL;
  g_keyIndex.queueTail = NULL;
  g_keyIndex.queueDepth = 0;
  return cleared;
}

static size_t KeyIndex_ClearDeferredLocked(void) {
  KeyIndexOp *op = atomic_exchange_explicit(&g_keyIndex.deferredHead, NULL, memory_order_acq_rel);
  atomic_store_explicit(&g_keyIndex.deferredDepth, 0, memory_order_release);
  return KeyIndex_FreeOpList(op);
}

static void KeyIndex_DestroyScanCursorLocked(void) {
  if (g_keyIndex.scanCursor) {
    RedisModule_ScanCursorDestroy(g_keyIndex.scanCursor);
    g_keyIndex.scanCursor = NULL;
  }
}

static void KeyIndex_FreeTrieLocked(void) {
  if (g_keyIndex.keys) {
    TrieMap_Free(g_keyIndex.keys, NULL);
    g_keyIndex.keys = NULL;
  }
}

static void KeyIndex_StopTimerLocked(void) {
  if (g_keyIndex.timerScheduled && RSDummyContext) {
    RedisModule_StopTimer(RSDummyContext, g_keyIndex.timerId, NULL);
  }
  g_keyIndex.timerScheduled = false;
  g_keyIndex.timerId = 0;
}

static void KeyIndex_TimerCallback(RedisModuleCtx *ctx, void *privdata);
static void KeyIndex_RestartWarmupLocked(bool countDroppedQueue);

static void KeyIndex_ScheduleTimerLocked(void) {
  if (!g_keyIndex.initialized || !g_keyIndex.enabled || g_keyIndex.timerScheduled || !RSDummyContext) {
    return;
  }
  g_keyIndex.timerId = RedisModule_CreateTimer(RSDummyContext, KEY_INDEX_TIMER_PERIOD_MS,
                                               KeyIndex_TimerCallback, NULL);
  g_keyIndex.timerScheduled = true;
}

static void KeyIndex_ResetTrieForWarmupLocked(void) {
  KeyIndex_FreeTrieLocked();
  g_keyIndex.keys = NewTrieMap();
  if (!g_keyIndex.keys) {
    g_keyIndex.state = KEY_INDEX_STATE_ERROR;
    return;
  }
  KeyIndex_DestroyScanCursorLocked();
  g_keyIndex.scanCursor = RedisModule_ScanCursorCreate();
  if (!g_keyIndex.scanCursor) {
    g_keyIndex.state = KEY_INDEX_STATE_ERROR;
    return;
  }
  g_keyIndex.state = KEY_INDEX_STATE_WARMING;
  g_keyIndex.bootstrapStartNs = KeyIndex_NowNs();
  g_keyIndex.stats.bootstrapDurationMs = 0;
}

static void KeyIndex_ApplyUpsertLocked(const char *key, size_t keyLen) {
  tm_len_t tmLen = 0;
  if (!g_keyIndex.keys || !KeyIndex_ToTmLen(keyLen, &tmLen)) {
    g_keyIndex.stats.droppedUpdates++;
    return;
  }
  TrieMap_Add(g_keyIndex.keys, key, tmLen, NULL, NULL);
  g_keyIndex.stats.appliedUpdates++;
}

static void KeyIndex_ApplyRemoveLocked(const char *key, size_t keyLen) {
  tm_len_t tmLen = 0;
  if (!g_keyIndex.keys || !KeyIndex_ToTmLen(keyLen, &tmLen)) {
    g_keyIndex.stats.droppedUpdates++;
    return;
  }
  TrieMap_Delete(g_keyIndex.keys, key, tmLen, NULL);
  g_keyIndex.stats.appliedUpdates++;
}

static void KeyIndex_ApplyRenameLocked(const char *from, size_t fromLen,
                                       const char *to, size_t toLen) {
  if (from && fromLen) {
    KeyIndex_ApplyRemoveLocked(from, fromLen);
  }
  if (to && toLen) {
    KeyIndex_ApplyUpsertLocked(to, toLen);
  }
}

static void KeyIndex_DrainQueueLocked(size_t maxOps) {
  uint64_t drainStartNs = KeyIndex_NowNs();
  uint64_t drainBudgetNs = g_keyIndex.state == KEY_INDEX_STATE_READY
                               ? KEY_INDEX_DRAIN_BUDGET_READY_NS
                               : KEY_INDEX_DRAIN_BUDGET_WARMING_NS;
  KeyIndexOp *deferred = atomic_exchange_explicit(&g_keyIndex.deferredHead, NULL, memory_order_acq_rel);
  size_t deferredDepth = atomic_exchange_explicit(&g_keyIndex.deferredDepth, 0, memory_order_acq_rel);
  if (deferred) {
    KeyIndexOp *orderedHead = NULL;
    KeyIndexOp *orderedTail = NULL;
    size_t actualDeferredDepth = 0;

    while (deferred) {
      KeyIndexOp *next = deferred->next;
      deferred->next = orderedHead;
      orderedHead = deferred;
      if (!orderedTail) {
        orderedTail = deferred;
      }
      deferred = next;
      ++actualDeferredDepth;
    }

    if (actualDeferredDepth > deferredDepth) {
      deferredDepth = actualDeferredDepth;
    }

    if (g_keyIndex.queueDepth + deferredDepth > KEY_INDEX_QUEUE_MAX_DEPTH) {
      // Overflow policy: drop queued/deferred updates and force warmup rebuild.
      g_keyIndex.stats.droppedUpdates += KeyIndex_FreeOpList(orderedHead);
      KeyIndex_RestartWarmupLocked(true);
    } else if (orderedHead) {
      if (!g_keyIndex.queueTail) {
        g_keyIndex.queueHead = orderedHead;
      } else {
        g_keyIndex.queueTail->next = orderedHead;
      }
      g_keyIndex.queueTail = orderedTail;
      g_keyIndex.queueDepth += actualDeferredDepth;
      g_keyIndex.stats.queuedUpdates += actualDeferredDepth;
    }
  }

  for (size_t i = 0; i < maxOps; ++i) {
    if (i > 0 && KeyIndex_NowNs() - drainStartNs >= drainBudgetNs) {
      break;
    }

    KeyIndexOp *op = g_keyIndex.queueHead;
    if (!op) {
      break;
    }

    g_keyIndex.queueHead = op->next;
    if (!g_keyIndex.queueHead) {
      g_keyIndex.queueTail = NULL;
    }
    g_keyIndex.queueDepth--;

    switch (op->type) {
      case KEY_INDEX_OP_UPSERT:
        KeyIndex_ApplyUpsertLocked(op->key, op->keyLen);
        break;
      case KEY_INDEX_OP_REMOVE:
        KeyIndex_ApplyRemoveLocked(op->key, op->keyLen);
        break;
      case KEY_INDEX_OP_RENAME:
        KeyIndex_ApplyRenameLocked(op->key, op->keyLen, op->renameTo, op->renameToLen);
        break;
      default:
        g_keyIndex.stats.droppedUpdates++;
        break;
    }
    KeyIndex_FreeOp(op);
  }

  if (g_keyIndex.state == KEY_INDEX_STATE_WARMING &&
      g_keyIndex.scanCursor == NULL &&
      g_keyIndex.queueDepth == 0 &&
      atomic_load_explicit(&g_keyIndex.deferredDepth, memory_order_acquire) == 0) {
    g_keyIndex.state = KEY_INDEX_STATE_READY;
  }
}

static void KeyIndex_RestartWarmupLocked(bool countDroppedQueue) {
  size_t dropped = KeyIndex_ClearQueueLocked();
  dropped += KeyIndex_ClearDeferredLocked();
  if (countDroppedQueue) {
    g_keyIndex.stats.droppedUpdates += dropped;
  }
  KeyIndex_ResetTrieForWarmupLocked();
  if (g_keyIndex.state != KEY_INDEX_STATE_ERROR) {
    KeyIndex_ScheduleTimerLocked();
  }
}

static bool KeyIndex_DupRedisKey(RedisModuleString *key, char **dst, size_t *dstLen) {
  if (!key || !dst || !dstLen) {
    return false;
  }
  size_t len = 0;
  const char *keyPtr = RedisModule_StringPtrLen(key, &len);
  if (!keyPtr) {
    return false;
  }
  char *copy = rm_malloc(len + 1);
  if (!copy) {
    return false;
  }
  memcpy(copy, keyPtr, len);
  copy[len] = '\0';
  *dst = copy;
  *dstLen = len;
  return true;
}

static void KeyIndex_EnqueueOp(KeyIndexOp *op) {
  if (!op) {
    return;
  }

  if (!atomic_load_explicit(&g_keyIndex.acceptsUpdates, memory_order_acquire)) {
    KeyIndex_FreeOp(op);
    return;
  }

  if (pthread_mutex_trylock(&g_keyIndex.lock) != 0) {
    if (!atomic_load_explicit(&g_keyIndex.acceptsUpdates, memory_order_acquire)) {
      KeyIndex_FreeOp(op);
      return;
    }

    KeyIndexOp *head = atomic_load_explicit(&g_keyIndex.deferredHead, memory_order_acquire);
    do {
      op->next = head;
    } while (!atomic_compare_exchange_weak_explicit(
        &g_keyIndex.deferredHead, &head, op, memory_order_acq_rel, memory_order_acquire));
    atomic_fetch_add_explicit(&g_keyIndex.deferredDepth, 1, memory_order_acq_rel);
    return;
  }

  if (!g_keyIndex.initialized || !g_keyIndex.enabled || g_keyIndex.state == KEY_INDEX_STATE_OFF) {
    pthread_mutex_unlock(&g_keyIndex.lock);
    KeyIndex_FreeOp(op);
    return;
  }

  if (g_keyIndex.queueDepth >= KEY_INDEX_QUEUE_MAX_DEPTH) {
    // Overflow policy: drop all queued updates, mark index non-ready, and
    // restart warmup. Integration must fallback until state becomes ready.
    g_keyIndex.stats.droppedUpdates++;
    KeyIndex_FreeOp(op);
    KeyIndex_RestartWarmupLocked(true);
    pthread_mutex_unlock(&g_keyIndex.lock);
    return;
  }

  op->next = NULL;
  if (!g_keyIndex.queueTail) {
    g_keyIndex.queueHead = g_keyIndex.queueTail = op;
  } else {
    g_keyIndex.queueTail->next = op;
    g_keyIndex.queueTail = op;
  }
  g_keyIndex.queueDepth++;
  g_keyIndex.stats.queuedUpdates++;
  KeyIndex_ScheduleTimerLocked();
  pthread_mutex_unlock(&g_keyIndex.lock);
}

static void KeyIndex_WarmupScanCallback(RedisModuleCtx *ctx, RedisModuleString *keyname,
                                        RedisModuleKey *key, void *privdata) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(key);
  REDISMODULE_NOT_USED(privdata);

  size_t keyLen = 0;
  const char *keyPtr = RedisModule_StringPtrLen(keyname, &keyLen);
  tm_len_t tmLen = 0;
  if (!keyPtr || !KeyIndex_ToTmLen(keyLen, &tmLen)) {
    return;
  }

  // Called while g_keyIndex.lock is already held by KeyIndex_RunWarmupStep().
  if (g_keyIndex.keys && g_keyIndex.enabled && g_keyIndex.state == KEY_INDEX_STATE_WARMING) {
    TrieMap_Add(g_keyIndex.keys, keyPtr, tmLen, NULL, NULL);
  }
}

static void KeyIndex_RunWarmupStep(RedisModuleCtx *ctx) {
  size_t scanStepsPerTick = KEY_INDEX_SCAN_STEPS_PER_TICK;

  pthread_mutex_lock(&g_keyIndex.lock);
  if (!g_keyIndex.initialized || !g_keyIndex.enabled ||
      g_keyIndex.state != KEY_INDEX_STATE_WARMING || !g_keyIndex.scanCursor) {
    pthread_mutex_unlock(&g_keyIndex.lock);
    return;
  }
  if (g_keyIndex.queueDepth == 0 &&
      atomic_load_explicit(&g_keyIndex.deferredDepth, memory_order_acquire) == 0) {
    scanStepsPerTick = KEY_INDEX_SCAN_STEPS_IDLE_PER_TICK;
  }

  int done = 0;
  for (size_t i = 0; i < scanStepsPerTick; ++i) {
    if (RedisModule_Scan(ctx, g_keyIndex.scanCursor, KeyIndex_WarmupScanCallback, NULL) == 0) {
      done = 1;
      break;
    }
  }

  if (!done) {
    pthread_mutex_unlock(&g_keyIndex.lock);
    return;
  }

  if (g_keyIndex.scanCursor) {
    RedisModule_ScanCursorDestroy(g_keyIndex.scanCursor);
    g_keyIndex.scanCursor = NULL;
    uint64_t nowNs = KeyIndex_NowNs();
    if (nowNs >= g_keyIndex.bootstrapStartNs) {
      g_keyIndex.stats.bootstrapDurationMs = (nowNs - g_keyIndex.bootstrapStartNs) / 1000000ULL;
    }
    if (g_keyIndex.queueDepth == 0 &&
        atomic_load_explicit(&g_keyIndex.deferredDepth, memory_order_acquire) == 0) {
      g_keyIndex.state = KEY_INDEX_STATE_READY;
    }
  }
  pthread_mutex_unlock(&g_keyIndex.lock);
}

static void KeyIndex_TimerCallback(RedisModuleCtx *ctx, void *privdata) {
  REDISMODULE_NOT_USED(privdata);

  pthread_mutex_lock(&g_keyIndex.lock);
  g_keyIndex.timerScheduled = false;

  if (!g_keyIndex.initialized || !g_keyIndex.enabled || g_keyIndex.state == KEY_INDEX_STATE_OFF) {
    pthread_mutex_unlock(&g_keyIndex.lock);
    return;
  }

  size_t drainBatchSize = KEY_INDEX_QUEUE_BATCH_SIZE;
  if (g_keyIndex.state == KEY_INDEX_STATE_WARMING &&
      g_keyIndex.queueDepth >= KEY_INDEX_QUEUE_BATCH_SIZE_WARMING_HIGH) {
    drainBatchSize = KEY_INDEX_QUEUE_BATCH_SIZE_WARMING_HIGH;
  }
  KeyIndex_DrainQueueLocked(drainBatchSize);
  pthread_mutex_unlock(&g_keyIndex.lock);

  KeyIndex_RunWarmupStep(ctx);

  pthread_mutex_lock(&g_keyIndex.lock);
  if (g_keyIndex.enabled &&
      (g_keyIndex.queueDepth > 0 ||
       atomic_load_explicit(&g_keyIndex.deferredDepth, memory_order_acquire) > 0 ||
       g_keyIndex.state == KEY_INDEX_STATE_WARMING)) {
    KeyIndex_ScheduleTimerLocked();
  }
  pthread_mutex_unlock(&g_keyIndex.lock);
}

static void KeyIndex_EnableLocked(void) {
  if (g_keyIndex.enabled && g_keyIndex.state != KEY_INDEX_STATE_OFF) {
    return;
  }
  g_keyIndex.enabled = true;
  atomic_store_explicit(&g_keyIndex.acceptsUpdates, true, memory_order_release);
  KeyIndex_ClearQueueLocked();
  KeyIndex_ClearDeferredLocked();
  KeyIndex_ResetTrieForWarmupLocked();
  if (g_keyIndex.state != KEY_INDEX_STATE_ERROR) {
    KeyIndex_ScheduleTimerLocked();
  }
}

static void KeyIndex_DisableLocked(void) {
  atomic_store_explicit(&g_keyIndex.acceptsUpdates, false, memory_order_release);
  g_keyIndex.enabled = false;
  g_keyIndex.state = KEY_INDEX_STATE_OFF;
  KeyIndex_StopTimerLocked();
  KeyIndex_DestroyScanCursorLocked();
  KeyIndex_ClearQueueLocked();
  KeyIndex_ClearDeferredLocked();
  KeyIndex_FreeTrieLocked();
}

int KeyIndex_Init(void) {
  if (g_keyIndex.initialized) {
    return REDISMODULE_OK;
  }

  if (pthread_mutex_init(&g_keyIndex.lock, NULL) != 0) {
    g_keyIndex.state = KEY_INDEX_STATE_ERROR;
    return REDISMODULE_ERR;
  }

  g_keyIndex.initialized = true;
  atomic_store_explicit(&g_keyIndex.deferredHead, NULL, memory_order_release);
  atomic_store_explicit(&g_keyIndex.deferredDepth, 0, memory_order_release);
  atomic_store_explicit(&g_keyIndex.acceptsUpdates, false, memory_order_release);
  g_keyIndex.state = KEY_INDEX_STATE_OFF;
  if (g_keyIndex.enabled) {
    pthread_mutex_lock(&g_keyIndex.lock);
    KeyIndex_EnableLocked();
    pthread_mutex_unlock(&g_keyIndex.lock);
  }

  return REDISMODULE_OK;
}

void KeyIndex_Shutdown(void) {
  if (!g_keyIndex.initialized) {
    g_keyIndex.enabled = false;
    g_keyIndex.state = KEY_INDEX_STATE_OFF;
    atomic_store_explicit(&g_keyIndex.acceptsUpdates, false, memory_order_release);
    KeyIndex_FreeOpList(atomic_exchange_explicit(&g_keyIndex.deferredHead, NULL, memory_order_acq_rel));
    atomic_store_explicit(&g_keyIndex.deferredDepth, 0, memory_order_release);
    return;
  }

  pthread_mutex_lock(&g_keyIndex.lock);
  KeyIndex_DisableLocked();
  g_keyIndex.initialized = false;
  pthread_mutex_unlock(&g_keyIndex.lock);
  pthread_mutex_destroy(&g_keyIndex.lock);
}

void KeyIndex_SetEnabled(bool enabled) {
  if (!g_keyIndex.initialized) {
    g_keyIndex.enabled = enabled;
    return;
  }

  pthread_mutex_lock(&g_keyIndex.lock);
  if (enabled) {
    KeyIndex_EnableLocked();
  } else {
    KeyIndex_DisableLocked();
  }
  pthread_mutex_unlock(&g_keyIndex.lock);
}

bool KeyIndex_IsEnabled(void) {
  bool enabled = false;
  if (!g_keyIndex.initialized) {
    return g_keyIndex.enabled;
  }
  pthread_mutex_lock(&g_keyIndex.lock);
  enabled = g_keyIndex.enabled;
  pthread_mutex_unlock(&g_keyIndex.lock);
  return enabled;
}

KeyIndexState KeyIndex_GetState(void) {
  KeyIndexState state = KEY_INDEX_STATE_OFF;
  if (!g_keyIndex.initialized) {
    return KEY_INDEX_STATE_OFF;
  }
  pthread_mutex_lock(&g_keyIndex.lock);
  state = g_keyIndex.state;
  pthread_mutex_unlock(&g_keyIndex.lock);
  return state;
}

const char *KeyIndex_StateToString(KeyIndexState state) {
  switch (state) {
    case KEY_INDEX_STATE_OFF:
      return "off";
    case KEY_INDEX_STATE_WARMING:
      return "warming";
    case KEY_INDEX_STATE_READY:
      return "ready";
    case KEY_INDEX_STATE_ERROR:
      return "error";
  }
  return "unknown";
}

void KeyIndex_QueueUpsert(RedisModuleString *key) {
  KeyIndexOp *op = rm_calloc(1, sizeof(*op));
  if (!op) {
    return;
  }
  if (!KeyIndex_DupRedisKey(key, &op->key, &op->keyLen)) {
    KeyIndex_FreeOp(op);
    return;
  }
  op->type = KEY_INDEX_OP_UPSERT;
  KeyIndex_EnqueueOp(op);
}

void KeyIndex_QueueRemove(RedisModuleString *key) {
  KeyIndexOp *op = rm_calloc(1, sizeof(*op));
  if (!op) {
    return;
  }
  if (!KeyIndex_DupRedisKey(key, &op->key, &op->keyLen)) {
    KeyIndex_FreeOp(op);
    return;
  }
  op->type = KEY_INDEX_OP_REMOVE;
  KeyIndex_EnqueueOp(op);
}

void KeyIndex_QueueRename(RedisModuleString *from, RedisModuleString *to) {
  if (!to) {
    return;
  }
  KeyIndexOp *op = rm_calloc(1, sizeof(*op));
  if (!op) {
    return;
  }
  op->type = KEY_INDEX_OP_RENAME;

  if (!KeyIndex_DupRedisKey(to, &op->renameTo, &op->renameToLen)) {
    KeyIndex_FreeOp(op);
    return;
  }

  if (from) {
    (void)KeyIndex_DupRedisKey(from, &op->key, &op->keyLen);
  }
  KeyIndex_EnqueueOp(op);
}

int KeyIndex_IterPrefix(const char *prefix, size_t len, KeyIndexIterCb cb, void *ctx) {
  if (!cb || (!prefix && len > 0)) {
    return REDISMODULE_ERR;
  }

  tm_len_t tmPrefixLen = 0;
  if (!KeyIndex_ToTmLen(len, &tmPrefixLen)) {
    return REDISMODULE_ERR;
  }

  KeyIndexSnapshot snapshot;
  KeyIndexSnapshot_Init(&snapshot);
  bool snapshotFailed = false;

  pthread_mutex_lock(&g_keyIndex.lock);
  if (!g_keyIndex.initialized || !g_keyIndex.enabled ||
      g_keyIndex.state != KEY_INDEX_STATE_READY || !g_keyIndex.keys) {
    g_keyIndex.stats.fallbackCount++;
    pthread_mutex_unlock(&g_keyIndex.lock);
    return REDISMODULE_ERR;
  }

  if (g_keyIndex.debugFailNextIter) {
    g_keyIndex.debugFailNextIter = false;
    g_keyIndex.stats.fallbackCount++;
    pthread_mutex_unlock(&g_keyIndex.lock);
    return REDISMODULE_ERR;
  }

  uintptr_t trieEntryCount = TrieMap_NUniqueKeys(g_keyIndex.keys);
  if (trieEntryCount > 0) {
    size_t reserveEntries = trieEntryCount > KEY_INDEX_SNAPSHOT_PRERESERVE_MAX_ENTRIES
                                ? KEY_INDEX_SNAPSHOT_PRERESERVE_MAX_ENTRIES
                                : (size_t)trieEntryCount;
    if (!KeyIndexSnapshot_ReserveEntries(&snapshot, reserveEntries)) {
      g_keyIndex.stats.fallbackCount++;
      pthread_mutex_unlock(&g_keyIndex.lock);
      return REDISMODULE_ERR;
    }
  }

  TrieMapIterator *it = TrieMap_IterateWithFilter(g_keyIndex.keys, prefix, tmPrefixLen, TM_PREFIX_MODE);
  if (!it) {
    g_keyIndex.stats.fallbackCount++;
    pthread_mutex_unlock(&g_keyIndex.lock);
    return REDISMODULE_ERR;
  }

  char *key = NULL;
  tm_len_t keyLen = 0;
  void *value = NULL;
  while (TrieMapIterator_Next(it, &key, &keyLen, &value)) {
    if (!KeyIndexSnapshot_Add(&snapshot, key, keyLen)) {
      snapshotFailed = true;
      break;
    }
  }
  TrieMapIterator_Free(it);

  if (!snapshotFailed && snapshot.bytes > g_keyIndex.stats.snapshotPeakBytes) {
    g_keyIndex.stats.snapshotPeakBytes = snapshot.bytes;
  }
  if (snapshotFailed) {
    g_keyIndex.stats.fallbackCount++;
  }
  if (atomic_load_explicit(&g_keyIndex.deferredDepth, memory_order_acquire) > 0) {
    KeyIndex_ScheduleTimerLocked();
  }
  pthread_mutex_unlock(&g_keyIndex.lock);

  if (snapshotFailed) {
    KeyIndexSnapshot_Free(&snapshot);
    return REDISMODULE_ERR;
  }

  for (size_t i = 0; i < snapshot.count; ++i) {
    const char *key = snapshot.blob + snapshot.keyOffsets[i];
    if (cb(key, snapshot.keyLens[i], ctx) != REDISMODULE_OK) {
      break;
    }
  }
  KeyIndexSnapshot_Free(&snapshot);
  return REDISMODULE_OK;
}

void KeyIndex_RecordFallback(void) {
  if (!g_keyIndex.initialized) {
    return;
  }
  pthread_mutex_lock(&g_keyIndex.lock);
  g_keyIndex.stats.fallbackCount++;
  pthread_mutex_unlock(&g_keyIndex.lock);
}

void KeyIndex_GetStats(KeyIndexStats *stats) {
  if (!stats) {
    return;
  }
  memset(stats, 0, sizeof(*stats));
  if (!g_keyIndex.initialized) {
    stats->state = KEY_INDEX_STATE_OFF;
    return;
  }

  pthread_mutex_lock(&g_keyIndex.lock);
  stats->state = g_keyIndex.state;
  stats->numEntries = g_keyIndex.keys ? TrieMap_NUniqueKeys(g_keyIndex.keys) : 0;
  stats->queueDepth = g_keyIndex.queueDepth +
                      atomic_load_explicit(&g_keyIndex.deferredDepth, memory_order_acquire);
  stats->fallbackCount = g_keyIndex.stats.fallbackCount;
  stats->bootstrapDurationMs = g_keyIndex.stats.bootstrapDurationMs;
  stats->snapshotPeakBytes = g_keyIndex.stats.snapshotPeakBytes;
  stats->queuedUpdates = g_keyIndex.stats.queuedUpdates;
  stats->droppedUpdates = g_keyIndex.stats.droppedUpdates;
  stats->appliedUpdates = g_keyIndex.stats.appliedUpdates;
  pthread_mutex_unlock(&g_keyIndex.lock);
}

void KeyIndex_DebugTriggerNextIterError(void) {
  if (!g_keyIndex.initialized) {
    return;
  }
  pthread_mutex_lock(&g_keyIndex.lock);
  g_keyIndex.debugFailNextIter = true;
  pthread_mutex_unlock(&g_keyIndex.lock);
}
