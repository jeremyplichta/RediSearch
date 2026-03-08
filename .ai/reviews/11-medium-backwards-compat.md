# Medium: Backwards Compatibility Review

**Priority: P2 - Medium**
**Category: Compatibility**
**Files:** `src/module.c`, `src/config.c`, `src/notifications.c`

## Redis Version Compatibility

### 1. Internal Command Flag Fallback ✓

Lines 1390-1401 in `module.c`:
```c
if (createCmdRc == REDISMODULE_ERR && internalCommand && !IsEnterprise()) {
  // Redis OSS 7.4.x rejects the `internal` flag. Retry with base flags.
  createCmdRc = RedisModule_CreateCommand(ctx, name, handler, flags,
                                          position.firstkey, position.lastkey,
                                          position.keystep);
  if (createCmdRc == REDISMODULE_OK) {
    RedisModule_Log(ctx, "notice",
                    "Registered internal command %s without `%s` flag...");
  }
}
```

Gracefully handles older Redis versions that don't support `internal` command flag.

### 2. Unprefixed Config API Detection ✓

```c
#define RM_CONFIG_UNPREFIXED_API_FIX 0x00080000
#define RS_CONFIG_UNPREFIXED \
  (isFeatureSupported(RM_CONFIG_UNPREFIXED_API_FIX) ? REDISMODULE_CONFIG_UNPREFIXED : 0)
```

Runtime detection of Redis 8+ config API features.

### 3. LoadDefaultConfigs API Check ✓

Lines 4509-4521 in `module.c`:
```c
if (hasLoadDefaultConfigsApi) {
  RM_TRY_F(RedisModule_LoadDefaultConfigs, ctx);
} else {
  RedisModule_Log(ctx, "notice",
                  "Redis version does not expose RedisModule_LoadDefaultConfigs...");
  RM_TRY_F(RedisModule_LoadConfigs, ctx);
  loadConfigsAlreadyApplied = true;
}
```

Handles differences between Redis 7.x and 8.x config loading APIs.

### 4. Version Requirements Updated

```c
Version supportedVersion = {
    .majorVersion = 7,
    .minorVersion = 4,
    .patchVersion = 0,
};
```

Supports Redis 7.4.0+. This is appropriate given the API usage.

### 5. Command Filter Fallback ✓

Line 676 in `notifications.c`:
```c
if (RSGlobalConfig.filterCommands || !isFeatureSupported(RM_CONFIG_UNPREFIXED_API_FIX)) {
  RedisModule_RegisterCommandFilter(ctx, CommandFilterCallback, 0);
}
```

Always registers filter on older Redis for config alias rewriting.

## Implementation Status

- **Triage outcome:** Acceptable.
- **Code change required:** No code change required.
- **Rationale:** Compatibility guards already cover Redis 7.4+ and newer API differences.
