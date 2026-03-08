# Medium: Configuration Integration Review

**Priority: P2 - Medium**
**Category: Configuration/Compatibility**
**Files:** `src/config.c`, `src/config.h`, `src/coord/config.c`, `src/notifications.c`

## Configuration Additions

### 1. Config Registration - GOOD ✓

Three configuration paths are correctly implemented:

1. **Redis CONFIG API** (`search-key-index`): Line 2261-2268
2. **FT.CONFIG API** (`KEY_INDEX`): Lines 1498-1502
3. **Module ARGS**: Handled via existing arg parsing

### 2. RS_CONFIG_UNPREFIXED Macro - CLEVER ✓

```c
#define RS_CONFIG_UNPREFIXED \
  (isFeatureSupported(RM_CONFIG_UNPREFIXED_API_FIX) ? REDISMODULE_CONFIG_UNPREFIXED : 0)
```

This pattern gracefully handles Redis 7.x vs 8.x API differences. Applied consistently across both `config.c` and `coord/config.c`.

### 3. Legacy Config Alias Rewriting - GOOD ✓

`RewriteLegacyConfigAliases()` in `notifications.c` handles older Redis versions that don't support unprefixed module configs:

```c
// search-key-index → search.search-key-index
```

### 4. CONCERN: Config Setter Side Effects

Line 221-227:
```c
int set_key_index_config(const char *name, int val, void *privdata,
                         RedisModuleString **err) {
  *(bool *)privdata = (val != 0);
  KeyIndex_SetEnabled(val != 0);
  return REDISMODULE_OK;
}
```

The setter correctly updates both the config struct AND the live service. This is the expected pattern.

### 5. Info String Updated ✓

Line 1719:
```c
ss = sdscatprintf(ss, "key index: %s, ", config->keyIndexEnabled ? "ON" : "OFF");
```

## Implementation Status

- **Triage outcome:** Acceptable.
- **Code change required:** No code change required.
- **Rationale:** Config wiring, version gating, and runtime setter behavior follow established module patterns.
