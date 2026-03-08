# Low: Test Coverage Review

**Priority: P3 - Low**
**Category: Testing**
**Files:** `tests/pytests/test_key_index.py`, `tests/pytests/test_config.py`, `tests/pytests/test_async.py`, `tests/pytests/test_index_oom.py`

## Test Coverage Analysis

### New Test File: `test_key_index.py`

| Test Case | Coverage |
|-----------|----------|
| `test_key_index_info_fields_and_state_transition` | State machine: OFF → WARMING → READY |
| `test_key_index_overlapping_prefixes_are_deduped` | Multi-prefix deduplication |
| `test_key_index_fallback_counter_increments_while_warming` | Fallback during warmup |
| `test_key_index_rename_pair_and_destination_fallback_semantics` | RENAME event handling |
| `test_key_index_fallback_counter_increments_when_disabled` | Disabled state fallback |
| `test_key_index_iter_error_fallback_increments_once` | Debug-triggered error |

### Configuration Tests Added

- `testGetConfigOptions`: Added KEY_INDEX check
- `testSetConfigOptions`: Added KEY_INDEX set tests
- `testAllConfig`: Added KEY_INDEX default check
- `testInitConfig`: Added KEY_INDEX init tests
- `testUnprefixedKeyIndexConfigParity`: Cross-API parity check

### Integration Tests Added

- `test_async_indexing_with_key_index_enabled`: Async + key-index combo
- `test_oom_flow_with_key_index_enabled`: OOM + key-index combo

## Missing Coverage (Low Priority)

1. **ERROR state transition**: No test for allocation failure → ERROR
2. **Queue overflow**: No test for 500K+ pending operations
3. **Timer period tuning**: Constants are not configurable for testing
4. **Snapshot memory pressure**: No explicit OOM-during-snapshot test

## Recommendations

Consider adding:
- Unit test for queue overflow restart behavior
- Stress test with rapid enable/disable cycles

## Implementation Status

### Added in this follow-up

- `tests/pytests/test_key_index.py::test_key_index_rapid_enable_disable_cycles_remain_consistent`
  - Deterministic rapid toggle coverage: repeatedly flips `search-key-index` between `yes` and `no` on a live index.
  - Verifies OFF/READY transitions complete on every cycle.
  - Verifies queue drains in READY (`key_index_update_queue_depth == 0`) and final indexed document count matches all writes done across cycles.
  - Verifies fallback counter remains monotonic (does not regress) across repeated enable/disable cycles.
- `tests/pytests/test_key_index.py::test_key_index_queue_depth_stays_empty_while_disabled_and_recovers_after_reenable`
  - Adds queue behavior assertions around disabled mode:
    - While OFF, queue depth remains zero even with additional writes.
    - After re-enable, index returns to READY and queue drains to zero.
  - Verifies writes performed while OFF are reflected after re-enable.
  - Verifies fallback counter remains monotonic through the disable/re-enable flow.

### Still unimplemented / non-deterministic in current framework

- **ERROR state transition via allocator failure**: still requires controlled fault injection hooks for deterministic reproduction.
- **Queue overflow (500K+ pending operations)**: intentionally not added due to runtime/flake risk in standard CI.
- **Timer period tuning scenarios**: still blocked by non-configurable timing constants.
- **Snapshot-memory-pressure (OOM during snapshot)**: still requires deterministic OOM injection during snapshot phase.

## Status: ACCEPTABLE - Good coverage for initial feature ✓
