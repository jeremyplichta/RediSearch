from common import *


def _seed_hash_docs(env, prefix, count):
    conn = getConnectionByEnv(env)
    for i in range(count):
        env.assertEqual(conn.execute_command('HSET', f'{prefix}{i}', 'name', f'name{i}'), 1)


def _seed_string_keys(env, prefix, count):
    conn = getConnectionByEnv(env)
    for i in range(count):
        env.assertEqual(conn.execute_command('SET', f'{prefix}{i}', f'v{i}'), 'OK')


def _key_index_info(env, idx='idx'):
    info = index_info(env, idx)
    for field in [
        'key_index_state',
        'key_index_num_entries',
        'key_index_update_queue_depth',
        'key_index_fallback_count',
        'key_index_bootstrap_duration_ms',
        'key_index_snapshot_peak_bytes',
    ]:
        env.assertContains(field, info)
    return info


def _key_index_fallback_count(env, idx='idx'):
    return int(_key_index_info(env, idx)['key_index_fallback_count'])


def _trigger_key_index_iter_error(env):
    conn = getConnectionByEnv(env)
    for debug_command in ['FT.DEBUG', '_FT.DEBUG']:
        try:
            env.assertEqual(conn.execute_command(debug_command, 'KEY_INDEX_TRIGGER_ITER_ERR'), 'OK')
            return debug_command
        except Exception:
            continue

    raise AssertionError('Failed to trigger key-index iterator error using FT.DEBUG or _FT.DEBUG')


def _wait_for_key_index_state(env, idx, expected_states, timeout=30):
    expected_states = set(expected_states)

    def _check():
        info = _key_index_info(env, idx)
        state = info['key_index_state']
        return state in expected_states, {
            'state': state,
            'queue_depth': info['key_index_update_queue_depth'],
        }

    wait_for_condition(_check, f'waiting for key index state in {sorted(expected_states)}', timeout=timeout)
    return _key_index_info(env, idx)['key_index_state']


def _wait_for_key_index_queue_drained(env, idx, timeout=45):
    def _check():
        info = _key_index_info(env, idx)
        queue_depth = int(info['key_index_update_queue_depth'])
        state = info['key_index_state']
        return state == 'ready' and queue_depth == 0, {'state': state, 'queue_depth': queue_depth}

    wait_for_condition(_check, 'waiting for key index queue to drain', timeout=timeout)


def _wait_for_fallback_count_at_least(env, idx, expected_min, timeout=30):
    def _check():
        current = _key_index_fallback_count(env, idx)
        return current >= expected_min, {'fallback_count': current, 'expected_min': expected_min}

    wait_for_condition(_check, f'waiting for fallback count >= {expected_min}', timeout=timeout)


@skip(cluster=True, redis_less_than='7.9.227')
def test_key_index_info_fields_and_state_transition(env):
    _seed_hash_docs(env, 'ki:state:', 200)
    env.expect('CONFIG', 'SET', 'search-key-index', 'no').ok()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'ki:state:', 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndex(env, 'idx')

    info = _key_index_info(env, 'idx')
    env.assertEqual(info['key_index_state'], 'off')
    env.assertGreaterEqual(int(info['key_index_num_entries']), 0)
    env.assertGreaterEqual(int(info['key_index_update_queue_depth']), 0)
    env.assertGreaterEqual(int(info['key_index_fallback_count']), 0)
    env.assertGreaterEqual(int(info['key_index_bootstrap_duration_ms']), 0)
    env.assertGreaterEqual(int(info['key_index_snapshot_peak_bytes']), 0)

    env.expect('CONFIG', 'SET', 'search-key-index', 'yes').ok()
    env.assertContains(_wait_for_key_index_state(env, 'idx', {'warming', 'ready'}), ['warming', 'ready'])
    env.assertEqual(_wait_for_key_index_state(env, 'idx', {'ready'}), 'ready')

    info = _key_index_info(env, 'idx')
    env.assertEqual(info['key_index_state'], 'ready')
    env.assertGreaterEqual(int(info['key_index_num_entries']), 200)


@skip(cluster=True, redis_less_than='7.9.227')
def test_key_index_overlapping_prefixes_are_deduped(env):
    total_docs = 300
    _seed_hash_docs(env, 'ki:dup:item:', total_docs)

    env.expect('CONFIG', 'SET', 'search-key-index', 'no').ok()
    env.expect(
        'FT.CREATE', 'idx_probe',
        'ON', 'HASH',
        'PREFIX', '1', 'ki:dup:item:',
        'SCHEMA', 'name', 'TEXT'
    ).ok()
    waitForIndex(env, 'idx_probe')

    env.expect('CONFIG', 'SET', 'search-key-index', 'yes').ok()
    env.assertEqual(_wait_for_key_index_state(env, 'idx_probe', {'ready'}), 'ready')
    baseline_fallback_count = _key_index_fallback_count(env, 'idx_probe')

    env.expect(
        'FT.CREATE', 'idx',
        'ON', 'HASH',
        'PREFIX', '2', 'ki:dup:', 'ki:dup:item:',
        'SCHEMA', 'name', 'TEXT'
    ).ok()
    waitForIndex(env, 'idx')

    info = index_info(env, 'idx')
    env.assertEqual(int(info['num_docs']), total_docs)
    env.assertEqual(env.cmd('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', 0, 0)[0], total_docs)
    env.assertEqual(_key_index_fallback_count(env, 'idx'), baseline_fallback_count)


@skip(cluster=True, redis_less_than='7.9.227')
def test_key_index_fallback_counter_increments_while_warming(env):
    indexed_docs = 500
    noise_keys = 30000
    _seed_hash_docs(env, 'ki:warming:doc:', indexed_docs)
    _seed_string_keys(env, 'ki:warming:noise:', noise_keys)

    env.expect('CONFIG', 'SET', 'search-key-index', 'no').ok()
    env.expect(
        'FT.CREATE', 'idx_probe',
        'ON', 'HASH',
        'PREFIX', '1', 'ki:warming:doc:',
        'SCHEMA', 'name', 'TEXT'
    ).ok()
    waitForIndex(env, 'idx_probe')

    baseline_fallback_count = _key_index_fallback_count(env, 'idx_probe')
    env.expect('CONFIG', 'SET', 'search-key-index', 'yes').ok()
    env.assertEqual(_wait_for_key_index_state(env, 'idx_probe', {'warming'}), 'warming')

    env.expect(
        'FT.CREATE', 'idx',
        'ON', 'HASH',
        'PREFIX', '1', 'ki:warming:doc:',
        'SCHEMA', 'name', 'TEXT'
    ).ok()
    waitForIndex(env, 'idx')

    info = _key_index_info(env, 'idx')
    env.assertEqual(int(info['num_docs']), indexed_docs)
    _wait_for_fallback_count_at_least(env, 'idx', baseline_fallback_count + 1)
    env.assertEqual(_wait_for_key_index_state(env, 'idx', {'ready'}, timeout=60), 'ready')


@skip(cluster=True, redis_less_than='7.9.227')
def test_key_index_rename_pair_and_destination_fallback_semantics(env):
    total_docs = 120
    src_prefix = 'ki:rename:src:'
    dst_prefix = 'ki:rename:dst:'
    dst_only_prefix = 'ki:rename:dst_only:'
    _seed_hash_docs(env, src_prefix, total_docs)

    env.expect('CONFIG', 'SET', 'search-key-index', 'no').ok()
    env.expect(
        'FT.CREATE', 'idx_probe',
        'ON', 'HASH',
        'PREFIX', '1', src_prefix,
        'SCHEMA', 'name', 'TEXT'
    ).ok()
    waitForIndex(env, 'idx_probe')

    env.expect('CONFIG', 'SET', 'search-key-index', 'yes').ok()
    env.assertEqual(_wait_for_key_index_state(env, 'idx_probe', {'ready'}), 'ready')

    conn = getConnectionByEnv(env)
    for i in range(total_docs):
        env.assertEqual(conn.execute_command('RENAME', f'{src_prefix}{i}', f'{dst_prefix}{i}'), 'OK')

    # Destination-only upsert mirrors the out-of-order rename_to fallback semantics.
    for i in range(total_docs):
        env.assertEqual(conn.execute_command('HSET', f'{dst_only_prefix}{i}', 'name', f'dst-only-{i}'), 1)

    _wait_for_key_index_queue_drained(env, 'idx_probe', timeout=60)
    baseline_fallback_count = _key_index_fallback_count(env, 'idx_probe')

    env.expect(
        'FT.CREATE', 'idx',
        'ON', 'HASH',
        'PREFIX', '2', dst_prefix, dst_only_prefix,
        'SCHEMA', 'name', 'TEXT'
    ).ok()
    waitForIndex(env, 'idx')

    info = _key_index_info(env, 'idx')
    env.assertEqual(int(info['num_docs']), total_docs * 2)
    env.assertEqual(env.cmd('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', 0, 0)[0], total_docs * 2)
    env.assertEqual(_key_index_fallback_count(env, 'idx'), baseline_fallback_count)


@skip(cluster=True, redis_less_than='7.9.227')
def test_key_index_fallback_counter_increments_when_disabled(env):
    total_docs = 250
    _seed_hash_docs(env, 'ki:fallback:', total_docs)

    env.expect('CONFIG', 'SET', 'search-key-index', 'no').ok()
    env.expect(
        'FT.CREATE', 'idx',
        'ON', 'HASH',
        'PREFIX', '1', 'ki:fallback:',
        'SCHEMA', 'name', 'TEXT'
    ).ok()
    waitForIndex(env, 'idx')

    info = _key_index_info(env, 'idx')
    env.assertEqual(int(info['num_docs']), total_docs)
    env.assertGreaterEqual(int(info['key_index_fallback_count']), 1)


@skip(cluster=True)
def test_key_index_iter_error_fallback_increments_once(env):
    total_docs = 128
    prefix = 'ki:itererr:'
    _seed_hash_docs(env, prefix, total_docs)

    env.expect('CONFIG', 'SET', 'search-key-index', 'no').ok()
    env.expect('FT.CREATE', 'idx_probe', 'ON', 'HASH', 'PREFIX', '1', prefix, 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndex(env, 'idx_probe')

    env.expect('CONFIG', 'SET', 'search-key-index', 'yes').ok()
    env.assertEqual(_wait_for_key_index_state(env, 'idx_probe', {'ready'}), 'ready')
    before = _key_index_fallback_count(env, 'idx_probe')

    _trigger_key_index_iter_error(env)

    env.expect('FT.CREATE', 'idx_iter_err', 'ON', 'HASH', 'PREFIX', '1', prefix, 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndex(env, 'idx_iter_err')

    after = _key_index_fallback_count(env, 'idx_iter_err')
    env.assertEqual(after - before, 1)
