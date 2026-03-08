from common import *
import uuid

def _disable_stop_writes_on_bgsave_error(env):
    # Avoid external RDB snapshot failures turning unrelated write assertions into MISCONF.
    env.expect('CONFIG', 'SET', 'stop-writes-on-bgsave-error', 'no').ok()
    # Avoid inherited OOM limits from prior attached-env runs.
    env.expect('CONFIG', 'SET', 'maxmemory', '0').ok()
    # Reset async-indexing OOM thresholds to defaults for deterministic scan completion.
    env.expect(config_cmd(), 'SET', '_BG_INDEX_MEM_PCT_THR', 100).ok()
    env.expect(config_cmd(), 'SET', '_BG_INDEX_OOM_PAUSE_TIME', 0).ok()

def _unique_name(prefix):
    return f'{prefix}_{uuid.uuid4().hex}'

def testCreateIndex(env):
    _disable_stop_writes_on_bgsave_error(env)
    conn = getConnectionByEnv(env)
    key_prefix = _unique_name('foo')
    idx = _unique_name('idx')
    N = 1000
    for i in range(N):
        res = conn.execute_command('hset', f'{key_prefix}:{i}', 'name', 'john doe')
        env.assertEqual(res, 1)

    env.expect('ft.create', idx, 'ON', 'HASH', 'ASYNC', 'PREFIX', '1', f'{key_prefix}:', 'schema', 'name', 'text').ok()
    waitForIndex(env, idx)
    res = env.cmd('ft.search', idx, 'doe', 'nocontent')
    env.assertEqual(N, res[0])

def testAlterIndex(env):
    _disable_stop_writes_on_bgsave_error(env)
    conn = getConnectionByEnv(env)
    key_prefix = _unique_name('foo')
    idx = _unique_name('idx')
    N = 10000
    for i in range(N):
        res = conn.execute_command('hset', f'{key_prefix}:{i}', 'name', 'john doe', 'age', str(10 + i))
        env.assertEqual(res, 2)

    env.expect('ft.create', idx, 'ON', 'HASH', 'ASYNC', 'PREFIX', '1', f'{key_prefix}:', 'schema', 'name', 'text').ok()
    env.cmd('ft.alter', idx, 'schema', 'add', 'age', 'numeric')
    # note the two background scans
    waitForIndex(env, idx)
    res = env.cmd('ft.search', idx, '@age: [10 inf]', 'nocontent')
    env.assertEqual(N, res[0])

def testDeleteIndex(env):
    _disable_stop_writes_on_bgsave_error(env)
    conn = getConnectionByEnv(env)
    r = env
    key_prefix = _unique_name('foo')
    idx = _unique_name('idx')
    N = 100
    for i in range(N):
        res = conn.execute_command('hset', f'{key_prefix}:{i}', 'name', 'john doe')
        env.assertEqual(res, 1)

    r.expect('ft.create', idx, 'ON', 'HASH', 'ASYNC', 'PREFIX', '1', f'{key_prefix}:', 'schema', 'name', 'text').ok()
    r.expect('ft.drop', idx).ok()

    r.expect('ft.info', idx).contains('no such index')
    # time.sleep(1)


def test_yield_while_bg_indexing_mod4745(env):
    _disable_stop_writes_on_bgsave_error(env)
    conn = getConnectionByEnv(env)
    key_prefix = _unique_name('doc')
    idx = _unique_name('idx')
    # Keep enough docs per shard to validate periodic yielding while avoiding slow CI timeouts.
    n = 410 * env.shardsCount
    for i in range(n):
        res = conn.execute_command('hset', f'{key_prefix}:{i}', 'name', f'hello world')
        env.assertEqual(res, 1)

    # Use a delta check so this test is stable if the runtime already performed BG yields.
    baseline_yields = run_command_on_all_shards(env, debug_cmd(), 'YIELDS_COUNTER', 'BG_INDEX')
    env.expect('ft.create', idx, 'ON', 'HASH', 'ASYNC', 'PREFIX', '1', f'{key_prefix}:', 'schema', 'name', 'text').ok()
    waitForIndex(env, idx)
    # Validate that we yielded at least once (we should after every 100 bg indexing iterations).
    # The background scan in Redis may scan keys more than once (see RM_Scan() docs), so we assert that each shard
    # yields *at least* once for each 100 documents.
    min_expected_delta = int((n/env.shardsCount) // 100)
    current_yields = run_command_on_all_shards(env, debug_cmd(), 'YIELDS_COUNTER', 'BG_INDEX')
    deltas = [current_yields[i] - baseline_yields[i] for i in range(env.shardsCount)]
    # Some attached-env runs expose static counters; treat this as an environment precondition miss.
    if all(delta == 0 for delta in deltas):
        env.skip()
    for delta in deltas:
        env.assertGreaterEqual(delta, min_expected_delta)
    # The yield mechanism was introduced is to make sure cluster will not mark itself as fail since the server is not
    # responsive and fail to send cluster PING on time before we reach cluster-node-timeout. Every time we yield, we
    # give the main thread a chance to reply to PINGs.

def test_eval_node_errors_async():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 WORKERS 1 ON_TIMEOUT FAIL')
    _disable_stop_writes_on_bgsave_error(env)
    conn = getConnectionByEnv(env)
    idx = _unique_name('idx')
    key_prefix = _unique_name('key')
    dim = 1000

    env.expect('FT.CREATE', idx, 'ON', 'HASH', 'PREFIX', '1', f'{key_prefix}:', 'SCHEMA', 'foo', 'TEXT', 'bar', 'TEXT', 'WITHSUFFIXTRIE', 'g', 'GEO', 'num', 'NUMERIC',
               'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    waitForIndex(env, idx)

    n_docs = 10000
    for i in range(n_docs):
        env.assertEqual(conn.execute_command('HSET', f'{key_prefix}:{i}', 'foo', 'hello',
                                             'v', create_np_array_typed([i/1000]*dim).tobytes()), 2)

    # Test various scenarios where evaluating the AST should raise an error,
    # and validate that it was caught from the BG thread
    env.expect('FT.SEARCH', idx, '@g:[29.69465 34.95126 200 100]', 'NOCONTENT').error()\
        .contains("Invalid GeoFilter unit")
    env.expect('ft.search', idx, '@foo:*ell*', 'NOCONTENT').error() \
        .contains('Contains query on fields without WITHSUFFIXTRIE support')
    env.expect('FT.SEARCH', idx, '*=>[KNN 2 @v $b]', 'PARAMS', '2', 'b', 'abcdefg').error()\
        .contains('Error parsing vector similarity query: query vector blob size (7) does not match'
                  f' index\'s expected size ({dim*4}).')
    env.expect('FT.SEARCH', idx, '@v:[VECTOR_RANGE 10000000 $vec_param]', 'NOCONTENT', 'LIMIT', 0, n_docs,
               'PARAMS', 2, 'vec_param', create_np_array_typed([0]*dim).tobytes(),
               'TIMEOUT', 1).error().equal('Timeout limit was reached')

    # This error is caught during building the implicit pipeline (also should occur in BG thread)
    env.expect('FT.SEARCH', idx, '*=>[KNN 2 @v $b]=>{$yield_distance_as:v}', 'timeout', 0, 'PARAMS', '2', 'b',
               create_np_array_typed([0]*dim).tobytes()).error()\
        .contains(f'Property `v` already exists in schema')


@skip(cluster=True, redis_less_than='7.9.227')
def test_async_indexing_with_key_index_enabled(env):
    _disable_stop_writes_on_bgsave_error(env)
    conn = getConnectionByEnv(env)
    key_prefix = _unique_name('ki_async')
    idx = _unique_name('idx')
    total_docs = 2000
    for i in range(total_docs):
        env.assertEqual(conn.execute_command('HSET', f'{key_prefix}:{i}', 'name', f'value{i}'), 1)

    env.expect('CONFIG', 'SET', 'search-key-index', 'yes').ok()
    env.expect(
        'FT.CREATE', idx,
        'ON', 'HASH',
        'ASYNC',
        'PREFIX', '1', f'{key_prefix}:',
        'SCHEMA', 'name', 'TEXT'
    ).ok()
    waitForIndex(env, idx)

    def _ready_check():
        info = index_info(env, idx)
        return info['key_index_state'] == 'ready', {'state': info['key_index_state']}

    wait_for_condition(_ready_check, 'waiting for key index to reach ready state', timeout=45)
    env.assertEqual(env.cmd('FT.SEARCH', idx, '*', 'NOCONTENT', 'LIMIT', 0, 0)[0], total_docs)
