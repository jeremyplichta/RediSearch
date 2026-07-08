import os
import pytest
import multiprocessing
import threading
import time
import signal
import tempfile
import numpy as np
from common import skip, getRDBFile, REDISEARCH_CACHE_DIR, config_cmd, debug_cmd, getConnectionByEnv, waitForIndex
from common import to_dict
from RLTest import Env

@skip(cluster=True)
@pytest.mark.timeout(120)
def test_rdb_load_no_deadlock():
    """
    Test that loading from RDB while constantly sending INFO commands doesn't cause deadlock.
    This test starts a clean Redis server, then triggers RDB loading from the client side
    while some subprocesses keep sending INFO commands.
    """
    # Bundled RDB fixture (see tests/pytests/test_rdbs/)
    rdb_filename = 'redisearch_8.0_with_vecsim.rdb'

    # Create a clean Redis environment
    test_env = Env(moduleArgs='')

    # Start the server first
    test_env.start()

    # Verify server is running
    test_env.expect('PING').equal(True)

    # Verify the bundled RDB fixture is available
    if not getRDBFile(test_env, rdb_filename):
        return

    # Configure indexer to yield more frequently during loading to increase chance of deadlock
    test_env.cmd('CONFIG', 'SET', 'search-indexer-yield-every-ops', '1')
    test_env.cmd('CONFIG', 'SET', 'busy-reply-threshold', 1)
    test_env.expect(debug_cmd(), 'INDEXER_SLEEP_BEFORE_YIELD_MICROS', '50000').ok()

    # Get Redis configuration for RDB file location
    dbFileName = test_env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = test_env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)

    # Path to the bundled RDB fixture
    filePath = os.path.join(REDISEARCH_CACHE_DIR, rdb_filename)

    # Create symlink to the downloaded RDB file
    try:
        os.unlink(rdbFilePath)
    except OSError:
        pass
    os.symlink(filePath, rdbFilePath)

    # Give the system time to process the symlink
    time.sleep(1)

    def info_command_process(port):
        """Process that continuously sends INFO commands"""
        import redis

        # Create a new connection in this process
        conn = redis.Redis(host='localhost', port=port, decode_responses=True)

        while True:
            try:
                result = conn.execute_command('INFO', 'everything')
            except Exception as e:
                continue

    # Start the INFO command thread
    redis_port = test_env.getConnection().connection_pool.connection_kwargs['port']
    info_processes = []

    for i in range(20):
        process = multiprocessing.Process(
            target=info_command_process,
            args=(redis_port,),
            daemon=True
        )
        process.start()
        info_processes.append(process)

    # Get current database size before reload
    # Trigger the reload - use NOSAVE to prevent overwriting our RDB file
    test_env.cmd('DEBUG', 'RELOAD', 'NOSAVE')
    for process in info_processes:
        process.terminate()
        process.join()

    test_env.expect('PING').equal(True)

    # Check database size to see if anything was loaded
    dbsize = test_env.cmd('DBSIZE')

    # Try to get info about any existing indices
    indices_info = test_env.cmd('FT._LIST')
    assert indices_info, "No indices found after RDB load"
    # If there are indices, verify we can get info about the first one
    test_env.expect('FT.INFO', indices_info[0]).noError()


@skip(cluster=True)
def test_rdb_reload_tq_round_trip():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = env.getConnection()
    index_name = 'idx_tq_rdb'
    doc_ids = ['tq:rdb:doc:1', 'tq:rdb:doc:2', 'tq:rdb:doc:3']

    env.cmd('FLUSHALL')

    params = [
        'TYPE', 'FLOAT32',
        'DIM', 2,
        'DISTANCE_METRIC', 'COSINE',
        'COMPRESSION', 'TQ8',
    ]

    env.expect('FT.CREATE', index_name, 'SCHEMA', 'v', 'VECTOR', 'HNSW', len(params), *params).ok()
    # Unit vectors with cosine distances 0.0, 1.0 and 2.0 from the query.
    conn.execute_command('HSET', doc_ids[0], 'v', np.array([1.0, 0.0], dtype=np.float32).tobytes())
    conn.execute_command('HSET', doc_ids[1], 'v', np.array([0.0, 1.0], dtype=np.float32).tobytes())
    conn.execute_command('HSET', doc_ids[2], 'v', np.array([-1.0, 0.0], dtype=np.float32).tobytes())
    waitForIndex(env, index_name)

    query = np.array([1.0, 0.0], dtype=np.float32).tobytes()
    before = env.cmd(
        'FT.SEARCH', index_name, '*=>[KNN 3 @v $blob AS dist]',
        'PARAMS', 2, 'blob', query,
        'SORTBY', 'dist',
        'RETURN', 1, 'dist',
        'DIALECT', 2,
    )
    env.assertEqual(before[1], doc_ids[0])
    env.assertEqual(before[3], doc_ids[1])
    env.assertEqual(before[5], doc_ids[2])

    env.restartAndReload()
    waitForIndex(env, index_name)

    after = to_dict(env.cmd('FT.INFO', index_name))
    attr = to_dict(after['attributes'][0])
    env.assertEqual(attr['algorithm'], 'HNSW')
    env.assertEqual(attr['compression'], 'TQ8')
    env.assertEqual(after['num_docs'], 3)

    round_trip = env.cmd(
        'FT.SEARCH', index_name, '*=>[KNN 3 @v $blob AS dist]',
        'PARAMS', 2, 'blob', query,
        'SORTBY', 'dist',
        'RETURN', 1, 'dist',
        'DIALECT', 2,
    )
    env.assertEqual(round_trip[1], doc_ids[0])
    env.assertEqual(round_trip[3], doc_ids[1])
    env.assertEqual(round_trip[5], doc_ids[2])


@skip(cluster=True)
def test_suffix_trie_survives_rdb_reload(env):
    """
    The suffix DS isn't serialized directly — it's rebuilt from the inverted
    index when the RDB is loaded. Verify the length-1 sub-suffix invariant
    survives the round-trip on both TEXT (rune trie) and TAG (byte triemap),
    in the dump and through a short-token query.
    """
    env.expect(config_cmd(), 'set', 'MINPREFIX', 1).ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA',
                         't_text', 'TEXT', 'WITHSUFFIXTRIE',
                         't_tag',  'TAG',  'WITHSUFFIXTRIE')

    conn.execute_command('HSET', 'doc:1', 't_text', 'banana', 't_tag', 'banana')

    text_dump_before = sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx'))
    tag_dump_before  = sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx', 't_tag'))

    env.dumpAndReload()
    waitForIndex(env, 'idx')

    env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx')), text_dump_before)
    env.assertEqual(sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx', 't_tag')), tag_dump_before)

    env.expect('FT.SEARCH', 'idx', '@t_text:*a*',  'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', '@t_tag:{*a*}', 'NOCONTENT').equal([1, 'doc:1'])
