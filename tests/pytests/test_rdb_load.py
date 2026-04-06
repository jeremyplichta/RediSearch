import os
import pytest
import multiprocessing
import threading
import time
import signal
import tempfile
import numpy as np
from common import skip, downloadFile, REDISEARCH_CACHE_DIR, debug_cmd
from common import to_dict, waitForIndex
from RLTest import Env

@skip(cluster=True)
@pytest.mark.timeout(120)
def test_rdb_load_no_deadlock():
    """
    Test that loading from RDB while constantly sending INFO commands doesn't cause deadlock.
    This test starts a clean Redis server, then triggers RDB loading from the client side
    while some subprocesses keep sending INFO commands.
    """
    # Download the RDB file using downloadFile function
    rdb_filename = 'redisearch_8.0_with_vecsim.rdb'

    # Create a clean Redis environment
    test_env = Env(moduleArgs='')

    # Start the server first
    test_env.start()

    # Verify server is running
    test_env.expect('PING').equal(True)

    # Download the RDB file
    if not downloadFile(test_env, rdb_filename):
        return

    # Configure indexer to yield more frequently during loading to increase chance of deadlock
    test_env.cmd('CONFIG', 'SET', 'search-indexer-yield-every-ops', '1')
    test_env.cmd('CONFIG', 'SET', 'busy-reply-threshold', 1)
    test_env.expect(debug_cmd(), 'INDEXER_SLEEP_BEFORE_YIELD_MICROS', '50000').ok()

    # Get Redis configuration for RDB file location
    dbFileName = test_env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = test_env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)

    # Get the downloaded RDB file path
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
def test_rdb_reload_tq_flat_round_trip():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = env.getConnection()
    index_name = 'idx_tq_rdb'
    doc_ids = ['tq:rdb:doc:1', 'tq:rdb:doc:2', 'tq:rdb:doc:3']

    env.cmd('FLUSHALL')

    params = [
        'TYPE', 'FLOAT32',
        'DIM', 2,
        'DISTANCE_METRIC', 'L2',
        'BITS', 8,
        'PROJECTIONS', 4,
        'SEED', 7,
        'ROTATION', 'ON',
    ]

    env.expect('FT.CREATE', index_name, 'SCHEMA', 'v', 'VECTOR', 'TQ-FLAT', len(params), *params).ok()
    conn.execute_command('HSET', doc_ids[0], 'v', np.array([0.0, 0.0], dtype=np.float32).tobytes())
    conn.execute_command('HSET', doc_ids[1], 'v', np.array([1.0, 0.0], dtype=np.float32).tobytes())
    conn.execute_command('HSET', doc_ids[2], 'v', np.array([2.0, 0.0], dtype=np.float32).tobytes())

    query = np.array([0.0, 0.0], dtype=np.float32).tobytes()
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
    env.assertEqual(attr['algorithm'], 'TQ-FLAT')
    env.assertEqual(attr['bits'], 8)
    env.assertEqual(attr['projections'], 4)
    env.assertEqual(attr['seed'], 7)
    env.assertEqual(attr['rotation'], 'ON')
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
