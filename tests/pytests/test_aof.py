from RLTest import Env
import random
import numpy as np
from includes import *
from common import getConnectionByEnv, waitForIndex, toSortedFlatList


def aofTestCommon(env, reloadfn):
        # TODO: Change this attribute in rmtest
        conn = getConnectionByEnv(env)
        env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'field1', 'text', 'field2', 'numeric')
        for x in range(1, 10):
            conn.execute_command('hset', f'doc{x}', 'field1', f'myText{x}', 'field2', 20 * x)

        reloadfn()
        waitForIndex(env, 'idx')
        exp = [9, 'doc1', ['field1', 'myText1', 'field2', '20'], 'doc2', ['field1', 'myText2', 'field2', '40'],
                   'doc3', ['field1', 'myText3', 'field2', '60'], 'doc4', ['field1', 'myText4', 'field2', '80'],
                   'doc5', ['field1', 'myText5', 'field2', '100'], 'doc6', ['field1', 'myText6', 'field2', '120'],
                   'doc7', ['field1', 'myText7', 'field2', '140'], 'doc8', ['field1', 'myText8', 'field2', '160'],
                   'doc9', ['field1', 'myText9', 'field2', '180']]

        reloadfn()
        waitForIndex(env, 'idx')
        ret = env.cmd('ft.search', 'idx', 'myt*')
        env.assertEqual(toSortedFlatList(ret), toSortedFlatList(exp))

def testAof():
    env = Env(useAof=True)
    aofTestCommon(env, lambda: env.restartAndReload())


def testRawAof():
    env = Env(useAof=True)
    if env.env == 'existing-env':
        env.skip()
    aofTestCommon(env, lambda: env.broadcast('debug', 'loadaof'))


def testAofTqFlatRoundTrip():
    env = Env(useAof=True, moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    index_name = 'idx_tq_aof'
    doc_ids = ['tq:aof:doc:1', 'tq:aof:doc:2']

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
    env.cmd('FT.CREATE', index_name, 'SCHEMA', 'v', 'VECTOR', 'TQ-FLAT', len(params), *params)
    conn.execute_command('HSET', doc_ids[0], 'v', np.array([0.0, 0.0], dtype=np.float32).tobytes())
    conn.execute_command('HSET', doc_ids[1], 'v', np.array([1.0, 0.0], dtype=np.float32).tobytes())
    waitForIndex(env, index_name)

    before = env.cmd(
        'FT.SEARCH', index_name, '*=>[KNN 2 @v $blob AS dist]',
        'PARAMS', '2', 'blob', np.array([0.0, 0.0], dtype=np.float32).tobytes(),
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2',
    )
    env.assertEqual(before[1], doc_ids[0])
    env.assertEqual(before[3], doc_ids[1])

    env.restartAndReload()
    waitForIndex(env, index_name)

    info = to_dict(env.cmd('FT.INFO', index_name))
    attr = to_dict(info['attributes'][0])
    env.assertEqual(attr['algorithm'], 'TQ-FLAT')
    env.assertEqual(attr['bits'], 8)
    env.assertEqual(attr['projections'], 4)
    env.assertEqual(attr['seed'], 7)
    env.assertEqual(attr['rotation'], 'ON')

    after = env.cmd(
        'FT.SEARCH', index_name, '*=>[KNN 2 @v $blob AS dist]',
        'PARAMS', '2', 'blob', np.array([0.0, 0.0], dtype=np.float32).tobytes(),
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2',
    )
    env.assertEqual(after[1], doc_ids[0])
    env.assertEqual(after[3], doc_ids[1])


def testRewriteAofSortables():
    env = Env(useAof=True)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'schema', 'field1', 'TEXT', 'SORTABLE', 'num1', 'NUMERIC', 'SORTABLE')
    con = env.getClusterConnectionIfNeeded()
    con.execute_command('FT.ADD', 'idx', 'doc', 1.0, 'FIELDS', 'field1', 'Hello World')
    env.restartAndReload()
    env.broadcast('SAVE')

    # Load some documents
    for x in range(100):
        con.execute_command('FT.ADD', 'idx', f'doc{x}', 1.0, 'FIELDS',
                'field1', f'txt{random.random()}',
                'num1', random.random())
    for sspec in [('field1', 'asc'), ('num1', 'desc')]:
        cmd = ['FT.SEARCH', 'idx', 'txt', 'SORTBY', sspec[0], sspec[1]]
        res = env.cmd(*cmd)
        env.restartAndReload()
        res2 = env.cmd(*cmd)
        env.assertEqual(res, res2)


def testAofRewriteSortkeys():
    env = Env(useAof=True)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'SCHEMA', 'foo', 'TEXT', 'SORTABLE', 'bar', 'TAG')
    con = env.getClusterConnectionIfNeeded()
    con.execute_command('FT.ADD', 'idx', '1', '1', 'FIELDS', 'foo', 'A', 'bar', '1')
    con.execute_command('FT.ADD', 'idx', '2', '1', 'fields', 'foo', 'B', 'bar', '1')

    res_exp = env.cmd('FT.SEARCH', 'idx', '@bar:{1}', 'SORTBY', 'foo', 'ASC',
                      'RETURN', '1', 'foo', 'WITHSORTKEYS')

    env.restartAndReload()
    waitForIndex(env, 'idx')
    res_got = env.cmd('FT.SEARCH', 'idx', '@bar:{1}', 'SORTBY', 'foo', 'ASC',
                      'RETURN', '1', 'foo', 'WITHSORTKEYS')

    env.assertEqual(res_exp, res_got)


def testAofRewriteTags():
    env = Env(useAof=True)
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'SCHEMA', 'foo', 'TEXT', 'SORTABLE', 'bar', 'TAG')
    con = env.getClusterConnectionIfNeeded()
    con.execute_command('FT.ADD', 'idx', '1', '1', 'FIELDS', 'foo', 'A', 'bar', '1')
    con.execute_command('FT.ADD', 'idx', '2', '1', 'fields', 'foo', 'B', 'bar', '1')

    info_a = to_dict(env.cmd('FT.INFO', 'idx'))
    env.restartAndReload()
    info_b = to_dict(env.cmd('FT.INFO', 'idx'))
    env.assertEqual(info_a['attributes'], info_b['attributes'])

    # Try to drop the schema
    env.cmd('FT.DROP', 'idx')

    conn.execute_command('del', '1')
    conn.execute_command('del', '2')

    # Try to create it again - should work!
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH',
            'SCHEMA', 'foo', 'TEXT', 'SORTABLE', 'bar', 'TAG')
    con.execute_command('FT.ADD', 'idx', '1', '1', 'FIELDS', 'foo', 'A', 'bar', '1')
    con.execute_command('FT.ADD', 'idx', '2', '1', 'fields', 'foo', 'B', 'bar', '1')
    res = env.cmd('FT.SEARCH', 'idx', '@bar:{1}', 'SORTBY', 'foo', 'ASC',
                  'RETURN', '1', 'foo', 'WITHSORTKEYS')
    env.assertEqual([2, '1', '$a', ['foo', 'A'],
                     '2', '$b', ['foo', 'B']], res)


def to_dict(r):
    return {r[i]: r[i + 1] for i in range(0, len(r), 2)}
