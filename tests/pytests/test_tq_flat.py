from common import *


def _tq_schema_params(dim=2, metric="L2", bits=8, projections=4, seed=7, rotation="ON"):
    return [
        "TYPE", "FLOAT32",
        "DIM", dim,
        "DISTANCE_METRIC", metric,
        "BITS", bits,
        "PROJECTIONS", projections,
        "SEED", seed,
        "ROTATION", rotation,
    ]


def _tq_hnsw_schema_params(dim=2, metric="L2", bits=8, projections=4, seed=7, rotation="ON",
                           m=16, ef_construction=200, ef_runtime=50):
    return _tq_schema_params(dim, metric, bits, projections, seed, rotation) + [
        "M", m,
        "EF_CONSTRUCTION", ef_construction,
        "EF_RUNTIME", ef_runtime,
    ]


def _field_stats_by_identifier(info, identifier):
    for field_stats in info["field statistics"]:
        field_stats = to_dict(field_stats)
        if field_stats["identifier"] == identifier:
            return field_stats
    raise AssertionError(f"missing field statistics for {identifier}")


@skip(no_json=True)
def test_tq_flat_json_single_and_multi_value():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = _tq_schema_params()
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_json", "ON", "JSON", "SCHEMA", "$.vec", "AS", "vec", "VECTOR", "TQ-FLAT", len(params), *params).ok()

    conn.execute_command("JSON.SET", "doc:1", "$", '{"vec":[0.0,0.0]}')
    waitForIndex(env, "idx_tq_json")

    query = np.array([0.0, 0.0], dtype=np.float32).tobytes()
    res = env.cmd(
        "FT.SEARCH", "idx_tq_json", "*=>[KNN 1 @vec $blob AS dist]",
        "PARAMS", "2", "blob", query,
        "SORTBY", "dist",
        "RETURN", "1", "dist",
        "DIALECT", "2",
    )
    env.assertEqual(res[0], 1)
    env.assertEqual(res[1], "doc:1")

    env.expect(
        "FT.CREATE", "idx_tq_json_multi", "ON", "JSON", "SCHEMA", "$.vecs[*]", "AS", "vec", "VECTOR", "TQ-FLAT", len(params), *params
    ).error().contains("TQ-FLAT does not support multi-value vectors")
    conn.execute_command("FT.DROPINDEX", "idx_tq_json", "DD")


def test_tq_flat_info():
    env = Env(protocol=3, moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = _tq_schema_params()
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_info", "SCHEMA", "vec", "VECTOR", "TQ-FLAT", len(params), *params).ok()
    waitForIndex(env, "idx_tq_info")

    info = to_dict(env.executeCommand("FT.INFO", "idx_tq_info"))
    attr = to_dict(info["attributes"][0])
    env.assertEqual(attr["identifier"], "vec")
    env.assertEqual(attr["attribute"], "vec")
    env.assertEqual(attr["type"], "VECTOR")
    env.assertEqual(attr["algorithm"], "TQ-FLAT")
    env.assertEqual(attr["data_type"], "FLOAT32")
    env.assertEqual(attr["dim"], 2)
    env.assertEqual(attr["distance_metric"], "L2")
    env.assertEqual(attr["bits"], 8)
    env.assertEqual(attr["projections"], 4)
    env.assertEqual(attr["seed"], 7)
    env.assertEqual(attr["rotation"], "ON")
    conn.execute_command("FT.DROPINDEX", "idx_tq_info", "DD")


def test_tq_flat_knn_and_range_query():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = _tq_schema_params()
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_knn", "SCHEMA", "v", "VECTOR", "TQ-FLAT", len(params), *params).ok()
    waitForIndex(env, "idx_tq_knn")

    conn.execute_command("HSET", "doc:1", "v", np.array([0.0, 0.0], dtype=np.float32).tobytes())
    conn.execute_command("HSET", "doc:2", "v", np.array([1.0, 0.0], dtype=np.float32).tobytes())
    conn.execute_command("HSET", "doc:3", "v", np.array([2.0, 0.0], dtype=np.float32).tobytes())

    query = np.array([0.0, 0.0], dtype=np.float32).tobytes()

    knn = env.cmd(
        "FT.SEARCH", "idx_tq_knn", "*=>[KNN 3 @v $blob AS dist]",
        "PARAMS", "2", "blob", query,
        "SORTBY", "dist",
        "RETURN", "1", "dist",
        "DIALECT", "2",
    )
    env.assertEqual(knn[0], 3)
    env.assertEqual(knn[1], "doc:1")
    env.assertEqual(knn[3], "doc:2")
    env.assertEqual(knn[5], "doc:3")

    range_res = env.cmd(
        "FT.SEARCH", "idx_tq_knn", "@v:[VECTOR_RANGE 1.01 $blob]=>{$yield_distance_as: dist}",
        "PARAMS", "2", "blob", query,
        "SORTBY", "dist",
        "RETURN", "1", "dist",
        "DIALECT", "2",
    )
    env.assertEqual(range_res[0], 2)
    env.assertEqual(range_res[1], "doc:1")
    env.assertEqual(range_res[3], "doc:2")
    conn.execute_command("FT.DROPINDEX", "idx_tq_knn", "DD")


def test_tq_flat_rejects_non_float32():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = [
        "TYPE", "FLOAT16",
        "DIM", 2,
        "DISTANCE_METRIC", "L2",
        "BITS", 8,
        "PROJECTIONS", 4,
    ]
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_reject", "SCHEMA", "v", "VECTOR", "TQ-FLAT", len(params), *params) \
        .error().contains("TQ-FLAT only supports FLOAT32 vectors")


def test_tq_hnsw_info_and_knn():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = _tq_hnsw_schema_params(metric="COSINE")
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_hnsw_info", "SCHEMA", "v", "VECTOR", "TQ-HNSW", len(params), *params).ok()
    waitForIndex(env, "idx_tq_hnsw_info")

    info = to_dict(env.executeCommand("FT.INFO", "idx_tq_hnsw_info"))
    attr = to_dict(info["attributes"][0])
    env.assertEqual(attr["identifier"], "v")
    env.assertEqual(attr["type"], "VECTOR")
    env.assertEqual(attr["algorithm"], "TQ-HNSW")
    env.assertEqual(attr["data_type"], "FLOAT32")
    env.assertEqual(attr["dim"], 2)
    env.assertEqual(attr["distance_metric"], "COSINE")
    env.assertEqual(attr["bits"], 8)
    env.assertEqual(attr["projections"], 4)
    env.assertEqual(attr["seed"], 7)
    env.assertEqual(attr["rotation"], "ON")
    env.assertEqual(attr["M"], 16)
    env.assertEqual(attr["ef_construction"], 200)
    env.assertEqual(attr["ef_runtime"], 50)

    conn.execute_command("HSET", "doc:1", "v", np.array([1.0, 0.0], dtype=np.float32).tobytes())
    conn.execute_command("HSET", "doc:2", "v", np.array([0.0, 1.0], dtype=np.float32).tobytes())
    waitForIndex(env, "idx_tq_hnsw_info")

    query = np.array([1.0, 0.0], dtype=np.float32).tobytes()
    res = env.cmd(
        "FT.SEARCH", "idx_tq_hnsw_info", "*=>[KNN 2 @v $blob AS dist]",
        "PARAMS", "2", "blob", query,
        "SORTBY", "dist",
        "RETURN", "1", "dist",
        "DIALECT", "2",
    )
    env.assertEqual(res[0], 2)
    env.assertEqual(res[1], "doc:1")
    conn.execute_command("FT.DROPINDEX", "idx_tq_hnsw_info", "DD")


def test_tq_hnsw_uses_tiered_flat_buffer():
    env = Env(moduleArgs="DEFAULT_DIALECT 2 WORKERS 1")
    conn = getConnectionByEnv(env)

    params = _tq_hnsw_schema_params(dim=256, metric="COSINE", projections=64)
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_hnsw_tiered", "SCHEMA", "v", "VECTOR", "TQ-HNSW", len(params), *params).ok()
    waitForIndex(env, "idx_tq_hnsw_tiered")

    pipe = conn.pipeline(transaction=False)
    for i in range(200):
        vec = np.random.default_rng(i).random(256, dtype=np.float32).tobytes()
        pipe.execute_command("HSET", f"doc:{i}", "v", vec)
    pipe.execute()

    info = to_dict(env.executeCommand("FT.INFO", "idx_tq_hnsw_tiered"))
    field_stats = _field_stats_by_identifier(info, "v")
    env.assertGreater(field_stats["flat_buffer_size"], 0)
    env.assertEqual(field_stats["direct_hnsw_insertions"], 0)

    env.assertEqual(info["num_docs"], 200)

    conn.execute_command("FT.DROPINDEX", "idx_tq_hnsw_tiered", "DD")
