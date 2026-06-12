from common import *


def _tq_schema_params(dim=2, metric="COSINE", compression="TQ8"):
    return [
        "TYPE", "FLOAT32",
        "DIM", dim,
        "DISTANCE_METRIC", metric,
        "COMPRESSION", compression,
    ]


def _tq_hnsw_schema_params(dim=2, metric="COSINE", compression="TQ8",
                           m=16, ef_construction=200, ef_runtime=50):
    return _tq_schema_params(dim, metric, compression) + [
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
def test_tq_json_single_and_multi_value():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = _tq_schema_params()
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_json", "ON", "JSON", "SCHEMA", "$.vec", "AS", "vec", "VECTOR", "HNSW", len(params), *params).ok()

    conn.execute_command("JSON.SET", "doc:1", "$", '{"vec":[1.0,0.0]}')
    waitForIndex(env, "idx_tq_json")

    query = np.array([1.0, 0.0], dtype=np.float32).tobytes()
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
        "FT.CREATE", "idx_tq_json_multi", "ON", "JSON", "SCHEMA", "$.vecs[*]", "AS", "vec", "VECTOR", "HNSW", len(params), *params
    ).error().contains("TQ compression does not support multi-value vectors")
    conn.execute_command("FT.DROPINDEX", "idx_tq_json", "DD")


def test_tq_info():
    env = Env(protocol=3, moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = _tq_hnsw_schema_params()
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_info", "SCHEMA", "vec", "VECTOR", "HNSW", len(params), *params).ok()
    waitForIndex(env, "idx_tq_info")

    info = to_dict(env.executeCommand("FT.INFO", "idx_tq_info"))
    attr = to_dict(info["attributes"][0])
    env.assertEqual(attr["identifier"], "vec")
    env.assertEqual(attr["attribute"], "vec")
    env.assertEqual(attr["type"], "VECTOR")
    # TQ-compressed fields are reported as HNSW with a compression attribute,
    # mirroring how SVS-VAMANA reports LVQ compression.
    env.assertEqual(attr["algorithm"], "HNSW")
    env.assertEqual(attr["data_type"], "FLOAT32")
    env.assertEqual(attr["dim"], 2)
    env.assertEqual(attr["distance_metric"], "COSINE")
    env.assertEqual(attr["M"], 16)
    env.assertEqual(attr["ef_construction"], 200)
    env.assertEqual(attr["compression"], "TQ8")
    # TurboQuant internals are not part of the API surface.
    env.assertNotContains("bits", attr)
    env.assertNotContains("projections", attr)
    env.assertNotContains("seed", attr)
    env.assertNotContains("rotation", attr)
    conn.execute_command("FT.DROPINDEX", "idx_tq_info", "DD")


def test_tq_knn_and_range_query():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = _tq_schema_params()
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_knn", "SCHEMA", "v", "VECTOR", "HNSW", len(params), *params).ok()
    waitForIndex(env, "idx_tq_knn")

    # Unit vectors with cosine distances 0.0, 1.0 and 2.0 from the query.
    conn.execute_command("HSET", "doc:1", "v", np.array([1.0, 0.0], dtype=np.float32).tobytes())
    conn.execute_command("HSET", "doc:2", "v", np.array([0.0, 1.0], dtype=np.float32).tobytes())
    conn.execute_command("HSET", "doc:3", "v", np.array([-1.0, 0.0], dtype=np.float32).tobytes())
    waitForIndex(env, "idx_tq_knn")

    query = np.array([1.0, 0.0], dtype=np.float32).tobytes()

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

    # The TQ cosine kernels score on an angle-based scale rather than 1-cos, so absolute
    # VECTOR_RANGE radii are not directly comparable to exact cosine distances. Use a radius
    # that covers all docs and assert mechanics + ordering only.
    range_res = env.cmd(
        "FT.SEARCH", "idx_tq_knn", "@v:[VECTOR_RANGE 10 $blob]=>{$yield_distance_as: dist}",
        "PARAMS", "2", "blob", query,
        "SORTBY", "dist",
        "RETURN", "1", "dist",
        "DIALECT", "2",
    )
    env.assertEqual(range_res[0], 3)
    env.assertEqual(range_res[1], "doc:1")
    env.assertEqual(range_res[3], "doc:2")
    env.assertEqual(range_res[5], "doc:3")
    conn.execute_command("FT.DROPINDEX", "idx_tq_knn", "DD")


def test_tq4_compression():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = _tq_schema_params(dim=8, compression="TQ4")
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq4", "SCHEMA", "v", "VECTOR", "HNSW", len(params), *params).ok()
    waitForIndex(env, "idx_tq4")

    info = to_dict(env.executeCommand("FT.INFO", "idx_tq4"))
    attr = to_dict(info["attributes"][0])
    env.assertEqual(attr["compression"], "TQ4")

    base = np.zeros(8, dtype=np.float32)
    base[0] = 1.0
    other = np.zeros(8, dtype=np.float32)
    other[1] = 1.0
    conn.execute_command("HSET", "doc:1", "v", base.tobytes())
    conn.execute_command("HSET", "doc:2", "v", other.tobytes())
    waitForIndex(env, "idx_tq4")

    res = env.cmd(
        "FT.SEARCH", "idx_tq4", "*=>[KNN 2 @v $blob AS dist]",
        "PARAMS", "2", "blob", base.tobytes(),
        "SORTBY", "dist",
        "RETURN", "1", "dist",
        "DIALECT", "2",
    )
    env.assertEqual(res[0], 2)
    env.assertEqual(res[1], "doc:1")
    conn.execute_command("FT.DROPINDEX", "idx_tq4", "DD")


def test_tq_rejects_non_float32():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = [
        "TYPE", "FLOAT16",
        "DIM", 2,
        "DISTANCE_METRIC", "COSINE",
        "COMPRESSION", "TQ8",
    ]
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_reject", "SCHEMA", "v", "VECTOR", "HNSW", len(params), *params) \
        .error().contains("TQ compression only supports FLOAT32 vectors")


def test_tq_rejects_l2_metric():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = _tq_schema_params(metric="L2")
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_l2", "SCHEMA", "v", "VECTOR", "HNSW", len(params), *params) \
        .error().contains("TQ compression with DISTANCE_METRIC L2 is not yet supported; use COSINE or IP")


def test_tq_rejects_unknown_compression():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = _tq_schema_params(compression="TQ16")
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_bad", "SCHEMA", "v", "VECTOR", "HNSW", len(params), *params) \
        .error().contains("COMPRESSION")


def test_tq_rejects_legacy_algorithm_names():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)
    conn.flushall()

    params = _tq_schema_params()
    for legacy in ("TQ-FLAT", "TQ-HNSW"):
        env.expect("FT.CREATE", "idx_tq_legacy", "SCHEMA", "v", "VECTOR", legacy, len(params), *params) \
            .error().contains("Bad arguments for vector similarity algorithm")


def test_tq_hnsw_uses_tiered_flat_buffer():
    env = Env(moduleArgs="DEFAULT_DIALECT 2 WORKERS 1")
    conn = getConnectionByEnv(env)

    params = _tq_hnsw_schema_params(dim=256)
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_hnsw_tiered", "SCHEMA", "v", "VECTOR", "HNSW", len(params), *params).ok()
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
