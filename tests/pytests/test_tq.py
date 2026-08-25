import json

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


def _distance_results(response):
    return {
        response[i]: float(to_dict(response[i + 1])["dist"])
        for i in range(1, len(response), 2)
    }


def _tq_payload_bytes(dim, bits):
    return ((bits - 1) * dim + 7) // 8 + (dim + 7) // 8 + 8


def _assert_dense_tq_identity(env, attr, dim, bits):
    env.assertEqual(attr["tq_rdb_marker"], 2)
    env.assertEqual(attr["tq_codec_version"], 1)
    env.assertEqual(attr["tq_profile"], "DenseReferenceV1")
    env.assertEqual(attr["tq_payload_layout"], "PaperV1")
    env.assertEqual(attr["tq_rotation"], "DenseHaarV1")
    env.assertEqual(attr["tq_qjl"], "DenseGaussianV1")
    env.assertEqual(attr["tq_construction_score"], "FullDecodeReferenceV1")
    env.assertEqual(attr["tq_metric_contract"], "CosineOrInnerProductV1")
    env.assertEqual(attr["tq_projections"], dim)
    env.assertEqual(attr["tq_seed"], 7)
    env.assertEqual(attr["tq_payload_bytes"], _tq_payload_bytes(dim, bits))


def _tq_hybrid_query(env, index_name, query, policy):
    return env.cmd(
        "FT.SEARCH", index_name,
        f"@n:[0 9]=>[KNN 4 @v $blob HYBRID_POLICY {policy}]=>{{$yield_distance_as:dist}}",
        "PARAMS", "2", "blob", query.tobytes(),
        "SORTBY", "dist", "RETURN", "1", "dist", "DIALECT", "2",
    )


def _queue_tq_hybrid_queries(env, index_name, query):
    results = {}
    errors = []

    def run(policy):
        try:
            results[policy] = _tq_hybrid_query(env, index_name, query, policy)
        except BaseException as error:
            errors.append(error)

    threads = [
        threading.Thread(target=run, args=(policy,), daemon=True)
        for policy in ("ADHOC_BF", "BATCHES")
    ]
    for thread in threads:
        thread.start()

    wait_for_condition(
        lambda: (
            getWorkersThpoolStats(env)["highPriorityPendingJobs"] >= len(threads),
            getWorkersThpoolStats(env),
        ),
        "Timeout waiting for split-tier TQ hybrid queries to enter the paused worker queue",
        timeout=10,
    )
    return threads, results, errors


def _assert_tq_hybrid_scores(env, queued):
    threads, results, errors = queued
    for thread in threads:
        thread.join(timeout=10)
        env.assertFalse(thread.is_alive(), message="TQ hybrid query did not finish after resume")
    if errors:
        raise errors[0]

    adhoc = results["ADHOC_BF"]
    batches = results["BATCHES"]
    env.assertEqual(adhoc[0], 4)
    env.assertEqual(batches[0], 4)
    adhoc_scores = _distance_results(adhoc)
    batches_scores = _distance_results(batches)
    env.assertEqual(set(adhoc_scores), set(batches_scores))
    for key, score in adhoc_scores.items():
        env.assertTrue(np.isfinite(score))
        env.assertAlmostEqual(score, batches_scores[key], delta=1e-5)


def _tq_split_tier_vectors(dim):
    vectors = []
    for i in range(4):
        vector = np.zeros(dim, dtype=np.float32)
        vector[i] = float(i + 1)
        vector[(i + 5) % dim] = 0.25 * float(i + 1)
        vectors.append(vector)
    return vectors


def test_tq_creation_requires_unstable_features():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    params = _tq_schema_params()
    env.expect("FT.CREATE", "idx_tq_gated", "SCHEMA", "v", "VECTOR", "HNSW",
               len(params), *params).error().contains(
        "enable ENABLE_UNSTABLE_FEATURES to create TQ indexes"
    )


@skip(no_json=True)
def test_tq_json_single_and_multi_value():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    enable_unstable_features(env)
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

    conn.execute_command("JSON.SET", "doc:1", "$", '{"vec":[0.0,1.0]}')
    waitForIndex(env, "idx_tq_json")
    updated_query = np.array([0.0, 1.0], dtype=np.float32).tobytes()
    updated = env.cmd(
        "FT.SEARCH", "idx_tq_json", "*=>[KNN 1 @vec $blob AS dist]",
        "PARAMS", "2", "blob", updated_query,
        "SORTBY", "dist", "RETURN", "1", "dist", "DIALECT", "2",
    )
    env.assertEqual(updated[0], 1)
    env.assertEqual(updated[1], "doc:1")

    conn.execute_command("JSON.DEL", "doc:1", "$.vec")
    waitForIndex(env, "idx_tq_json")
    env.assertEqual(env.cmd(
        "FT.SEARCH", "idx_tq_json", "*=>[KNN 1 @vec $blob]",
        "PARAMS", "2", "blob", updated_query, "DIALECT", "2",
    )[0], 0)
    conn.execute_command("JSON.SET", "doc:1", "$", '{"vec":[1.0,0.0]}')
    waitForIndex(env, "idx_tq_json")
    env.assertEqual(env.cmd(
        "FT.SEARCH", "idx_tq_json", "*=>[KNN 1 @vec $blob]",
        "PARAMS", "2", "blob", query, "DIALECT", "2",
    )[1], "doc:1")
    conn.execute_command("DEL", "doc:1")
    waitForIndex(env, "idx_tq_json")
    env.assertEqual(env.cmd(
        "FT.SEARCH", "idx_tq_json", "*=>[KNN 1 @vec $blob]",
        "PARAMS", "2", "blob", query, "DIALECT", "2",
    )[0], 0)

    env.expect(
        "FT.CREATE", "idx_tq_json_multi", "ON", "JSON", "SCHEMA", "$.vecs[*]", "AS", "vec", "VECTOR", "HNSW", len(params), *params
    ).error().contains("TQ compression does not support multi-value vectors")
    conn.execute_command("FT.DROPINDEX", "idx_tq_json", "DD")


def test_tq_info():
    env = Env(protocol=3, moduleArgs="DEFAULT_DIALECT 2")
    enable_unstable_features(env)
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
    env.assertEqual(attr["ef_runtime"], 50)
    env.assertEqual(attr["compression"], "TQ8")
    _assert_dense_tq_identity(env, attr, dim=2, bits=8)
    # Diagnostic identity is reported, but there is still no public backend selector.
    env.assertNotContains("bits", attr)
    env.assertNotContains("projections", attr)
    env.assertNotContains("seed", attr)
    env.assertNotContains("rotation", attr)
    env.assertNotContains("tq_backend", attr)
    conn.execute_command("FT.DROPINDEX", "idx_tq_info", "DD")


def test_tq_knn_and_range_query():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    enable_unstable_features(env)
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

    # TQ cosine results use the standard approximate 1-cosine distance scale. A 0.5 radius
    # includes the aligned vector and excludes the orthogonal and opposite vectors.
    range_res = env.cmd(
        "FT.SEARCH", "idx_tq_knn", "@v:[VECTOR_RANGE 0.5 $blob]=>{$yield_distance_as: dist}",
        "PARAMS", "2", "blob", query,
        "SORTBY", "dist",
        "RETURN", "1", "dist",
        "DIALECT", "2",
    )
    env.assertEqual(range_res[0], 1)
    env.assertEqual(range_res[1], "doc:1")
    conn.execute_command("FT.DROPINDEX", "idx_tq_knn", "DD")


def test_tq_hash_update_delete_and_reindex():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    params = _tq_schema_params(dim=8)
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_hash_lifecycle", "SCHEMA", "v", "VECTOR", "HNSW",
               len(params), *params).ok()

    first = np.zeros(8, dtype=np.float32)
    first[0] = 1.0
    second = np.zeros(8, dtype=np.float32)
    second[1] = 1.0

    def knn(query):
        return env.cmd(
            "FT.SEARCH", "idx_tq_hash_lifecycle", "*=>[KNN 1 @v $blob AS dist]",
            "PARAMS", "2", "blob", query.tobytes(),
            "SORTBY", "dist", "RETURN", "1", "dist", "DIALECT", "2",
        )

    conn.execute_command("HSET", "doc:1", "v", first.tobytes())
    waitForIndex(env, "idx_tq_hash_lifecycle")
    env.assertEqual(knn(first)[1], "doc:1")

    conn.execute_command("HSET", "doc:1", "v", second.tobytes())
    waitForIndex(env, "idx_tq_hash_lifecycle")
    env.assertEqual(knn(second)[1], "doc:1")

    conn.execute_command("HDEL", "doc:1", "v")
    waitForIndex(env, "idx_tq_hash_lifecycle")
    env.assertEqual(knn(second)[0], 0)

    conn.execute_command("HSET", "doc:1", "v", first.tobytes())
    waitForIndex(env, "idx_tq_hash_lifecycle")
    env.assertEqual(knn(first)[1], "doc:1")

    conn.execute_command("DEL", "doc:1")
    waitForIndex(env, "idx_tq_hash_lifecycle")
    env.assertEqual(knn(first)[0], 0)


def test_tq_compression_variants():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)

    base = np.zeros(8, dtype=np.float32)
    base[0] = 1.0
    other = np.zeros(8, dtype=np.float32)
    other[1] = 1.0

    for compression in ("TQ2", "TQ4", "TQ8"):
        index_name = f"idx_{compression.lower()}"
        params = _tq_schema_params(dim=8, compression=compression)
        conn.flushall()
        env.expect("FT.CREATE", index_name, "SCHEMA", "v", "VECTOR", "HNSW",
                   len(params), *params).ok()
        waitForIndex(env, index_name)

        info = to_dict(env.executeCommand("FT.INFO", index_name))
        attr = to_dict(info["attributes"][0])
        env.assertEqual(attr["compression"], compression)

        conn.execute_command("HSET", "doc:1", "v", base.tobytes())
        conn.execute_command("HSET", "doc:2", "v", other.tobytes())
        waitForIndex(env, index_name)

        res = env.cmd(
            "FT.SEARCH", index_name, "*=>[KNN 2 @v $blob AS dist]",
            "PARAMS", "2", "blob", base.tobytes(),
            "SORTBY", "dist",
            "RETURN", "1", "dist",
            "DIALECT", "2",
        )
        env.assertEqual(res[0], 2)
        env.assertEqual(res[1], "doc:1")
        conn.execute_command("FT.DROPINDEX", index_name, "DD")


def test_tq_inner_product_query():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)

    params = _tq_schema_params(dim=8, metric="IP")
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_ip", "SCHEMA", "v", "VECTOR", "HNSW",
               len(params), *params).ok()
    waitForIndex(env, "idx_tq_ip")

    query = np.zeros(8, dtype=np.float32)
    query[0] = 1.0
    aligned = query.copy()
    aligned[0] = 2.0
    orthogonal = np.zeros(8, dtype=np.float32)
    orthogonal[1] = 1.0
    opposite = -query

    conn.execute_command("HSET", "doc:aligned", "v", aligned.tobytes())
    conn.execute_command("HSET", "doc:orthogonal", "v", orthogonal.tobytes())
    conn.execute_command("HSET", "doc:opposite", "v", opposite.tobytes())
    waitForIndex(env, "idx_tq_ip")

    res = env.cmd(
        "FT.SEARCH", "idx_tq_ip", "*=>[KNN 3 @v $blob AS dist]",
        "PARAMS", "2", "blob", query.tobytes(),
        "SORTBY", "dist",
        "RETURN", "1", "dist",
        "DIALECT", "2",
    )
    env.assertEqual(res[0], 3)
    env.assertEqual(res[1], "doc:aligned")
    env.assertEqual(res[3], "doc:orthogonal")
    env.assertEqual(res[5], "doc:opposite")
    conn.execute_command("FT.DROPINDEX", "idx_tq_ip", "DD")


def test_tq_hybrid_adhoc_scores_frontend_and_backend_hash_vectors():
    env = Env(moduleArgs="DEFAULT_DIALECT 2 WORKERS 1 MIN_OPERATION_WORKERS 0",
              enableDebugCommand=True)
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    dim = 16
    vectors = _tq_split_tier_vectors(dim)
    query = np.zeros(dim, dtype=np.float32)
    query[0] = 2.75
    query[1] = -0.5

    for metric in ("COSINE", "IP"):
        index_name = f"idx_tq_hybrid_hash_{metric.lower()}"
        params = _tq_hnsw_schema_params(dim=dim, metric=metric, compression="TQ4")
        conn.flushall()
        env.expect("FT.CREATE", index_name, "SCHEMA", "v", "VECTOR", "HNSW",
                   len(params), *params, "n", "NUMERIC").ok()

        for i in range(2):
            conn.execute_command("HSET", f"doc:{i}", "v", vectors[i].tobytes(), "n", i)
        waitForIndex(env, index_name)
        env.expect(debug_cmd(), "WORKERS", "DRAIN").ok()
        env.expect(debug_cmd(), "WORKERS", "PAUSE").ok()
        workers_paused = True
        try:
            for i in range(2, 4):
                conn.execute_command("HSET", f"doc:{i}", "v", vectors[i].tobytes(), "n", i)
            info = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", index_name, "v"))
            env.assertGreater(to_dict(info["FRONTEND_INDEX"])["INDEX_SIZE"], 0)
            env.assertGreater(to_dict(info["BACKEND_INDEX"])["INDEX_SIZE"], 0)
            queued = _queue_tq_hybrid_queries(env, index_name, query)
            env.expect(debug_cmd(), "WORKERS", "RESUME").ok()
            workers_paused = False
            _assert_tq_hybrid_scores(env, queued)
        finally:
            if workers_paused:
                env.expect(debug_cmd(), "WORKERS", "RESUME").ok()
            env.expect(debug_cmd(), "WORKERS", "DRAIN").ok()


@skip(no_json=True)
def test_tq_hybrid_adhoc_scores_frontend_and_backend_json_vectors():
    env = Env(moduleArgs="DEFAULT_DIALECT 2 WORKERS 1 MIN_OPERATION_WORKERS 0",
              enableDebugCommand=True)
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    dim = 16
    params = _tq_hnsw_schema_params(dim=dim, metric="COSINE", compression="TQ4")
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_hybrid_json", "ON", "JSON", "SCHEMA",
               "$.v", "AS", "v", "VECTOR", "HNSW", len(params), *params,
               "$.n", "AS", "n", "NUMERIC").ok()
    vectors = _tq_split_tier_vectors(dim)
    query = np.zeros(dim, dtype=np.float32)
    query[0] = 2.75
    query[1] = -0.5

    for i in range(2):
        conn.execute_command("JSON.SET", f"doc:{i}", "$",
                             json.dumps({"v": vectors[i].tolist(), "n": i}))
    waitForIndex(env, "idx_tq_hybrid_json")
    env.expect(debug_cmd(), "WORKERS", "DRAIN").ok()
    env.expect(debug_cmd(), "WORKERS", "PAUSE").ok()
    workers_paused = True
    try:
        for i in range(2, 4):
            conn.execute_command("JSON.SET", f"doc:{i}", "$",
                                 json.dumps({"v": vectors[i].tolist(), "n": i}))
        info = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx_tq_hybrid_json", "v"))
        env.assertGreater(to_dict(info["FRONTEND_INDEX"])["INDEX_SIZE"], 0)
        env.assertGreater(to_dict(info["BACKEND_INDEX"])["INDEX_SIZE"], 0)
        queued = _queue_tq_hybrid_queries(env, "idx_tq_hybrid_json", query)
        env.expect(debug_cmd(), "WORKERS", "RESUME").ok()
        workers_paused = False
        _assert_tq_hybrid_scores(env, queued)
    finally:
        if workers_paused:
            env.expect(debug_cmd(), "WORKERS", "RESUME").ok()
        env.expect(debug_cmd(), "WORKERS", "DRAIN").ok()


@skip(cluster=True)
def test_tq_rdb_round_trip():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)

    params = _tq_hnsw_schema_params(dim=8, compression="TQ8")
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_rdb", "SCHEMA", "v", "VECTOR", "HNSW",
               len(params), *params).ok()

    query = np.zeros(8, dtype=np.float32)
    query[0] = 1.0
    orthogonal = np.zeros(8, dtype=np.float32)
    orthogonal[1] = 1.0
    conn.execute_command("HSET", "doc:1", "v", query.tobytes())
    conn.execute_command("HSET", "doc:2", "v", orthogonal.tobytes())
    waitForIndex(env, "idx_tq_rdb")

    before = env.cmd(
        "FT.SEARCH", "idx_tq_rdb", "*=>[KNN 2 @v $blob AS dist]",
        "PARAMS", "2", "blob", query.tobytes(),
        "SORTBY", "dist",
        "RETURN", "1", "dist",
        "DIALECT", "2",
    )
    before_info = to_dict(env.executeCommand("FT.INFO", "idx_tq_rdb"))
    before_attr = to_dict(before_info["attributes"][0])
    _assert_dense_tq_identity(env, before_attr, dim=8, bits=8)
    # The rollout flag gates new schema creation, not loading a known persisted identity.
    env.expect(config_cmd(), "SET", "ENABLE_UNSTABLE_FEATURES", "false").ok()

    for _ in env.reloadingIterator():
        info = to_dict(env.executeCommand("FT.INFO", "idx_tq_rdb"))
        attr = to_dict(info["attributes"][0])
        env.assertEqual(attr["compression"], "TQ8")
        _assert_dense_tq_identity(env, attr, dim=8, bits=8)
        for key in (
            "tq_rdb_marker", "tq_codec_version", "tq_profile", "tq_payload_layout",
            "tq_rotation", "tq_qjl", "tq_construction_score", "tq_metric_contract",
            "tq_projections", "tq_seed", "tq_payload_bytes",
        ):
            env.assertEqual(attr[key], before_attr[key])

        res = env.cmd(
            "FT.SEARCH", "idx_tq_rdb", "*=>[KNN 2 @v $blob AS dist]",
            "PARAMS", "2", "blob", query.tobytes(),
            "SORTBY", "dist",
            "RETURN", "1", "dist",
            "DIALECT", "2",
        )
        env.assertEqual(res, before)


def test_tq_rejects_non_float32():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    enable_unstable_features(env)
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
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)

    params = _tq_schema_params(metric="L2")
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_l2", "SCHEMA", "v", "VECTOR", "HNSW", len(params), *params) \
        .error().contains("TQ compression with DISTANCE_METRIC L2 is not yet supported; use COSINE or IP")


def test_tq_accepts_odd_dimensions_and_rejects_dimension_one():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)

    params = _tq_schema_params(dim=3)
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_odd_dim", "SCHEMA", "v", "VECTOR", "HNSW",
               len(params), *params).ok()
    conn.execute_command("FT.DROPINDEX", "idx_tq_odd_dim", "DD")

    params = _tq_schema_params(dim=1)
    env.expect("FT.CREATE", "idx_tq_dim_one", "SCHEMA", "v", "VECTOR", "HNSW",
               len(params), *params) \
        .error().contains("TQ compression requires vector dimension >= 2")


def test_tq_rejects_unknown_compression():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)

    params = _tq_schema_params(compression="TQ16")
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_bad", "SCHEMA", "v", "VECTOR", "HNSW", len(params), *params) \
        .error().contains("COMPRESSION")


def test_tq_does_not_expose_model_backend_selector():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    enable_unstable_features(env)
    params = _tq_schema_params() + ["TQ_PROFILE", "FastStructuredV1"]
    env.expect("FT.CREATE", "idx_tq_profile", "SCHEMA", "v", "VECTOR", "HNSW",
               len(params), *params).error().contains("TQ_PROFILE")


def test_tq_rejects_legacy_algorithm_names():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    conn.flushall()

    params = _tq_schema_params()
    for legacy in ("TQ-FLAT", "TQ-HNSW"):
        env.expect("FT.CREATE", "idx_tq_legacy", "SCHEMA", "v", "VECTOR", legacy, len(params), *params) \
            .error().contains("Bad arguments for vector similarity algorithm")


def test_tq_hnsw_uses_tiered_flat_buffer():
    env = Env(moduleArgs="DEFAULT_DIALECT 2 WORKERS 1")
    enable_unstable_features(env)
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


@skip(cluster=True)
def test_tq_tiered_update_repair_and_gc():
    env = Env(protocol=3, moduleArgs=(
        "DEFAULT_DIALECT 2 WORKERS 1 MIN_OPERATION_WORKERS 0 FORK_GC_RUN_INTERVAL 50000"
    ), enableDebugCommand=True)
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    params = _tq_hnsw_schema_params(dim=8, compression="TQ8")
    conn.flushall()
    env.expect("FT.CREATE", "idx_tq_gc", "SCHEMA", "v", "VECTOR", "HNSW",
               len(params), *params).ok()

    for i in range(32):
        vector = np.random.default_rng(i).normal(size=8).astype(np.float32)
        conn.execute_command("HSET", f"doc:{i}", "v", vector.tobytes())
    env.expect(debug_cmd(), "WORKERS", "DRAIN").ok()
    env.expect(debug_cmd(), "WORKERS", "PAUSE").ok()
    try:
        replacement = np.zeros(8, dtype=np.float32)
        replacement[0] = 4.0
        conn.execute_command("HSET", "doc:0", "v", replacement.tobytes())
        waitForIndex(env, "idx_tq_gc")
        debug_info = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx_tq_gc", "v"))
        env.assertEqual(to_dict(debug_info["BACKEND_INDEX"])["NUMBER_OF_MARKED_DELETED"], 1)
        env.assertEqual(to_dict(debug_info["FRONTEND_INDEX"])["INDEX_SIZE"], 1)
    finally:
        env.expect(debug_cmd(), "WORKERS", "RESUME").ok()

    env.expect(debug_cmd(), "WORKERS", "DRAIN").ok()
    env.expect(config_cmd(), "SET", "FORK_GC_CLEAN_THRESHOLD", "0").ok()
    forceInvokeGC(env, "idx_tq_gc", timeout=100000)
    debug_info = to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx_tq_gc", "v"))
    env.assertEqual(to_dict(debug_info["FRONTEND_INDEX"])["INDEX_SIZE"], 0)
    env.assertEqual(to_dict(debug_info["BACKEND_INDEX"])["NUMBER_OF_MARKED_DELETED"], 0)

    result = env.cmd(
        "FT.SEARCH", "idx_tq_gc", "*=>[KNN 1 @v $blob AS dist]",
        "PARAMS", "2", "blob", replacement.tobytes(),
        "SORTBY", "dist", "RETURN", "1", "dist", "DIALECT", "2",
    )
    env.assertEqual(result["total_results"], 1)
    env.assertEqual(result["results"][0]["id"], "doc:0")
