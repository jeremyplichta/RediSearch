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


def test_tq_flat_info():
    env = Env(protocol=3, moduleArgs="DEFAULT_DIALECT 2")

    params = _tq_schema_params()
    env.expect("FT.CREATE", "idx", "SCHEMA", "vec", "VECTOR", "TQ-FLAT", len(params), *params).ok()
    waitForIndex(env, "idx")

    info = to_dict(env.executeCommand("FT.INFO", "idx"))
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


def test_tq_flat_knn_and_range_query():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    params = _tq_schema_params()
    env.expect("FT.CREATE", "idx", "SCHEMA", "v", "VECTOR", "TQ-FLAT", len(params), *params).ok()
    waitForIndex(env, "idx")

    conn.execute_command("HSET", "doc:1", "v", np.array([0.0, 0.0], dtype=np.float32).tobytes())
    conn.execute_command("HSET", "doc:2", "v", np.array([1.0, 0.0], dtype=np.float32).tobytes())
    conn.execute_command("HSET", "doc:3", "v", np.array([2.0, 0.0], dtype=np.float32).tobytes())

    query = np.array([0.0, 0.0], dtype=np.float32).tobytes()

    knn = env.cmd(
        "FT.SEARCH", "idx", "*=>[KNN 3 @v $blob AS dist]",
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
        "FT.SEARCH", "idx", "@v:[VECTOR_RANGE 1.01 $blob]=>{$yield_distance_as: dist}",
        "PARAMS", "2", "blob", query,
        "SORTBY", "dist",
        "RETURN", "1", "dist",
        "DIALECT", "2",
    )
    env.assertEqual(range_res[0], 2)
    env.assertEqual(range_res[1], "doc:1")
    env.assertEqual(range_res[3], "doc:2")


def test_tq_flat_rejects_non_float32():
    env = Env(moduleArgs="DEFAULT_DIALECT 2")

    params = [
        "TYPE", "FLOAT16",
        "DIM", 2,
        "DISTANCE_METRIC", "L2",
        "BITS", 8,
        "PROJECTIONS", 4,
    ]
    env.expect("FT.CREATE", "idx", "SCHEMA", "v", "VECTOR", "TQ-FLAT", len(params), *params) \
        .error().contains("TQ-FLAT only supports FLOAT32 vectors")
