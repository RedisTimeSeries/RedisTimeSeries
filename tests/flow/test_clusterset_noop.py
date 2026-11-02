from includes import Env


def _run_id(conn):
    # INFOCLUSTER layout: ["MyId", <id>, "MyRunId", <run id>, ...]
    return conn.execute_command("timeseries.INFOCLUSTER")[3]


def _long_form_clusterset(my_port, second_shard_port):
    return [
        "HASHFUNC", "CRC16", "NUMSLOTS", "16384", "MYID", "1", "RANGES", "2",
        "SHARD", "1", "SLOTRANGE", "0", "8191",
        "ADDR", f"@127.0.0.1:{my_port}", "MASTER",
        "SHARD", "2", "SLOTRANGE", "8192", "16383",
        "ADDR", f"@127.0.0.1:{second_shard_port}", "MASTER",
    ]


def test_identical_long_form_clusterset_is_noop():
    env = Env(decodeResponses=True, moduleArgs="ts-topology-events no")
    env.skipOnCluster()
    env.skipOnSlave()

    conn = env.getConnection()
    try:
        conn.execute_command("debug", "MARK-INTERNAL-CLIENT")
    except Exception:
        pass  # INFOCLUSTER is not internal-only on older Redis versions.

    my_port = conn.connection_pool.connection_kwargs["port"]
    initial = _long_form_clusterset(my_port, my_port + 1)

    assert conn.execute_command("timeseries.CLUSTERSET", *initial) == "OK"
    run_id = _run_id(conn)

    # LibMR generates a run ID when it builds the cluster, so preserving that
    # externally visible ID proves the DMC re-broadcast did not rebuild it
    # (and therefore did not drop connections or abort in-flight executions).
    assert conn.execute_command("timeseries.CLUSTERSET", *initial) == "OK"
    assert _run_id(conn) == run_id

    # A real endpoint change must still replace it.
    changed = _long_form_clusterset(my_port, my_port + 2)
    assert conn.execute_command("timeseries.CLUSTERSET", *changed) == "OK"
    assert _run_id(conn) != run_id
