import time

from includes import *
from utils import slot_table


def _decode_info(info):
    if isinstance(info, bytes):
        return info.decode()
    if isinstance(info, str):
        return info
    return None


def _info_section(conn, section):
    info = conn.execute_command("INFO")
    text = _decode_info(info)
    if text is None:
        return {}
    result = {}
    in_section = False
    for line in text.splitlines():
        if line.startswith("# "):
            in_section = line[2:] == section
            continue
        if in_section and ":" in line:
            key, val = line.split(":", 1)
            result[key] = val
    return result


def _wait_for_updates(conn, min_updates, timeout_sec=2.0):
    start = time.time()
    while time.time() - start < timeout_sec:
        info = _info_section(conn, "shard_directory")
        if int(info.get("directory_updates_received", "0")) >= min_updates:
            return
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for directory updates")


def _local_slot_range(conn):
    raw = conn.execute_command("CLUSTER", "NODES")
    text = _decode_info(raw)
    assert text is not None
    for line in text.splitlines():
        if "myself" not in line or "master" not in line:
            continue
        parts = line.split()
        slots = [p for p in parts[8:] if "-" in p]
        assert slots
        start, end = slots[0].split("-")
        return int(start), int(end)
    raise AssertionError("No local slot range found")


def _key_for_shard(conn, suffix):
    start, _ = _local_slot_range(conn)
    tag = slot_table[start]
    return f"ts:{{{tag}}}:{suffix}"


def test_cluster_mrange_prunes_shards(env):
    if env.shardsCount < 3 or not env.is_cluster():
        env.skip()

    conns = [env.getConnection(i) for i in range(env.shardsCount)]
    coordinator = conns[0]

    key_a0 = _key_for_shard(conns[0], "tenant_a_0")
    key_a1 = _key_for_shard(conns[1], "tenant_a_1")
    key_b2 = _key_for_shard(conns[2], "tenant_b_2")

    conns[0].execute_command("TS.CREATE", key_a0, "LABELS", "tenant", "a")
    conns[1].execute_command("TS.CREATE", key_a1, "LABELS", "tenant", "a")
    conns[2].execute_command("TS.CREATE", key_b2, "LABELS", "tenant", "b")

    conns[0].execute_command("TS.ADD", key_a0, 1, 10)
    conns[1].execute_command("TS.ADD", key_a1, 1, 20)
    conns[2].execute_command("TS.ADD", key_b2, 1, 30)

    _wait_for_updates(coordinator, 1)

    before = _info_section(coordinator, "shard_directory")
    before_pruned = int(before.get("mrangecoord_pruned", "0"))

    res = coordinator.execute_command("TS.MRANGE", "-", "+", "FILTER", "tenant=a")
    if isinstance(res, dict):
        keys = {decode_if_needed(k) for k in res.keys()}
    else:
        keys = {decode_if_needed(item[0]) for item in res}
    assert keys == {key_a0, key_a1}

    after = _info_section(coordinator, "shard_directory")
    after_pruned = int(after.get("mrangecoord_pruned", "0"))
    avg_targets = float(after.get("mrangecoord_targets_avg", "0"))
    assert after_pruned == before_pruned + 1
    assert avg_targets >= 2.0


def test_cluster_mget_queryindex_pruning(env):
    if env.shardsCount < 3 or not env.is_cluster():
        env.skip()

    conns = [env.getConnection(i) for i in range(env.shardsCount)]
    coordinator = conns[0]

    key_a0 = _key_for_shard(conns[0], "tenant_a_0")
    key_a1 = _key_for_shard(conns[1], "tenant_a_1")
    key_b2 = _key_for_shard(conns[2], "tenant_b_2")

    conns[0].execute_command("TS.CREATE", key_a0, "LABELS", "tenant", "a")
    conns[1].execute_command("TS.CREATE", key_a1, "LABELS", "tenant", "a")
    conns[2].execute_command("TS.CREATE", key_b2, "LABELS", "tenant", "b")

    conns[0].execute_command("TS.ADD", key_a0, 1, 10)
    conns[1].execute_command("TS.ADD", key_a1, 1, 20)
    conns[2].execute_command("TS.ADD", key_b2, 1, 30)

    _wait_for_updates(coordinator, 1)

    before = _info_section(coordinator, "shard_directory")
    before_pruned = int(before.get("mrangecoord_pruned", "0"))

    res = coordinator.execute_command("TS.QUERYINDEX", "tenant=a")
    decoded = decode_if_needed(res)
    if isinstance(decoded, (set, list, tuple)):
        assert set(decoded) == {key_a0, key_a1}
    else:
        assert decoded == {key_a0, key_a1}

    res = coordinator.execute_command("TS.MGET", "FILTER", "tenant=a")
    if isinstance(res, dict):
        keys = {decode_if_needed(k) for k in res.keys()}
    else:
        keys = {decode_if_needed(item[0]) for item in res}
    assert keys == {key_a0, key_a1}

    after = _info_section(coordinator, "shard_directory")
    after_pruned = int(after.get("mrangecoord_pruned", "0"))
    assert after_pruned >= before_pruned + 2
