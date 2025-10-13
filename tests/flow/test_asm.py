from includes import Env
from dataclasses import dataclass
import re
import time


@dataclass(frozen=True)
class SlotRange:
    start: int
    end: int

    @staticmethod
    def from_str(s: str):
        start, end = map(int, s.split("-"))
        assert 0 <= start <= end < 2**14
        return SlotRange(start, end)


@dataclass
class ClusterNode:
    id: str
    ip: str
    port: int
    cport: int  # cluster bus port
    hostname: str | None
    flags: set[str]
    master: str  # Either this node's primary replica or '-'
    ping_sent: int
    pong_recv: int
    config_epoch: int
    link_state: bool  # True: connected, False: disconnected
    slots: set[SlotRange]

    @staticmethod
    def from_str(s: str):
        # <id> <ip:port @cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot-range> [<slot-range>> ...]
        # e.g. a5e5068caceb2adabed3ed657b21b627deadbfaa 127.0.0.1:6379 @16379 master - 0 1760353421847 1 connected 1000-2000 10000-15000
        parts = s.split()
        node_id, addr, flags, master, ping_sent, pong_recv, config_epoch, link_state, *slots = parts
        match = re.match(r"^(?P<ip>[^:]+):(?P<port>\d+)@(?P<cport>\d+)(?:,(?P<hostname>.+))?$", addr)
        ip = match.group("ip")
        port = int(match.group("port"))
        cport = int(match.group("cport"))
        hostname = match.group("hostname")

        return ClusterNode(
            id=node_id,
            ip=ip,
            port=port,
            cport=cport,
            hostname=hostname,
            flags=set(flags.split(",")),
            master=master,
            ping_sent=int(ping_sent),
            pong_recv=int(pong_recv),
            config_epoch=int(config_epoch),
            link_state=link_state == "connected",
            slots={SlotRange.from_str(s) for s in slots},
        )


def test_asm_without_data():
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":  # TODO: convert to a proper fixture
        env.skip()

    migrate_slots_back_and_forth(env)


def migrate_slots_back_and_forth(env):
    def cluster_node_of(conn) -> ClusterNode:
        (result,) = (
            cluster_node
            for line in conn.execute_command("cluster", "nodes").splitlines()
            if "myself" in (cluster_node := ClusterNode.from_str(line)).flags
        )
        return result

    def middle_slot_range(slot_range: SlotRange) -> SlotRange:
        third = (slot_range.end - slot_range.start) // 3
        return SlotRange(slot_range.start + third, slot_range.end - third)

    def cantorized_slot_set(slot_range: SlotRange) -> set[SlotRange]:  # https://en.wikipedia.org/wiki/Cantor_set ;)
        middle = middle_slot_range(slot_range)
        return {SlotRange(slot_range.start, middle.start - 1), SlotRange(middle.end + 1, slot_range.end)}

    first_conn, second_conn = env.getConnection(0), env.getConnection(1)
    # Store some original values to be used throughout the test
    (original_first_slot_range,) = cluster_node_of(first_conn).slots
    (original_second_slot_range,) = cluster_node_of(second_conn).slots
    middle_of_original_first = middle_slot_range(original_first_slot_range)
    middle_of_original_second = middle_slot_range(original_second_slot_range)

    import_slots(first_conn, middle_of_original_second)
    assert cluster_node_of(first_conn).slots == {original_first_slot_range, middle_of_original_second}
    assert cluster_node_of(second_conn).slots == cantorized_slot_set(original_second_slot_range)

    import_slots(second_conn, middle_of_original_second)
    assert cluster_node_of(first_conn).slots == {original_first_slot_range}
    assert cluster_node_of(second_conn).slots == {original_second_slot_range}

    import_slots(second_conn, middle_of_original_first)
    assert cluster_node_of(second_conn).slots == {original_second_slot_range, middle_of_original_first}
    assert cluster_node_of(first_conn).slots == cantorized_slot_set(original_first_slot_range)

    import_slots(first_conn, middle_of_original_first)
    assert cluster_node_of(first_conn).slots == {original_first_slot_range}
    assert cluster_node_of(second_conn).slots == {original_second_slot_range}


def import_slots(conn, slot_range: SlotRange):
    task_id = conn.execute_command("CLUSTER", "MIGRATION", "IMPORT", slot_range.start, slot_range.end)
    start_time = time.time()
    timeout = 5
    while time.time() - start_time < timeout:
        (migration_status,) = conn.execute_command("CLUSTER", "MIGRATION", "STATUS", "ID", task_id)
        migration_status = {key: value for key, value in zip(migration_status[0::2], migration_status[1::2])}
        if migration_status["state"] == "completed":
            break
        time.sleep(0.1)
    else:
        raise TimeoutError
