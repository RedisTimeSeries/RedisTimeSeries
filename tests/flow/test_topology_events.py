import time
from includes import *
from utils import migrate_slots_back_and_forth, dump_node_infocluster, dump_infocluster, shard_id_of


def test_legacy():
    env = Env(shardsCount=3, decodeResponses=True, moduleArgs="ts-topology-events no")
    if not env.isCluster():
        env.skip()
    dump_infocluster(env)
    time.sleep(2)

    def validate_result(conn):
        print(f"shard {shard_id_of(env, conn)}")
        dump_node_infocluster(conn)
        return True

    migrate_slots_back_and_forth(env, validate_result, validate_all_nodes=True)


def test_asm():
    env = Env(shardsCount=3, decodeResponses=True, skipRefreshCluster=True)
    if not env.isCluster():
        env.skip()
    dump_infocluster(env)
    time.sleep(2)

    def validate_result(conn):
        print(f"shard {shard_id_of(env, conn)}")
        dump_node_infocluster(conn)
        return True

    migrate_slots_back_and_forth(env, validate_result, validate_all_nodes=True)
