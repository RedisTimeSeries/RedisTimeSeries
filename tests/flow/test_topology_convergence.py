# RLTest requires an active Env even for mocked tests; without it the pinned
# runner reports "Environment destroyed" after a successful return.
from unittest.mock import MagicMock, Mock, patch

from test_topology_events import post_failover
from utils import ClusterNode, SlotRange


def topology(node_id, port):
    return {node_id: ClusterNode(id=node_id, ip="127.0.0.1", port=port,
                                flags={"master"}, slots={SlotRange(0, 16383)})}


def test_post_failover_rejects_agreement_on_stale_module_topology(env):
    old = topology("old", 6379)
    current = topology("promoted", 6380)
    connection = MagicMock()
    connection.__enter__.return_value = connection
    with patch("test_topology_events.wait_for_valid_cluster", return_value=current):
        with patch("test_topology_events.redis.Redis", return_value=connection):
            with patch("test_topology_events.validate_cluster", return_value=current):
                with patch("test_topology_events.ts_cluster_from_conn", side_effect=[old, current]) as read:
                    with patch("test_topology_events.time.sleep") as sleep:
                        post_failover(Mock(shardsCount=1))
    assert read.call_count == 2
    sleep.assert_called_once_with(0.2)
    connection.__exit__.assert_called_once()


def test_post_failover_refreshes_transient_topology_and_polls_added_node(env):
    current = topology("promoted", 6380)
    replica = ClusterNode(id="old", ip="127.0.0.1", port=6379,
                          flags={"slave"}, slots=set(), master="promoted")
    stable = dict(current, old=replica)
    slotless = ClusterNode(id="old", ip="127.0.0.1", port=6379,
                           flags={"master"}, slots=set())
    transient = dict(current, old=slotless)
    original, promoted = MagicMock(), MagicMock()
    original.__enter__.return_value = original
    promoted.__enter__.return_value = promoted
    env = Mock(shardsCount=1)
    env.getConnection.return_value = original
    # Bootstrap can contain a slotless old master. Redis first disagrees,
    # then agrees on that transient state, then settles. LibMR on the promoted
    # node lags for one additional poll, even though the original node is ready.
    with patch("test_topology_events.wait_for_valid_cluster", return_value=transient):
        with patch("test_topology_events.redis.Redis", side_effect=[promoted, original]):
            with patch("test_topology_events.validate_cluster", side_effect=[
                    transient, stable, transient, transient, stable, stable, stable, stable]) as redis_read:
                with patch("test_topology_events.ts_cluster_from_conn", side_effect=[
                        transient, current, current, current]) as module_read:
                    with patch("test_topology_events.time.sleep") as sleep:
                        post_failover(env)
    assert redis_read.call_count == 8
    assert [call[0][0] for call in module_read.call_args_list] == [promoted, original, promoted, original]
    assert sleep.call_count == 3
    original.__exit__.assert_called_once()
    promoted.__exit__.assert_called_once()


def down_state_fixture():
    victim = Mock()
    survivors = [Mock(), Mock()]
    env = Mock()
    env.envRunner.shards = [survivors[0], victim, survivors[1]]
    return env, victim, [shard.getConnection.return_value for shard in survivors]


def test_down_state_retries_clusterdown_and_requires_all_views(env):
    from test_topology_events import CLUSTERDOWN_ERROR, redis, wait_for_down_state_view_agreement
    cluster_env, victim, conns = down_state_fixture()
    current = {"node": ("127.0.0.1", 6379)}
    stale = {"old": ("127.0.0.1", 6380)}
    # Fail the second survivor's read, then disagree, then finally agree.
    with patch("test_topology_events.redis_master_addrs", return_value=current):
        with patch("test_topology_events.ts_master_addrs", side_effect=[
                current, redis.exceptions.ResponseError(CLUSTERDOWN_ERROR),
                current, stale, current, current]) as read:
            with patch("test_topology_events.time.sleep") as sleep:
                wait_for_down_state_view_agreement(cluster_env, victim)
    assert [call.args[0] for call in read.call_args_list] == conns * 3
    assert sleep.call_count == 2
    victim.getConnection.assert_not_called()


def test_down_state_persistent_clusterdown_times_out(env):
    from test_topology_events import CLUSTERDOWN_ERROR, redis, wait_for_down_state_view_agreement
    cluster_env, victim, conns = down_state_fixture()
    error = redis.exceptions.ResponseError(CLUSTERDOWN_ERROR)
    with patch("test_topology_events.redis_master_addrs", return_value={}):
        with patch("test_topology_events.ts_master_addrs", side_effect=error):
            with patch("test_topology_events.time.time", side_effect=[0, 0, 11]):
                with patch("test_topology_events.get_timeout", return_value=10):
                    with patch("test_topology_events.time.sleep") as sleep:
                        with patch("test_topology_events.dump_node_cluster_nodes") as dump_redis:
                            with patch("test_topology_events.dump_node_infocluster", side_effect=error) as dump_ts:
                                try:
                                    wait_for_down_state_view_agreement(cluster_env, victim)
                                except AssertionError as failure:
                                    assert "did not agree" in str(failure)
                                else:
                                    assert False, "Persistent CLUSTERDOWN must time out"
    sleep.assert_called_once_with(0.2)
    assert [call.args[0] for call in dump_redis.call_args_list] == conns
    assert [call.args[0] for call in dump_ts.call_args_list] == conns


def test_down_state_propagates_unexpected_errors(env):
    from test_topology_events import redis, wait_for_down_state_view_agreement
    cluster_env, victim, _ = down_state_fixture()
    error = redis.exceptions.ResponseError("unexpected error")
    with patch("test_topology_events.redis_master_addrs", return_value={}):
        with patch("test_topology_events.ts_master_addrs", side_effect=error):
            with patch("test_topology_events.time.sleep") as sleep:
                try:
                    wait_for_down_state_view_agreement(cluster_env, victim)
                except redis.exceptions.ResponseError as failure:
                    assert failure is error
                else:
                    assert False, "Unexpected errors must propagate"
    sleep.assert_not_called()


def test_node_down_cycle_restarts_victim_after_failed_check(env):
    from contextlib import ExitStack
    from threading import Event
    from test_topology_events import test_take_node_down_and_up
    cluster_env = Mock()
    untouchable, victim, other = Mock(), Mock(), Mock()
    cluster_env.envRunner.shards = [untouchable, victim, other]
    error = AssertionError("down-state check failed")
    with ExitStack() as stack:
        stack.enter_context(patch("test_topology_events.Env", return_value=cluster_env))
        for name in ("skip_if_needed", "wait_for_valid_cluster", "wait_for_valid_ts_infocluster",
                     "fill_some_data", "time.sleep"):
            stack.enter_context(patch("test_topology_events." + name))
        stack.enter_context(patch("test_topology_events.wait_for_down_state_view_agreement", side_effect=error))
        stack.enter_context(patch("test_topology_events.run_until_first_finishes",
                                  side_effect=lambda validate, cycle: cycle(Event())))
        try:
            test_take_node_down_and_up()
        except AssertionError as failure:
            assert failure is error
        else:
            assert False, "The failed check must propagate"
    victim.stopEnv.assert_called_once_with()
    victim.startEnv.assert_called_once_with()
    untouchable.stopEnv.assert_not_called()
    other.stopEnv.assert_not_called()
