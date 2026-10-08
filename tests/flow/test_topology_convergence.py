from unittest.mock import Mock, patch

from test_topology_events import wait_for_valid_ts_infocluster
from utils import ClusterNode, SlotRange


def topology(node_id, port):
    return {node_id: ClusterNode(id=node_id, ip="127.0.0.1", port=port,
                                flags={"master"}, slots={SlotRange(0, 16383)})}


def test_infocluster_agreement_must_match_current_masters(env):
    old = topology("old", 6379)
    current = topology("promoted", 6380)
    connections = [Mock(), Mock()]
    # Both nodes agree, but only on the pre-failover topology on the first poll.
    with patch("test_topology_events.ts_cluster_from_conn", side_effect=[old, old, current, current]) as read:
        with patch("test_topology_events.time.sleep") as sleep:
            result = wait_for_valid_ts_infocluster(None, connections=connections, expected=current)
    assert result == current
    assert read.call_count == 4
    sleep.assert_called_once_with(0.2)


def test_infocluster_waits_for_promoted_node_outside_original_env(env):
    old = topology("old", 6379)
    current = topology("promoted", 6380)
    original, promoted = Mock(), Mock()
    original_env = Mock(shardsCount=1)
    original_env.getConnection.return_value = original
    # The original node has caught up, while the newly promoted node has not.
    with patch("test_topology_events.ts_cluster_from_conn", side_effect=[current, old, current, current]) as read:
        with patch("test_topology_events.time.sleep") as sleep:
            result = wait_for_valid_ts_infocluster(original_env, connections=[original, promoted], expected=current)
    assert result == current
    assert [args[0][0] for args in read.call_args_list] == [original, promoted, original, promoted]
    sleep.assert_called_once_with(0.2)
