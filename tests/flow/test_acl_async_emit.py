import time

from includes import *


# Regression for MOD-15896 ACL bug: async multi-key emit path resolved ACLs
# via the thread-safe reply ctx, which has no client → no current user. The
# module would log "No context user set, can't check for the ACLs for key X"
# warn-and-allow on every prefetched key.
#
# Fix: capture user-name on the original command ctx, stash in MGetCtx /
# MRangeCtx, re-check ACL via stashed name in the async emit.
#
# This test exercises the async path (BIGREDIS_TESTS=1 → bigredis_enabled =
# true → can_use_async_prefetch() returns true) and asserts:
#   1. Restricted-user TS.MGET / TS.MRANGE returns the expected reply.
#   2. Server log contains NO "No context user set" lines after the run.
#
# On non-Flex builds the command takes the sync path and the warning was never
# possible — test still passes (helps catch future regressions if anyone
# rewires the sync path).


@skip(onVersionLowerThan='7.4.0')
def test_async_emit_acl_does_not_warn(env):
    if env.isCluster():
        env.skip()

    username = 'asyncaclusr'

    with env.getConnection() as conn:
        nkeys = 8
        for i in range(nkeys):
            conn.execute_command(
                'TS.CREATE', f'ts:async:{i}',
                'LABELS', 'group', 'async-acl'
            )
            conn.execute_command('TS.ADD', f'ts:async:{i}', '*', i)

        # User with full access to the matched keyset. This drives the async
        # path through the happy case: upfront CheckDictKeysAreAllowedToRead
        # passes, command hops to the async emit, and the per-key ACL re-check
        # via stashed user_name must succeed silently.
        conn.execute_command(
            'ACL', 'SETUSER', username,
            'on', '>asyncaclpw', 'resetkeys',
            '+@read', '+@timeseries', '+ACL',
            '~ts:async:*'
        )

        # Snapshot log size before the user-scoped commands so we only look at
        # what those commands produced.
        try:
            log_path = get_server_log_path(env)
            log_offset_before = 0
            try:
                with open(log_path) as f:
                    f.seek(0, 2)
                    log_offset_before = f.tell()
            except FileNotFoundError:
                pass
        except Exception:
            log_path = None
            log_offset_before = 0

        try:
            conn.execute_command('AUTH', username, 'asyncaclpw')

            mget_reply = conn.execute_command('TS.MGET', 'FILTER', 'group=async-acl')
            env.assertEqual(len(mget_reply), nkeys,
                            message=f"TS.MGET should return {nkeys} series; "
                                    f"got {len(mget_reply)}")

            mrange_reply = conn.execute_command(
                'TS.MRANGE', '-', '+', 'FILTER', 'group=async-acl'
            )
            env.assertEqual(len(mrange_reply), nkeys,
                            message=f"TS.MRANGE should return {nkeys} series; "
                                    f"got {len(mrange_reply)}")

            mrange_grouped = conn.execute_command(
                'TS.MRANGE', '-', '+', 'FILTER', 'group=async-acl',
                'GROUPBY', 'group', 'REDUCE', 'MAX'
            )
            env.assertEqual(len(mrange_grouped), 1,
                            message="TS.MRANGE GROUPBY should return one group")
        finally:
            # Drop back to default user so other tests aren't restricted.
            try:
                conn.execute_command('AUTH', 'default', '')
            except Exception:
                pass
            try:
                conn.execute_command('ACL', 'DELUSER', username)
            except Exception:
                pass

        # Inspect the server log range produced by the user-scoped commands.
        # The bug logs ONE warning per emitted key on the async path.
        if log_path:
            # Give Redis a moment to flush buffered log lines.
            time.sleep(0.2)
            tail_bytes = b''
            try:
                with open(log_path, 'rb') as f:
                    f.seek(log_offset_before)
                    tail_bytes = f.read()
            except FileNotFoundError:
                pass
            tail = tail_bytes.decode('utf-8', errors='replace')
            env.assertFalse(
                'No context user set' in tail,
                message='Async multi-key emit logged "No context user set" — '
                        'the async ACL re-check did not resolve the stashed '
                        'user-name. Lines:\n' +
                        '\n'.join(l for l in tail.splitlines()
                                  if 'No context user set' in l)
            )
