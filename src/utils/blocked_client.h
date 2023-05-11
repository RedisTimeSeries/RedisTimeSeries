/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */

#include "RedisModulesSDK/redismodule.h"

// create blocked client and report start time
RedisModuleBlockedClient *RTS_BlockClient(RedisModuleCtx *ctx,
                                          void (*free_privdata)(RedisModuleCtx *, void *));

// unblock blocked client and report end time
void RTS_UnblockClient(RedisModuleBlockedClient *bc, void *privdata);
