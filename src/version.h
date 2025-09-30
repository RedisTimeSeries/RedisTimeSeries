/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef REDISTIMESERIES_MODULE_VERSION

#define REDISTIMESERIES_VERSION_MAJOR 8
#define REDISTIMESERIES_VERSION_MINOR 3
#define REDISTIMESERIES_VERSION_PATCH 80

#define REDISTIMESERIES_SEMANTIC_VERSION(major, minor, patch) \
  (major * 10000 + minor * 100 + patch)

#define REDISTIMESERIES_MODULE_VERSION REDISTIMESERIES_SEMANTIC_VERSION(REDISTIMESERIES_VERSION_MAJOR, REDISTIMESERIES_VERSION_MINOR, REDISTIMESERIES_VERSION_PATCH)

#endif
