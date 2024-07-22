/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */

#ifndef REDISTIMESERIES_MODULE_VERSION

#define REDISTIMESERIES_VERSION_MAJOR 1
#define REDISTIMESERIES_VERSION_MINOR 12
#define REDISTIMESERIES_VERSION_PATCH 2

#define REDISTIMESERIES_SEMANTIC_VERSION(major, minor, patch) \
  (major * 10000 + minor * 100 + patch)

#define REDISTIMESERIES_MODULE_VERSION REDISTIMESERIES_SEMANTIC_VERSION(REDISTIMESERIES_VERSION_MAJOR, REDISTIMESERIES_VERSION_MINOR, REDISTIMESERIES_VERSION_PATCH)

#endif
