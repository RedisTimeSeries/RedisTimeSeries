#!/usr/bin/env python

from __future__ import print_function
import re
import argparse
import redis
from gevent.server import StreamServer


REDIS_POOL = None
GRAPHITE_PROTO_RE = re.compile(r"(.*?)\s+([\d.]+)\s+(\d+)")

def process_connection(socket, _):
    """
    Per-Connection handler, read all lines and send to redis
    """
    # using a makefile because we want to use readline()
    rfileobj = socket.makefile(mode='rb')
    redis_client = redis.Redis(connection_pool=REDIS_POOL)
    while True:
        line = rfileobj.readline()
        if not line:
            # client disconnect
            break
        data = GRAPHITE_PROTO_RE.findall(line)
        if data:
            # the line is in graphite format
            try:
                path, value, timestamp = data[0]
                value = float(value)
                timestamp = int(timestamp)
            except Exception as ex:
                print("could parse an element %s" % ex)
                break

            try:
                redis_client.execute_command("ts.add", path, timestamp, value)
            except redis.ResponseError as ex:
                # small hack, for performance reasons its better to first try to add an metric
                # instead of checking per metric if it exists or not
                if 'the key does not exists' in ex.message:
                    redis_client.execute_command("ts.create",
                                                 path,
                                                 MAX_RETENTION,
                                                 SAMPLES_PER_CHUNK)
                    redis_client.execute_command("ts.add", path, timestamp, value)
                else:
                    raise

        else:
            print("line is not in graphite format: %s" % line)
            break
    rfileobj.close()


def main():
    global REDIS_POOL, MAX_RETENTION, SAMPLES_PER_CHUNK

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", help="server address to listen to", default="127.0.0.1")
    parser.add_argument("--port", help="port number to listen to", default=2003, type=int)
    parser.add_argument("--redis-server", help="redis server address")
    parser.add_argument("--redis-port", help="redis server port", default=6379, type=int)
    parser.add_argument("--max-retention", help="default retention time (in seconds)", default=3600, type=int)
    parser.add_argument("--samples-per-chunk", help="default samples per memory chunk", default=360, type=int)

    args = parser.parse_args()

    MAX_RETENTION = args.max_retention
    SAMPLES_PER_CHUNK = args.samples_per_chunk
    REDIS_POOL = redis.ConnectionPool(host=args.redis_server, port=args.redis_port)

    server = StreamServer((args.host, args.port), process_connection)
    print('Starting Graphite server on %s:%s' % (args.host, args.port))
    server.serve_forever()

if __name__ == '__main__':
    main()
