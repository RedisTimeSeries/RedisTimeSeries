# A utility to populate a time-series either compressed or uncompressed. 
# To run: 
# python3 populate.py compressed
# or
# python3 populate.py uncompressed

import random
import os
import sys
import redis
import shutil

NUM_KEYS = 1000
NUM_SAMPLES = 4096
PORT = 6379

WORK_DIR = '/software_dev/redis'
OUTPUT_RDB = 'dump.rdb'
RDB_PATH = os.path.join(WORK_DIR, OUTPUT_RDB)
VERSION = "1.6.14"

def main(encoding):
 
    print("Attempting to connect to Redis server...")
    try:
        redis_conn = redis.StrictRedis(host='localhost', port=PORT, password='')

        print("waiting for server")
        while True:
            try:
                redis_conn.ping()
                break
            except redis.exceptions.ConnectionError:
                pass

        redis_conn.flushall()

        print("Polulating Samples...")
        for k in range(1, NUM_KEYS):
            key = "key" + str(k)
            redis_conn.execute_command('ts.create', key, 'ENCODING', encoding, 'LABELS', 'region', 'phoenix')
            print("Created time-series " + key)
            for i in range(1, NUM_SAMPLES):
                assert redis_conn.execute_command('ts.add', key, i, random.uniform(1.0, 10000.0)) == i
       
        print("Finished polulating Samples...")

        print("Create rdb.")
        redis_conn.save()
        redis_conn.ping()
        shutil.copyfile(RDB_PATH, os.path.join('rdbs', "{}.rdb".format(VERSION)))

    finally:
        print("Done")


if __name__ == '__main__':
    main(sys.argv[1])
