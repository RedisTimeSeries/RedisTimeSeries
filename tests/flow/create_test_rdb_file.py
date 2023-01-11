import calendar
import csv
import os
import shutil
import subprocess
import sys
from datetime import datetime

import redis

WORK_DIR = '/tmp/redis_wd'
OUTPUT_RDB = 'output.rdb'
PORT = 26379
RDB_PATH = os.path.join(WORK_DIR, OUTPUT_RDB)
PIPELINE_SIZE = 1000

# CSV file headers

AverageTemperature = 1
AverageTemperatureUncertainty = 2
City = 3
Country = 4
Latitude = 5
Longitude = 6

def read_from_disk():
    with open('GlobalLandTemperaturesByMajorCity.csv', encoding = "utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        _ = next(reader)  # skip the column
        for row in reader:
            yield row

def parse_timestamp(ts):
    date_time_obj = datetime.strptime(ts, '%Y-%m-%d')
    return calendar.timegm(date_time_obj.timetuple()) * 1000

def create_compacted_key(redis, i, source, agg, bucket):
    dest = '%s_%s_%s' % (source, agg, bucket)
    redis.delete(dest)
    redis.execute_command('ts.create', dest, 'RETENTION', 0, 'CHUNK_SIZE', 360,
                          'LABELS', 'index', i, "aggregation", agg, "bucket", bucket)
    redis.execute_command('ts.createrule', source, dest, 'AGGREGATION', agg, bucket, )

def load_into_redis(redis_conn):
    label = '1'
    keyname = 't1'
    redis_conn.execute_command('TS.CREATE', keyname, 'RETENTION', 0, 'CHUNK_SIZE', 360, 'LABELS', 'index', label)
    create_compacted_key(redis_conn, label, keyname, 'avg', 10)
    create_compacted_key(redis_conn, label, keyname, 'avg', 60)
    create_compacted_key(redis_conn, label, keyname, 'count', 10)
    create_compacted_key(redis_conn, label, keyname, 'max', 10)
    create_compacted_key(redis_conn, label, keyname, 'min', 10)
    create_compacted_key(redis_conn, label, keyname, 'first', 10)
    create_compacted_key(redis_conn, label, keyname, 'last', 10)
    create_compacted_key(redis_conn, label, keyname, 'sum', 10)
    create_compacted_key(redis_conn, label, keyname, 'range', 10)
    create_compacted_key(redis_conn, label, keyname, 'std.p', 10)
    create_compacted_key(redis_conn, label, keyname, 'std.s', 10)
    create_compacted_key(redis_conn, label, keyname, 'var.s', 10)
    create_compacted_key(redis_conn, label, keyname, 'var.p', 10)
    r = redis_conn.pipeline(transaction=False)
    count = 0
    for row in read_from_disk():
        timestamp = parse_timestamp(row[0])
        if timestamp < 0:
            continue

        if count > PIPELINE_SIZE:
            r.execute()
            count = 0
            r = redis_conn.pipeline(transaction=False)

        city = row[City]
        country = row[Country].replace("(", "[").replace(")", "]")
        if row[AverageTemperature]:
            r.execute_command('TS.ADD', '{}:{}'.format('AverageTemperature', city),
                              timestamp, row[AverageTemperature],
                              'LABELS',
                              'metric', 'temperature',
                              'city', city,
                              'country', country,
                              "latitude", row[Latitude], 'longitude', row[Longitude])
        if row[AverageTemperatureUncertainty]:
            r.execute_command('TS.ADD', '{}:{}'.format('AverageTemperatureUncertainty', city),
                              timestamp, row[AverageTemperatureUncertainty],
                              'LABELS', 'city', city, 'country', country,
                              "latitude", row[Latitude], 'longitude', row[Longitude])
        count += 1

        r.execute()


def main(version, i):
    if not os.path.exists(WORK_DIR):
        os.mkdir(WORK_DIR)
    elif os.path.exists(RDB_PATH):
        os.unlink(RDB_PATH)

    args = ['docker', 'run',
            '-p', '{}:{}'.format(PORT, PORT),
            '-v', '{}:{}'.format(WORK_DIR, WORK_DIR),
            '--name', 'rdb_test_{0}'.format(i),
            '--rm', 'redislabs/redistimeseries:{}'.format(version),
            'redis-server',
            '--port', str(PORT),
            '--dir', WORK_DIR,
            '--dbfilename', OUTPUT_RDB,
            '--loadmodule', '/usr/lib/redis/modules/redistimeseries.so']
    print(args)
    proc = subprocess.Popen(args)
    try:
        redis_conn = redis.StrictRedis(port=PORT)

        print("waiting for server")
        while True:
            try:
                redis_conn.ping()
                print("done")
                break
            except redis.exceptions.ConnectionError:
                pass

        redis_conn.flushall()

        load_into_redis(redis_conn)

        redis_conn.save()
        redis_conn.ping()
        shutil.copyfile(RDB_PATH, os.path.join('rdbs', "{}.rdb".format(version)))

    finally:
        os.system("docker stop rdb_test_{0}".format(i))
        proc.send_signal(15)


if __name__ == '__main__':
    i = 0
    for arg in sys.argv:
        if i > 0:
            main(arg, i)
        i += 1
