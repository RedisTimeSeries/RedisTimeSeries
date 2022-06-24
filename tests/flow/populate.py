import random
import redis

NUM_KEYS = 1000
NUM_SAMPLES = 4096
PORT = 6379

def main():
 
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
            redis_conn.execute_command('ts.create', key)
            print("Created time-series " + key)
            for i in range(1, NUM_SAMPLES):
                assert redis_conn.execute_command('ts.add', key, i, random.uniform(1.0, 10000.0)) == i
       
        print("Finished polulating Samples...")

    except:
        print("Done")


if __name__ == '__main__':
    main()
