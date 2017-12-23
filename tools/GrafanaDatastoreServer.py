#!/usr/bin/env python

import argparse
import redis
import flask
import calendar
import dateutil.parser
from gevent.wsgi import WSGIServer
from flask import Flask, jsonify
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)

REDIS_POOL = None

@app.route('/')
@cross_origin()
def hello_world():
    return 'OK'

@app.route('/search', methods=["POST", 'GET'])
@cross_origin()
def search():
    redis_client = redis.Redis(connection_pool=REDIS_POOL)
    return jsonify(redis_client.keys())

def process_targets(targets, redis_client):
    result = []
    for target in targets:
        if '*' in target:
            result.extend(redis_client.keys(target))
        else:
            result.append(target)
    return result

@app.route('/query', methods=["POST", 'GET'])
def query():
    request = flask.request.get_json()
    response = []

    stime = calendar.timegm(dateutil.parser.parse(request['range']['from']).timetuple())
    etime = calendar.timegm(dateutil.parser.parse(request['range']['to']).timetuple())

    redis_client = redis.Redis(connection_pool=REDIS_POOL)
    targets = process_targets([t['target'] for t in request['targets']], redis_client)

    for target in targets:
        args = ['ts.range', target, int(stime), int(etime)]
        if 'intervalMs' in request and request['intervalMs'] > 0 and request['intervalMs']/1000 > 1:
            args += ['avg', int(round(request['intervalMs']/1000))]
        print(args)
        redis_resp = redis_client.execute_command(*args)
        datapoints = [(x2.decode("ascii"), x1*1000) for x1, x2 in redis_resp]
        response.append(dict(target=target, datapoints=datapoints))
    return jsonify(response)


@app.route('/annotations')
def annotations():
    return jsonify([])

def main():
    global REDIS_POOL
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", help="server address to listen to", default="0.0.0.0")
    parser.add_argument("--port", help="port number to listen to", default=8080, type=int)
    parser.add_argument("--redis-server", help="redis server address", default="localhost")
    parser.add_argument("--redis-port", help="redis server port", default=6379, type=int)
    args = parser.parse_args()

    REDIS_POOL = redis.ConnectionPool(host=args.redis_server, port=args.redis_port)

    http_server = WSGIServer(('', args.port), app)
    http_server.serve_forever()

if __name__ == '__main__':
    main()
