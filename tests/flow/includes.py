import os
import sys
from logging import exception
from RLTest import Env as _rltestEnv


try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../deps/readies"))
    import paella
except exception:
    pass

OSNICK = paella.Platform().osnick

RLEC_CLUSTER = os.getenv('RLEC_CLUSTER') == '1'

SANITIZER = os.getenv('SANITIZER', '')
VALGRIND = os.getenv('VALGRIND', '0') == '1'
CODE_COVERAGE = os.getenv('CODE_COVERAGE', '0') == '1'


def skip_on_rlec():
    if RLEC_CLUSTER:
        _rltestEnv().skip()


def decode_if_needed(data):
    if isinstance(data, list):
        ret = []
        for item in data:
            ret.append(decode_if_needed(item))
        return ret
    elif isinstance(data, bytes):
        return data.decode()
    else:
        return data
