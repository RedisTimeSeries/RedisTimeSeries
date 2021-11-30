import os
import sys
from logging import exception

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
