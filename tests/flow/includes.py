import os
import sys

try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../deps/readies"))
    import paella
except:
    pass

RLEC_CLUSTER = os.getenv('RLEC_CLUSTER') == '1'
