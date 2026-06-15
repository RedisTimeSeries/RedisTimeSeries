"""pytest hooks to print START/END markers for every test.

RLTest's own [PASS] markers print after a test completes, so a hang inside a
test shows only the previous test's [PASS] in CI — leaving the actual culprit
invisible. These hooks emit a START line BEFORE setup and an END line AFTER
teardown, flushed immediately, so the CI log pinpoints the hanging test.
"""

import sys
import time


def _emit(msg):
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def pytest_runtest_logstart(nodeid, location):
    _emit(">>> START %s @%.3f" % (nodeid, time.time()))


def pytest_runtest_logfinish(nodeid, location):
    _emit("<<< END   %s @%.3f" % (nodeid, time.time()))
