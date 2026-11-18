#!/usr/bin/env python3
"""Bounded snapshots for the unsplit macOS runner; stdout is streamed by CI."""
import datetime
import os
import subprocess
import sys
import time


def snapshot():
    print('RUNNER_DIAGNOSTICS', datetime.datetime.now(datetime.timezone.utc).isoformat(), flush=True)
    for command in [['vm_stat'], ['sysctl', 'vm.swapusage'],
                    ['ps', '-axo', 'pid=,ppid=,%cpu=,rss=,comm=']]:
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=10)
            output = result.stdout
            if command[0] == 'ps':
                # Exclude arguments and unrelated processes from the CI log.
                rows = []
                for line in output.splitlines():
                    fields = line.split(None, 4)
                    if len(fields) == 5 and any(name in os.path.basename(fields[4]).lower()
                                               for name in ['redis', 'python', 'runner']):
                        rows.append(' '.join(fields[:4] + [os.path.basename(fields[4])]))
                output = 'PID PPID CPU_PERCENT RSS_KB EXECUTABLE\n' + '\n'.join(rows)
            print(command[0], 'exit=', result.returncode, '\n' + output, flush=True)
        except (OSError, subprocess.TimeoutExpired) as error:
            print(command[0], type(error).__name__, flush=True)


while True:
    snapshot()
    if '--once' in sys.argv:
        break
    time.sleep(60)
