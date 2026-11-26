#!/usr/bin/env python3
"""Bounded snapshots for the unsplit macOS runner; stdout is streamed by CI."""
import datetime
import os
import re
import shutil
from pathlib import Path
import subprocess
import sys
import time


def snapshot():
    print('RUNNER_DIAGNOSTICS', datetime.datetime.now(datetime.timezone.utc).isoformat(), flush=True)
    workspace = Path(os.environ.get('GITHUB_WORKSPACE', '.'))
    disk = shutil.disk_usage(workspace)
    print(f'workspace_disk_free_mib={disk.free // (1024 ** 2)}', flush=True)
    if disk.free / disk.total < 0.05:
        print('::warning::Runner workspace disk has less than 5% free', flush=True)

    try:
        with (workspace / 'test_output.log').open('rb') as log:
            progress = os.fstat(log.fileno())
            log.seek(max(0, progress.st_size - 65536))
            # Include only a test identifier, never arbitrary test output.
            tail = re.sub(rb'\x1b\[[0-9;]*m', b'', log.read())
            tests = re.findall(rb'\b(test_\w+:test_\w+)\b', tail)
            if tests:
                print('last_test_in_output=' + tests[-1].decode('ascii'), flush=True)
    except FileNotFoundError:
        pass  # The build or test setup may still be running.
    else:
        quiet = max(0, int(time.time() - progress.st_mtime))
        print(f'test_output_bytes={progress.st_size} test_output_quiet_seconds={quiet}', flush=True)
        if quiet >= 300:
            print('::warning::No new test output for at least 5 minutes', flush=True)

    for command in [['vm_stat'], ['sysctl', 'vm.swapusage'],
                    ['sysctl', '-n', 'kern.memorystatus_vm_pressure_level'],
                    ['ps', '-axo', 'pid=,ppid=,%cpu=,rss=,comm=']]:
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=10)
            output = result.stdout
            if command[-1] == 'kern.memorystatus_vm_pressure_level' and result.returncode == 0:
                level = output.strip()
                output = f'vm_pressure_level={level}'
                if level in ('2', '4'):
                    print('::warning::macOS reports elevated VM pressure: ' + level, flush=True)
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
