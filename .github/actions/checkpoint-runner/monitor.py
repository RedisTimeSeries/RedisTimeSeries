"""Collect samples independently of the test step and publish completed batches."""
import datetime
from pathlib import Path
import os
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
SAMPLER = HERE.parent / 'run-tests' / 'macos-resources.py'


def monitor(directory, node, baseline):
    report = directory / 'runner-health.log'
    next_upload = time.monotonic() if baseline else time.monotonic() + 300
    prior_warning = False
    while True:
        started = time.monotonic()
        result = subprocess.run([sys.executable, '-u', SAMPLER, '--once'], capture_output=True, text=True)
        with report.open('a') as output:
            output.write(result.stdout)
            if result.returncode:
                print(f'sampler_exit={result.returncode}', file=output)
        warning = '::warning::' in result.stdout
        if time.monotonic() >= next_upload or (warning and not prior_warning):
            stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            name = f'runner-health-{os.environ["GITHUB_RUN_ATTEMPT"]}-{stamp}'
            try:
                subprocess.run([node, HERE / 'publish.mjs', name, report], check=True, timeout=45)
            except (OSError, subprocess.SubprocessError) as error:
                next_upload = time.monotonic()
                if baseline:
                    raise
                print(f'Checkpoint upload failed: {error}', flush=True)
                # Keep the pending samples and retry after the next sample.
            else:
                report.unlink()
                next_upload = time.monotonic() + 300
                if baseline:
                    return
        prior_warning = warning
        time.sleep(max(0, 60 - (time.monotonic() - started)))


if __name__ == '__main__':
    monitor(Path(sys.argv[1]), sys.argv[2], '--baseline' in sys.argv[3:])
