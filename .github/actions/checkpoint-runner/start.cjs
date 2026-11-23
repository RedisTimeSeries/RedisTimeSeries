const { spawn, spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const directory = fs.mkdtempSync(path.join(process.env.RUNNER_TEMP, 'runner-health-'));
const script = path.join(__dirname, 'monitor.py');
const args = [script, directory, process.execPath];
// Verify that the artifact service has a baseline before starting the workload.
const baseline = spawnSync('python3', [...args, '--baseline'], { stdio: 'inherit' });
if (baseline.error || baseline.status !== 0) {
  throw new Error('Could not publish the initial runner health checkpoint');
}
const log = fs.openSync(path.join(directory, 'monitor.log'), 'a');
const child = spawn('python3', args, { detached: true, stdio: ['ignore', log, log] });
child.unref();
fs.closeSync(log);
fs.appendFileSync(process.env.GITHUB_STATE, 'monitorPid=' + child.pid + os.EOL);
