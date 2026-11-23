const pid = Number(process.env.STATE_monitorPid);
if (Number.isInteger(pid) && pid > 0) {
  try {
    process.kill(pid, 'SIGTERM');
  } catch (error) {
    if (error.code !== 'ESRCH') throw error;
  }
}
