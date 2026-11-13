"""Per-thread cumulative CPU seconds and names for local Linux/macOS benchmarks."""

import ctypes
import os
import pathlib
import sys


def snapshot(pid):
    if sys.platform == 'linux':
        result = {}
        ticks = os.sysconf('SC_CLK_TCK')
        for path in pathlib.Path(f'/proc/{pid}/task').glob('*/stat'):
            text = path.read_text()
            name, fields = text.split('(', 1)[1].rsplit(')', 1)
            fields = fields.split()
            result[path.parent.name] = {'name': name,
                                       'seconds': (int(fields[11]) + int(fields[12])) / ticks}
        return result
    if sys.platform == 'darwin':
        class ThreadInfo(ctypes.Structure):
            _fields_ = [('user', ctypes.c_uint64), ('system', ctypes.c_uint64),
                        ('state', ctypes.c_int32 * 8), ('name', ctypes.c_char * 64)]

        lib = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
        lib.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64,
                                    ctypes.c_void_p, ctypes.c_int]
        lib.proc_pidinfo.restype = ctypes.c_int
        tids = (ctypes.c_uint64 * 4096)()
        size = lib.proc_pidinfo(pid, 6, 0, tids, ctypes.sizeof(tids))
        if size <= 0 or size >= ctypes.sizeof(tids):
            raise RuntimeError(f'Cannot enumerate threads for {pid}: {ctypes.get_errno()}')
        result = {}
        for tid in tids[:size // ctypes.sizeof(ctypes.c_uint64)]:
            info = ThreadInfo()
            if lib.proc_pidinfo(pid, 5, tid, ctypes.byref(info), ctypes.sizeof(info)) != ctypes.sizeof(info):
                raise RuntimeError(f'Cannot read thread {tid}: {ctypes.get_errno()}')
            result[str(tid)] = {'name': info.name.decode(errors='replace'),
                                'seconds': (info.user + info.system) / 1e9}
        return result
    raise RuntimeError('Thread CPU measurement requires Linux or macOS')


def delta(before, after):
    return [{'id': tid, 'name': value['name'],
             'cpu_seconds': value['seconds'] - before.get(tid, {'seconds': 0})['seconds']}
            for tid, value in after.items()]
