
from contextlib import contextmanager
import os

def fread(fname, mode = 'rb'):
	with open(fname, mode) as file:
		return file.read()

def flines(fname, mode = 'rb'):
	return [line.rstrip() for line in open(fname)]

@contextmanager
def cwd(path):
    d0 = os.getcwd()
    os.chdir(str(path))
    try:
        yield
    finally:
        os.chdir(d0)

def mkdir_p(dir):
    if dir != '':
        os.makedirs(dir, exist_ok=True)
