
from contextlib import contextmanager
import os

def fread(fname, mode = 'rb'):
	with open(fname, mode) as file:
		return file.read()

@contextmanager
def cwd(path):
    d0 = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(d0)

def mkdir_p(dir):
    if dir != '':
        os.makedirs(dir, exist_ok=True)
