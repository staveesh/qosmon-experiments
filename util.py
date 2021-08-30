import os


def create_dir(path):
    os.umask(0)
    os.mkdir(path, mode=0o777)
