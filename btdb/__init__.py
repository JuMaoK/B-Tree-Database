import os

from btdb.interface import DBDB_BTree


__all__ = ['DBDB_BTree', 'connect']


def connect(dbname):
    try:
        f = open(dbname, 'r+b')
    except IOError:
        fd = os.open(dbname, os.O_RDWR | os.O_CREAT)
        f = os.fdopen(fd, 'r+b')
    return DBDB_BTree(f)
