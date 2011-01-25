"""
shelve wrapper for a SqliteMap
"""
import shelve
# Try using cPickle and cStringIO if available.
try:
    from cPickle import loads, dumps
except ImportError:
    from pickle import loads, dumps

import sqlite3dbm.dbm

__all__ = [
    'SqliteMapShelf',
    'open',
]

class SqliteMapShelf(shelve.Shelf):
    """Shelf implementation using the SqliteMap interface.

    Adds SqliteMap's `get_many` and `select` functions to the standard dbm
    interface.

    The actual db must be opened by constructing a SqliteMap or using
    the `open` method provided in that module.
    """
    def __init__(self, smap, protocol=None, writeback=False):
        # Force the Sqlite DB to return bytestrings.  By default it returns
        # unicode by default, which causes Pickle to shit its pants.
        smap.conn.text_factory = str

        # SqliteMapShelf < Shelf < DictMixin which is an old style class :-P
        shelve.Shelf.__init__(self, smap, protocol, writeback)

    def get_many(self, *args, **kwargs):
        # Pickle 'default' for consistency when we de-pickle
        default = dumps(kwargs.get('default'))
        kwargs['default'] = default

        return [
            loads(v)
            for v in self.dict.get_many(*args, **kwargs)
        ]

    def select(self, *args):
        return [
            loads(v)
            for v in self.dict.select(*args)
        ]

def open(filename, flag='c', mode=0666, protocol=None, writeback=False):
    """Open a persistent, sqlite-backed dictionary for reading and writing"""
    smap = sqlite3dbm.dbm.open(filename, flag=flag, mode=mode)
    return SqliteMapShelf(smap, protocol=protocol, writeback=writeback)
