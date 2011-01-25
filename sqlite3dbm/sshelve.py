# Copyright 2011 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""shelve wrapper for a SqliteMap"""

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
    """A subclass of :class:`shelve.Shelf` supporting :mod:`sqlite3dbm.dbm`.

    Exposes :func:`~sqlite3dbm.dbm.SqliteMap.select` and
    :func:`~sqlite3dbm.dbm.SqliteMap.get_many` which are available in
    :mod:`sqlite3dbm.dbm` but none of the other database modules.  The dict
    object passed to the constructor must support these methods, which is
    generally done by calling :func:`sqlite3dbm.dbm.open`.

    The optional `protocol` and `writeback` parameters behave the same as
    they do for :class:`shelve.Shelf`.
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
