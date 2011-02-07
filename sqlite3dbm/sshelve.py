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

"""shelve wrapper for a SqliteMap

Usage Example:
>>> import sqlite3dbm
>>> db = sqlite3dbm.sshelve.open('mydb.sqlite3')
>>> db['foo'] = 'bar'
>>> db['baz'] = [1, 2, 3]
>>> db['baz']
[1, 2, 3]
>>> db.select('foo', 'baz')
['bar', [1, 2, 3]]
>>> db.get_many('foo', 'qux', default='')
['bar', '']
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
    """A subclass of shelve.Shelf supporting sqlite3dbm.

    Exposes select() and get_many() which are available in sqlite3dbm but none
    of the other database modules.  The dict object passed to the constructor
    must support these methods, which is generally done by calling
    sqlite3dbm.open.

    The optional `protocol` and `writeback` parameters behave the same as
    they do for shelve.Shelf.
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

    # Performance override: we want to batch writes into one transaction
    def update(self, *args, **kwargs):
        # Copied from sqlite3dbm.dbm
        def kv_gen():
            """Generator that combines all the args for easy iteration."""
            for arg in args:
                if isinstance(arg, dict):
                    for k, v in arg.iteritems():
                        yield k, v
                else:
                    for k, v in arg:
                        yield k, v

            for k, v in kwargs.iteritems():
                yield k, v
        inserts = list(kv_gen())

        if self.writeback:
            self.cache.update(inserts)

        self.dict.update([
            (k, dumps(v, protocol=self._protocol))
            for k, v in inserts
        ])

    # Performance override: clear in one sqlite command
    def clear(self):
        self.dict.clear()

def open(filename, flag='c', mode=0666, protocol=None, writeback=False):
    """Open a persistent sqlite3-backed dictionary.  The *filename* specificed
    is the path to the underlying database.

    The *flag* and *mode* parameters have the same semantics as sqlite3dbm.open
    (and, in fact, are directly passed through to this function).

    The *protocl* and *writeback* parameters behave as outlined in shelve.open.
    """
    smap = sqlite3dbm.dbm.open(filename, flag=flag, mode=mode)
    return SqliteMapShelf(smap, protocol=protocol, writeback=writeback)
