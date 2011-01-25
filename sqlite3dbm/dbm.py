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

"""A SQLite-backed dictionary that respects the dbm interface"""

from __future__ import with_statement

import os
import sqlite3

__all__ = [
    'open',
    'error',
]

# Maximum number of bindable parameters in a SQLite query
SQLITE_MAX_QUERY_VARS = 999

# Unique sentinel that we can do pointer comparisons against to check if an
# optional kwarg has been supplied to pop.
__POP_SENTINEL__ = ('__pop__',)

# Unique sentinel that we can use to distinguish missing values from None
# values in `select`
__MISSING_SENTINEL__ = ('__missing__',)


## Pre-compile all queries as raw SQL for speed and
## to avoid outside dependencies

_GET_QUERY = 'SELECT kv_table.val FROM kv_table WHERE kv_table.key = ?'
_GET_ALL_QUERY = 'SELECT kv_table.key, kv_table.val FROM kv_table'
_GET_ONE_QUERY = 'SELECT kv_table.key, kv_table.val FROM kv_table LIMIT 1 OFFSET 0'

# The get-many query generation is slightly unfortunate in that sqlite does not
# seem to have an interface for binding a list of values into a query.  Thus,
# we must generate a query string with the right number of missing parameter
# values for the particular select we want to do
_GET_MANY_QUERY_TEMPLATE = (
    'SELECT kv_table.key, kv_table.val FROM kv_table '
    'WHERE kv_table.key IN (%s)'
)
def get_many_query(num_keys):
    # Cache the super-big query, as it may happen many times
    # through big select/get_many calls
    if (num_keys == SQLITE_MAX_QUERY_VARS and
        hasattr(get_many_query, '_big_query_cache')):
        return get_many_query._big_query_cache

    interpolation_params = ','.join('?' * num_keys)
    tmpl = _GET_MANY_QUERY_TEMPLATE % interpolation_params

    if num_keys == SQLITE_MAX_QUERY_VARS:
        get_many_query._big_query_cache = tmpl

    return tmpl

# Do INSERT OR REPLACE instead of a vanilla INSERT
# to mimic normal dict overwrite-on-insert behavior
_SET_QUERY = 'INSERT OR REPLACE INTO kv_table (key, val) VALUES (?, ?)'

_DEL_QUERY = 'DELETE FROM kv_table WHERE kv_table.key = ?'
_CLEAR_QUERY = 'DELETE FROM kv_table; VACUUM;'

_COUNT_QUERY = 'SELECT COUNT(*) FROM kv_table'

# Table has a String key, which puts an upper bound on the
# size of keys that can be inserted.  The Text format of
# the values should be fine in general, but could be optimized
# if we were storing ints/floats.  Also, should probably
# be changed to Blob if we use a binary serialization format.
#
# TODO: Make this more configurable re the above comment
# Use TEXT for the keys now... maybe want to limit it for better
# indexing?  Should do some performance profiling...
_CREATE_TABLE = (
    'CREATE TABLE IF NOT EXISTS '
    'kv_table (key TEXT PRIMARY KEY, val TEXT)'
)

def _to_unicode(s):
    """Convert raw strings to unicode.

    First try to decode from utf-8, then fallback
    to windows-1252 then latin-1.
    """
    # We only change bytestrings
    if not isinstance(s, str):
        return s

    try:
        return s.decode('utf-8')
    except UnicodeDecodeError:
        try:
            return s.decode('windows-1252')
        except UnicodeDecodeError:
            return s.decode('latin-1')

class SqliteMapException(Exception):
    pass
# DBM interface
error = SqliteMapException


class SqliteMap(object):
    """Dictionary interface backed by a SQLite DB.

    This dictionary only accepts string key/values.

    This is not remotely threadsafe.
    """

    def __init__(self, path, flag='r', mode=0666):
        """Create an dict backed by a SQLite DB at `sqlite_db_path`.

        Args:
            path: Path on disk to db to back this map
            flag: How to open the db.
                c: create if it doesn't exist
                n: new empty
                w: open existing
                r: open existing readonly [default]
            mode: Unix mode of underlying file if it is created. Modified by umask.
        """
        # Need an absolute path to db on the filesystem
        path = os.path.abspath(path)

        if flag not in ('c', 'n', 'w', 'r'):
            raise error('Invalid flag "%s"' % (flag,))

        # Default behavior is to create if the file does not already exist.
        # We tweak from this default behavior to accommodate the other flag options

        self.readonly = flag == 'r'

        # r and w require the db to exist ahead of time
        if not os.path.exists(path):
            if flag in ('r', 'w'):
                raise error('DB does not exist at %s' % (path,))
            else:
                # Ghetto way of respecting mode, since unexposed by sqlite3.connect
                # Manually create the file before sqlite3 connects to it
                os.open(path, os.O_CREAT, mode)

        self.conn = sqlite3.connect(path)
        self.conn.execute(_CREATE_TABLE)

        # n option requires us to clear out existing data
        if flag == 'n':
            self.clear()

    def __setitem__(self, k, v):
        """x.__setitem__(k, v) <==> x[k] = v"""
        if self.readonly:
            raise error('DB is readonly')

        self.conn.execute(_SET_QUERY, (k, v))
        self.conn.commit()

    def __getitem__(self, k):
        """x.__getitem__(k) <==> x[k]

        This version of :meth:`__getitem__` also transparently works on lists:
            >>> smap.update({'1': 'a', '2': 'b', '3': 'c'})
            >>> smap['1', '2', '3']
            [u'a', u'b', u'c']
            >>> smap[['1', '2', '3']]
            [u'a', u'b', u'c']
        """
        if hasattr(k, '__iter__'):
            return self.select(k)

        row = self.conn.execute(_GET_QUERY, (k,)).fetchone()
        if row is None:
            raise KeyError(k)
        return row[0]

    def __delitem__(self, k):
        """x.__delitem__(k) <==> del x[k]"""
        if self.readonly:
            raise error('DB is readonly')

        # So, the delete actually has no problem running when
        # the key k was not present in the map.  Unfortunately,
        # this does not conform to the dict interface so we
        # do a __getitem__ here to make sure a KeyError gets
        # thrown when it should. I think this is dumb :-P
        self[k]

        self.conn.execute(_DEL_QUERY, (k,))
        self.conn.commit()

    def __contains__(self, k):
        """D.__contains__(k) -> True if D has a key k, else False"""
        try:
            self[k]
        except KeyError:
            return False
        else:
            return True

    def clear(self):
        """D.clear() -> None. Remove all items from D."""
        if self.readonly:
            raise error('DB is readonly')

        self.conn.executescript(_CLEAR_QUERY)
        self.conn.commit()

    def get(self, k, d=None):
        """D.get(k[,d]) -> D[k] if k in D, else d. d defaults to None."""
        try:
            return self[k]
        except KeyError:
            return d

    def has_key(self, k):
        """D.has_key(k) -> True if D has a key k, else False."""
        return k in self

    def pop(self, k, d=__POP_SENTINEL__):
        """D.pop(k[,d]) -> v, remove specified key and return the corresponding value.
        If key is not found, d is returned if given, otherwise KeyError is raised.
        """
        if self.readonly:
            raise error('DB is readonly')

        try:
            val = self[k]
            del self[k]
            return val
        except KeyError:
            if d is __POP_SENTINEL__:
                raise KeyError(k)
            else:
                return d

    def popitem(self):
        """D.popitem() -> (k, v), remove and return some (key, value) pair as a
        2-tuple; but raise KeyError if D is empty
        """
        if self.readonly:
            raise error('DB is readonly')

        rows = [row for row in self.conn.execute(_GET_ONE_QUERY)]
        if len(rows) != 1:
            raise KeyError(
                'Found %d rows when there should have been 1' % (len(rows),)
            )

        key, val = rows[0]
        del self[key]
        return key, val

    def setdefault(self, k, d=None):
        """D.setdefault(k[,d]) -> D.get(k,d), also set D[k]=d if k not in D"""
        if self.readonly:
            raise error('DB is readonly')

        if k in self:
            return self[k]
        else:
            self[k] = d
            return d

    def get_many(self, *args, **kwargs):
        """Basically :meth:`~sqlite3dbm.dbm.SqliteMap.get`
        and :meth:`~sqlite3dbm.dbm.SqliteMap.select` combined.

        The interface is the same as :meth:`~sqlite3dbm.dbm.SqliteMap.select`
        except for the additional option argument `default`.  This argument
        specifies what value should be used for keys that are not present in
        the dict.
        """
        default = kwargs.pop('default', None)
        if kwargs:
            raise TypeError(
                'Got an unexpected keyword argument: %r' % (kwargs,)
            )

        def k_gen():
            """Generator to make iterating over args easy."""
            for arg in args:
                if hasattr(arg, '__iter__'):
                    for k in arg:
                        yield k
                else:
                    yield arg

        def lookup(keys):
            """Reuse the slightly weird logic to lookup values"""
            # Do all the selects in a single transaction
            key_to_val = dict(self.conn.execute(get_many_query(len(keys)), keys))

            # Need to do this whole map lookup thing because the
            # select does not have a return order
            #
            # Need to convert keys to unicode because Sqlite does that
            # automatically, but builtin dicts distinguish unicode
            # and non-unicode keys
            return (key_to_val.get(_to_unicode(key), default) for key in keys)

        keys = []
        result = []
        for k in k_gen():
            if len(keys) < SQLITE_MAX_QUERY_VARS:
                keys.append(k)
            else:
                result.extend(lookup(keys))
                keys = [k]
        if len(keys) > 0:
            result.extend(lookup(keys))

        return result

    def select(self, *args):
        """List based version of :meth:`__getitem__`.  Complement of :meth:`~sqlite3dbm.dbm.SqliteMap.update`.

        `args` are the keys to retrieve from the dict.  All of the following work:
            >>> smap.update({'1': 'a', '2': 'b', '3': 'c'})
            >>> smap.select('1', '2', '3')
            [u'a', u'b', u'c']
            >>> smap.select(['1', '2', '3'])
            [u'a', u'b', u'c']
            >>> smap.select(['1', '2'], '3')
            [u'a', u'b', u'c']
            >>> smap.select(['1', '2'], ['3'])
            [u'a', u'b', u'c']

        Returns:
            List of values corresponding to the requested keys in order

        Raises:
            KeyError if any of the keys are missing
        """
        vals = self.get_many(default=__MISSING_SENTINEL__, *args)
        if __MISSING_SENTINEL__ in vals:
            raise KeyError('One of the requested keys is missing!')
        return vals

    def update(self, *args, **kwargs):
        """D.update(E, **F) -> None.  Update D from E and F: for k in E: D[k] = E[k]
        (if E has keys else: for (k, v) in E: D[k] = v) then: for k in F: D[k] = F[k]
        """
        if self.readonly:
            raise error('DB is readonly')

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
        rows = [(k, v) for k, v in kv_gen()]

        # Do all the inserts in a single transaction for the sake of efficiency
        # TODO: Compare preformance of INSERT MANY to many INSERTS.  Will
        # have to do it in blocks to not exceed query-size limits
        self.conn.executemany(_SET_QUERY, rows)
        self.conn.commit()

    def __len__(self):
        """x.__len__() <==> len(x)"""
        return self.conn.execute(_COUNT_QUERY).fetchone()[0]

    ## Iteration
    def iteritems(self):
        """D.iteritems() -> an iterator over the (key, value) items of D"""
        for key, val in self.conn.execute(_GET_ALL_QUERY):
            yield key, val

    def items(self):
        """D.items() -> list of D's (key, value) pairs, as 2-tuples"""
        return [(k, v) for k, v in self.iteritems()]
    def iterkeys(self):
        """D.iterkeys() -> an iterator over the keys of D"""
        return (k for k, _ in self.iteritems())
    def keys(self):
        """D.iterkeys() -> an iterator over the keys of D"""
        return [k for k in self.iterkeys()]
    def itervalues(self):
        """D.itervalues() -> an iterator over the values of D"""
        return (v for _, v in self.iteritems())
    def values(self):
        """D.values() -> list of D's values"""
        return [v for v in self.itervalues()]
    def __iter__(self):
        """Iterate over the keys of D.  Consistent with dict."""
        return self.iterkeys()

def open(filename, flag='r', mode=0666):
    return SqliteMap(filename, flag=flag, mode=mode)
