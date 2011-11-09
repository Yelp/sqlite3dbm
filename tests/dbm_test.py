# -*- coding: utf-8 -*-
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

"""Test the sqlite3dbm module"""

import os
import shutil
import stat
import tempfile

import testify

import sqlite3dbm.dbm

class SqliteMapTestCase(testify.TestCase):
    """Some common functionality for open test cases"""

    @testify.setup
    def create_map(self):
        self.tmpdir = tempfile.mkdtemp()
        self.path = os.path.join(self.tmpdir, 'sqlite_map_test_db.sqlite')
        self.smap = sqlite3dbm.dbm.open(self.path, flag='c')

    @testify.teardown
    def teardown_map(self):
        shutil.rmtree(self.tmpdir)

    def prepopulate_map_test(self, d, smap):
        for k, v in d.iteritems():
            smap[k] = v

class TestSqliteMapInterface(SqliteMapTestCase):
    """Test the dictionary interface of the open"""

    def test_setitem(self):
        self.smap['darwin'] = 'drools'

        cursor = self.smap._conn.cursor()
        cursor.execute('select * from kv_table')
        rows = list(cursor.fetchall())

        testify.assert_equal(len(rows), 1)
        k, v = rows[0]
        testify.assert_equal(k, 'darwin')
        testify.assert_equal(v, 'drools')

    def test_getitem(self):
        self.smap['jugglers'] = 'awesomesauce'
        testify.assert_equal(self.smap['jugglers'], 'awesomesauce')
        testify.assert_raises(
            KeyError,
            lambda: self.smap['unicyclers']
        )

    def test_getitem_tuple(self):
        self.smap.update({
            'jason': 'fennell',
            'dave': 'marin',
        })

        testify.assert_equal(self.smap['jason','dave'], ['fennell', 'marin'])
        testify.assert_equal(self.smap['dave', 'jason'], ['marin', 'fennell'])
        testify.assert_equal(self.smap[('jason', 'dave')], ['fennell', 'marin'])
        gen = (x for x in ['jason', 'dave'])
        testify.assert_equal(self.smap[gen], ['fennell', 'marin'])
        testify.assert_raises(
            KeyError,
            lambda: self.smap['jason', 'brandon']
        )

    def test_overwrite(self):
        self.smap['yelp'] = 'yelp!'
        testify.assert_equal(self.smap['yelp'], 'yelp!')
        self.smap['yelp'] = 'yelp!1234'
        testify.assert_equal(self.smap['yelp'], 'yelp!1234')

    def test_delitem(self):
        self.smap['boo'] = 'ahhh!'
        testify.assert_equal(self.smap['boo'], 'ahhh!')
        del self.smap['boo']
        testify.assert_not_in('boo', self.smap)

        testify.assert_raises(KeyError, lambda : self.smap['boo'])

        def try_delete():
            del self.smap['boo']
        testify.assert_raises(KeyError, try_delete)

    def test_contains(self):
        self.smap['containers'] = 'blah'
        testify.assert_in('containers', self.smap)

    def test_iteritems(self):
        expected_d = {'1': 'a', '2': 'b', '3': 'c'}
        self.prepopulate_map_test(expected_d, self.smap)
        real_d = dict(self.smap.iteritems())
        testify.assert_equal(expected_d, real_d)

    def test_items(self):
        expected_d = {'1': 'a', '2': 'b', '3': 'c'}
        self.prepopulate_map_test(expected_d, self.smap)
        real_d = dict(self.smap.items())
        testify.assert_equal(expected_d, real_d)

    def test_iterkeys(self):
        d = {'1': 'a', '2': 'b', '3': 'c'}
        self.prepopulate_map_test(d, self.smap)
        expected_keys = set(d.iterkeys())
        real_keys = set(self.smap.iterkeys())
        testify.assert_equal(expected_keys, real_keys)

    def test_keys(self):
        d = {'1': 'a', '2': 'b', '3': 'c'}
        self.prepopulate_map_test(d, self.smap)
        expected_keys = set(d.keys())
        real_keys = set(self.smap.keys())
        testify.assert_equal(expected_keys, real_keys)

    def test_itervalues(self):
        d = {'1': 'a', '2': 'b', '3': 'c'}
        self.prepopulate_map_test(d, self.smap)
        expected_values = set(d.itervalues())
        real_values = set(self.smap.itervalues())
        testify.assert_equal(expected_values, real_values)

    def test_values(self):
        d = {'1': 'a', '2': 'b', '3': 'c'}
        self.prepopulate_map_test(d, self.smap)
        expected_values = set(d.values())
        real_values = set(self.smap.values())
        testify.assert_equal(expected_values, real_values)

    def test_iter(self):
        # __iter__ should behave like iterkeys()
        d = {'1': 'a', '2': 'b', '3': 'c'}
        self.prepopulate_map_test(d, self.smap)
        expected_keys = set(d.iterkeys())
        real_keys = set(self.smap)
        testify.assert_equal(expected_keys, real_keys)

    def test_len(self):
        self.smap['1'] = 'a'
        testify.assert_equal(len(self.smap), 1)
        self.smap['2'] = 'b'
        testify.assert_equal(len(self.smap), 2)
        self.smap['3'] = 'c'
        testify.assert_equal(len(self.smap), 3)
        del self.smap['2']
        testify.assert_equal(len(self.smap), 2)
        self.smap['1'] = 'z'
        testify.assert_equal(len(self.smap), 2)

    def test_clear(self):
        d = {'1': 'a', '2': 'b', '3': 'c'}

        self.prepopulate_map_test(d, self.smap)
        for k in d:
            testify.assert_in(k, self.smap)

        self.smap.clear()
        for k in d:
            testify.assert_not_in(k, self.smap)

    def test_get(self):
        self.smap['jason'] = 'fennell'
        testify.assert_equal(self.smap.get('jason'), 'fennell')
        assert self.smap.get('brandon') is None
        testify.assert_equal(self.smap.get('brandon', 'dion'), 'dion')

    def test_has_key(self):
        self.smap['jason'] = 'fennell'
        assert self.smap.has_key('jason')
        assert not self.smap.has_key('brandon')

    def test_pop(self):
        self.smap['jason'] = 'fennell'
        testify.assert_equal(self.smap.pop('jason'), 'fennell')

        testify.assert_not_in('jason', self.smap)

        assert self.smap.pop('jason', None) is None
        testify.assert_raises(
            KeyError,
            lambda: self.smap.pop('jason')
        )

    def test_popitem(self):
        d = {'1': 'a', '2': 'b'}
        self.prepopulate_map_test(d, self.smap)

        out_d = {}
        k, v = self.smap.popitem()
        out_d[k] = v
        k, v = self.smap.popitem()
        out_d[k] = v

        testify.assert_equal(out_d,  d)

        testify.assert_raises(
            KeyError,
            lambda: self.smap.popitem()
        )

    def test_setdefault(self):
        self.smap.setdefault('jason', 'fennell')
        testify.assert_equal(self.smap['jason'], 'fennell')

        self.smap.setdefault('jason', 'daniel')
        testify.assert_equal(self.smap['jason'], 'fennell')

        self.smap.setdefault('brandon')
        assert self.smap['brandon'] is None

    def test_update_dict(self):
        self.smap['foo'] = 'bar'

        names = {
            'jason': 'fennell',
            'brandon': 'fennell',
        }
        self.smap.update(names)
        names['foo'] = 'bar'
        testify.assert_equal(names, dict(self.smap.items()))

        middle_names = {
            'jason': 'daniel',
            'brandon': 'dion',
        }
        self.smap.update(middle_names)
        middle_names['foo'] = 'bar'
        testify.assert_equal(middle_names, dict(self.smap.items()))

    def test_update_list(self):
        self.smap['foo'] = 'bar'

        names = {
            'jason': 'fennell',
            'brandon': 'fennell',
        }
        self.smap.update(names.items())
        names['foo'] = 'bar'
        testify.assert_equal(names, dict(self.smap.items()))

    def test_update_iter(self):
        self.smap['foo'] = 'bar'

        names = {
            'jason': 'fennell',
            'brandon': 'fennell',
        }
        self.smap.update(names.iteritems())
        names['foo'] = 'bar'
        testify.assert_equal(names, dict(self.smap.items()))

    def test_update_kwargs(self):
        self.smap['foo'] = 'bar'

        names = {
            'jason': 'fennell',
            'brandon': 'fennell',
        }
        self.smap.update(**names)
        names['foo'] = 'bar'
        testify.assert_equal(names, dict(self.smap.items()))

    def test_select(self):
        self.smap.update({
            'jason': 'fennell',
            'dave': 'marin',
            'benjamin': 'goldenberg',
        })

        testify.assert_equal(
            self.smap.select('jason', 'dave'),
            ['fennell', 'marin']
        )
        testify.assert_equal(
            self.smap.select(['dave', 'jason']),
            ['marin', 'fennell']
        )
        gen = (x for x in ['benjamin', 'dave', 'jason'])
        testify.assert_equal(
            self.smap.select(gen),
            ['goldenberg', 'marin', 'fennell']
        )
        testify.assert_raises(
            KeyError,
            lambda: self.smap.select('jason', 'brandon')
        )

    def test_get_many(self):
        self.smap.update({
            'jason': 'fennell',
            'dave': 'marin',
            'benjamin': 'goldenberg',
        })

        testify.assert_equal(self.smap.get_many([]), [])
        testify.assert_equal(
            self.smap.get_many('jason', 'dave'),
            ['fennell', 'marin']
        )
        testify.assert_equal(
            self.smap.get_many(['dave', 'jason']),
            ['marin', 'fennell']
        )
        gen = (x for x in ['benjamin', 'dave', 'jason'])
        testify.assert_equal(
            self.smap.get_many(gen),
            ['goldenberg', 'marin', 'fennell']
        )
        testify.assert_equal(
            self.smap.get_many('jason', 'brandon'),
            ['fennell', None]
        )
        testify.assert_equal(
            self.smap.get_many('jason', 'brandon', default=''),
            ['fennell', '']
        )


class TestSqliteRegressions(SqliteMapTestCase):
    """A place for regression tests"""

    def test_huge_selects(self):
        """There is a 1000-variable limit when binding variables in sqlite
        statements.  Make sure we can do selects bigger than this
        transparently.
        """

        select_sizes = [10, 37, 100, 849, 1000, 2348, 10000]
        for size in select_sizes:
            self.smap.clear()
            self.smap.update((str(x), str(x)) for x in xrange(size))
            keys = [str(x) for x in xrange(size)]
            expected = [str(x) for x in xrange(size)]
            testify.assert_equal(
                self.smap.select(keys),
                expected,
                message='Select failed on %d elements' % size
            )

    def test_get_many_unicode_keys(self):
        """Make sure get_many works correctly with unicode keys."""
        k = u'\u6d77\u5bf6\u9ede\u5fc3\u7f8e\u98df\u574a'
        v = 'hello'
        self.smap[k] = v

        testify.assert_equal(self.smap[k], v)
        testify.assert_equal(self.smap.get(k), v)
        testify.assert_equal(self.smap.get_many([k]), [v])


class TestSqliteStorage(SqliteMapTestCase):
    """Tests things like key capacity and persistence to disk"""

    def test_multiple_open_maps_per_path(self):
        smap1 = self.smap
        smap2 = sqlite3dbm.dbm.open(self.path, flag='w')

        # Write in the first map
        smap1['foo'] = 'a'
        testify.assert_equal(smap1['foo'], 'a')
        testify.assert_equal(smap2['foo'], 'a')

        # Write in the second map
        smap2['bar'] = 'b'
        testify.assert_equal(smap1['bar'], 'b')
        testify.assert_equal(smap2['bar'], 'b')

        # Overwrite
        smap1['foo'] = 'c'
        testify.assert_equal(smap1['foo'], 'c')
        testify.assert_equal(smap2['foo'], 'c')

        # Delete
        del smap1['foo']
        testify.assert_not_in('foo', smap1)
        testify.assert_not_in('foo', smap2)

    def test_persistence_through_reopens(self):
        self.smap['foo'] = 'a'
        testify.assert_equal(self.smap['foo'], 'a')

        # Remove/close the map and open a new one
        del self.smap
        smap = sqlite3dbm.dbm.open(self.path, flag='w')
        testify.assert_equal(smap['foo'], 'a')


class TestSqliteMemoryStorage(testify.TestCase):
    """Test that storage for in-memory databases works as expected."""

    def test_multiple_in_memory_maps(self):
        # In-memory maps should not share state
        smap1 = sqlite3dbm.dbm.open(':memory:', flag='w')
        smap2 = sqlite3dbm.dbm.open(':memory:', flag='w')

        # Write to just the first map
        smap1['foo'] = 'a'
        testify.assert_equal(smap1['foo'], 'a')
        testify.assert_not_in('foo', smap2)

        # Write to just the second map
        smap2['bar'] = 'b'
        testify.assert_not_in('bar', smap1)
        testify.assert_equal(smap2['bar'], 'b')

    def test_not_persistent_through_reopen(self):
        smap = sqlite3dbm.dbm.open(':memory:', flag='w')
        smap['foo'] = 'a'
        testify.assert_equal(smap['foo'], 'a')

        # We shuld have an empty map after closing & opening a new onw
        del smap
        smap = sqlite3dbm.dbm.open(':memory:', flag='w')
        testify.assert_equal(smap.items(), [])


class SqliteCreationTest(testify.TestCase):
    """Base class for tests checking creation of open backend stores"""
    @testify.setup
    def create_tmp_working_area(self):
        self.tmpdir = tempfile.mkdtemp()
        self.path = os.path.join(self.tmpdir, 'sqlite_map_test_db.sqlite')
        # Do not yet create a db.  Most of what we are testing
        # here involves the constructor

    @testify.teardown
    def teardown_tmp_working_area(self):
        shutil.rmtree(self.tmpdir)


class TestFlags(SqliteCreationTest):
    def test_create(self):
        # Should be able to create a db when none was present
        smapcontainer = sqlite3dbm.dbm.open(self.path, flag='c')
        smap = smapcontainer

        # Writeable
        smap['foo'] = 'bar'
        testify.assert_equal(smap['foo'], 'bar')

        # Persists across re-open
        smapcontainer = sqlite3dbm.dbm.open(self.path, flag='c')
        smap = smapcontainer
        testify.assert_equal(smap['foo'], 'bar')

    def test_read_only(self):
        # Read mode exects db to already exist
        testify.assert_raises(
            sqlite3dbm.dbm.error,
            lambda: sqlite3dbm.dbm.open(self.path, flag='r'),
        )
        # Create the db then re-open read-only
        smap = sqlite3dbm.dbm.open(self.path, flag='c')
        smap = sqlite3dbm.dbm.open(self.path, flag='r')

        # Check that all mutators raise exceptions
        mutator_raises = lambda callable_method: testify.assert_raises(
            sqlite3dbm.dbm.error,
            callable_method
        )

        def do_setitem():
            smap['foo'] = 'bar'
        mutator_raises(do_setitem)
        def do_delitem():
            del smap['foo']
        mutator_raises(do_delitem)
        mutator_raises(lambda: smap.clear())
        mutator_raises(lambda: smap.pop('foo'))
        mutator_raises(lambda: smap.popitem())
        mutator_raises(lambda: smap.update({'baz': 'qux'}))

    def test_default_read_only(self):
        """Check that the default flag is read-only"""
        # Should be upset if db is not there already
        testify.assert_raises(
            sqlite3dbm.dbm.error,
            lambda: sqlite3dbm.dbm.open(self.path)
        )
        # Create and re-open
        smap = sqlite3dbm.dbm.open(self.path, flag='c')
        smap = sqlite3dbm.dbm.open(self.path)

        # Setitem should cause an error
        def do_setitem():
            smap['foo'] = 'bar'
        testify.assert_raises(
            sqlite3dbm.dbm.error,
            do_setitem
        )

    def test_writeable(self):
        # Read/write mode requites db to already exist
        testify.assert_raises(
            sqlite3dbm.dbm.error,
            lambda: sqlite3dbm.dbm.open(self.path, flag='w')
        )
        # Create db and re-open for writing
        smap = sqlite3dbm.dbm.open(self.path, flag='c')
        smap = sqlite3dbm.dbm.open(self.path, flag='w')

        # Check writeable
        smap['foo'] = 'bar'
        testify.assert_equal(smap['foo'], 'bar')

        # Check persistent through re-open
        smap = sqlite3dbm.dbm.open(self.path, flag='w')
        testify.assert_equal(smap['foo'], 'bar')

    def test_new_db(self):
        # New, empty db should be fine with file not existing
        smap = sqlite3dbm.dbm.open(self.path, flag='n')

        # Writeable
        smap['foo'] = 'bar'
        testify.assert_equal(smap['foo'], 'bar')

        # Re-open should give an empty db
        smap = sqlite3dbm.dbm.open(self.path, flag='n')
        testify.assert_not_in('foo', smap)
        testify.assert_equal(len(smap), 0)


class TestModes(SqliteCreationTest):
    # Make sure that any changes to umask do not
    # corrupt the state of other tests
    @testify.setup
    def fix_umask_setup(self):
        # Have to set a umask in order to get the current one
        self.orig_umask = os.umask(0000)
        os.umask(self.orig_umask)

    @testify.teardown
    def fix_umask_teardown(self):
        # Reset to the original umask
        os.umask(self.orig_umask)

    def get_perm_mask(self, path):
        """Pull out the permission bits of a file"""
        return stat.S_IMODE(os.stat(path).st_mode)

    def test_default_mode(self):
        # Turn off umask for inital testing of modes
        os.umask(0000)

        # Create a db, check default mode is 0666
        sqlite3dbm.dbm.open(self.path, flag='c')
        testify.assert_equal(self.get_perm_mask(self.path), 0666)

    def test_custom_mode(self):
        # Turn off umask
        os.umask(0000)

        # Create a db with a custom mode
        mode = 0600
        sqlite3dbm.dbm.open(self.path, flag='c', mode=mode)
        testify.assert_equal(self.get_perm_mask(self.path), mode)

    def test_respects_umask(self):
        mode = 0777
        umask = 0002
        os.umask(umask)
        expected_mode = mode & ~umask

        sqlite3dbm.dbm.open(self.path, flag='c', mode=mode)
        testify.assert_equal(self.get_perm_mask(self.path), expected_mode)


class SanityCheckOpen(SqliteCreationTest):
    def test_open_creates(self):
        smap = sqlite3dbm.dbm.open(self.path, flag='c')
        smap['foo'] = 'bar'
        testify.assert_equal(smap['foo'], 'bar')



class SqliteCreationTest(testify.TestCase):
    """Base class for tests checking creation of open backend stores"""
    @testify.setup
    def create_tmp_working_area(self):
        self.tmpdir = tempfile.mkdtemp()
        self.path = os.path.join(self.tmpdir, 'sqlite_map_test_db.sqlite')
        # Do not yet create a db.  Most of what we are testing
        # here involves the constructor

    @testify.teardown
    def teardown_tmp_working_area(self):
        shutil.rmtree(self.tmpdir)


class TestSqliteContainerInterface(testify.TestCase):
    def test_multiple_tables(self):
        map_db = sqlite3dbm.dbm.open_container(':memory:', flag='c')

        map_one = map_db.map_one
        map_two = map_db.map_two

        # Third map remains empty
        map_three = map_db.map_three

        map_one['darwin'] = 'drools'
        testify.assert_equal(map_one['darwin'], 'drools')
        testify.assert_not_in('darwin', map_two)

        map_two['jason'] = 'fennell'
        map_two['dave'] = 'marin'
        testify.assert_not_in('jason', map_one)
        testify.assert_not_in('dave', map_one)
        testify.assert_equal(map_two['jason'], 'fennell')
        testify.assert_equal(map_two['dave'], 'marin')

        testify.assert_equal(1, len(map_one))
        testify.assert_equal(2, len(map_two))
        testify.assert_equal(0, len(map_three))
        testify.assert_equal(3, len(map_db.mapnames()))

    def test_dropped_table(self):
        map_db = sqlite3dbm.dbm.open_container(':memory:', flag='c')

        map_one = map_db.map_one
        map_one['darwin'] = 'drools'

        testify.assert_equal(1, len(map_one))
        testify.assert_equal(1, len(map_db.mapnames()))

        map_db.drop_map('map_one')
        testify.assert_equal(0, len(map_db.mapnames()))

        map_one = map_db.map_one
        testify.assert_equal(0, len(map_one))
        testify.assert_equal(1, len(map_db.mapnames()))

    def test_table_iteration(self):
        map_db = sqlite3dbm.dbm.open_container(':memory:', flag='c')
        map_one = map_db.map_one
        map_two = map_db.map_two
        map_three = map_db.map_three

        expected_tables = set(['map_one', 'map_two', 'map_three'])
        testify.assert_equal(expected_tables, set(map_db.mapnames()))

if __name__ == '__main__':
    testify.run()
