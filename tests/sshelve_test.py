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

"""Test the shelve wrapper around the SqliteMap"""

import os
import shutil
import tempfile
import time

import testify

import sqlite3dbm

class TestSqliteShelf(testify.TestCase):
    @testify.setup
    def create_shelf(self):
        self.tmpdir = tempfile.mkdtemp()
        self.path = os.path.join(self.tmpdir, 'sqlite_map_test_db.sqlite')
        self.smap = sqlite3dbm.open(self.path, flag='c')
        self.smap_shelf = sqlite3dbm.sshelve.SqliteMapShelf(self.smap)

    @testify.teardown
    def teardown_shelf(self):
        shutil.rmtree(self.tmpdir)

    def test_basic_serialization(self):
        def check(k, v):
            self.smap_shelf[k] = v
            testify.assert_equal(self.smap_shelf[k], v)

        check('foo', 'bar')
        check('foo', True)
        check('foo', False)
        check('foo', [1, 2, 3])
        check('foo', (1, 2, 3))
        check('foo', {'a': 1, 'b': 2})
        check('foo', [{'a': 1}, [2, 3], 4])

    def test_select(self):
        droid = ['R2-D2', 'C-3P0']
        self.smap_shelf.update({
            'jason': 'fennell',
            'droid': droid,
            'pi': 3.14
        })

        testify.assert_equal(
            self.smap_shelf.select('jason', 'droid', 'pi'),
            ['fennell', droid, 3.14]
        )
        testify.assert_raises(
            KeyError,
            lambda: self.smap_shelf.select('jason', 'droid', 'brandon'),
        )

    def test_get_many(self):
        droid = ['R2-D2', 'C-3P0']
        self.smap_shelf.update({
            'jason': 'fennell',
            'droid': droid,
            'pi': 3.14
        })

        testify.assert_equal(
            self.smap_shelf.get_many('jason', 'droid', 'pi'),
            ['fennell', droid, 3.14]
        )
        testify.assert_equal(
            self.smap_shelf.get_many('jason', 'droid', 'brandon'),
            ['fennell', droid, None]
        )
        testify.assert_equal(
            self.smap_shelf.get_many('jason', 'droid', 'brandon', default=0),
            ['fennell', droid, 0]
        )

    def test_update(self):
        droid = ['R2-D2', 'C-3P0']
        self.smap_shelf.update({
            'jason': 'fennell',
            'droid': droid,
            'pi': 3.14
        })

        testify.assert_equal(self.smap_shelf['jason'], 'fennell')
        testify.assert_equal(self.smap_shelf['droid'], droid)
        testify.assert_equal(self.smap_shelf['pi'], 3.14)

    def test_clear(self):
        droid = ['R2-D2', 'C-3P0']
        self.smap_shelf.update({
            'jason': 'fennell',
            'droid': droid,
            'pi': 3.14
        })

        testify.assert_equal(self.smap_shelf['jason'], 'fennell')
        testify.assert_equal(len(self.smap_shelf), 3)

        self.smap_shelf.clear()

        testify.assert_equal(len(self.smap_shelf), 0)
        testify.assert_not_in('jason', self.smap_shelf)

    def test_preserves_unicode(self):
        """Be paranoid about unicode."""
        k = u'café'.encode('utf-8')
        v = u'bläserforum'
        self.smap_shelf[k] = v

        testify.assert_equal(self.smap_shelf[k], v)
        testify.assert_equal(self.smap_shelf.get_many([k]), [v])


class TestShelfOpen(testify.TestCase):
    @testify.setup
    def create_shelf(self):
        self.tmpdir = tempfile.mkdtemp()
        self.path = os.path.join(self.tmpdir, 'sqlite_map_test_db.sqlite')

    @testify.teardown
    def teardown_shelf(self):
        shutil.rmtree(self.tmpdir)

    def test_open(self):
        smap_shelf = sqlite3dbm.sshelve.open(self.path)
        smap_shelf['foo'] = ['bar', 'baz', 'qux']
        testify.assert_equal(smap_shelf['foo'], ['bar', 'baz', 'qux'])


class TestShelfPerf(testify.TestCase):
    @testify.setup
    def create_environ(self):
        self.tmpdir = tempfile.mkdtemp()

    @testify.teardown
    def teardown(self):
        shutil.rmtree(self.tmpdir)

    def test_update_perf(self):
        """update() should be faster than lots of individual inserts"""

        # Knobs that control how long this test takes vs. how accurate it is
        # This test *should not flake*, but if you run into problems then you
        # should increase `insert_per_iter` (the test will take longer though)
        num_iters = 5
        insert_per_iter = 300
        min_ratio = 10

        # Setup dbs
        def setup_dbs(name):
            name = name + '%d'
            db_paths = [
                os.path.join(self.tmpdir, name % i)
                for i in xrange(num_iters)
            ]
            return [sqlite3dbm.sshelve.open(path) for path in db_paths]
        update_dbs = setup_dbs('update')
        insert_dbs = setup_dbs('insert')

        # Setup data
        insert_data = [
            ('foo%d' % i, 'bar%d' % i)
            for i in xrange(insert_per_iter)
        ]

        # Time upates
        update_start = time.time()
        for update_db in update_dbs:
            update_db.update(insert_data)
        update_time = time.time() - update_start

        # Time inserts
        insert_start = time.time()
        for insert_db in insert_dbs:
            for k, v in insert_data:
                insert_db[k] = v
        insert_time = time.time() - insert_start

        # Inserts should take a subsantially greater amount of time
        testify.assert_gt(insert_time, min_ratio*update_time)


if __name__ == '__main__':
    testify.run()
