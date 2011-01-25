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

import testify

import sqlite3dbm.dbm
import sqlite3dbm.sshelve

class TestSqliteShelf(testify.TestCase):
    @testify.setup
    def create_shelf(self):
        self.tmpdir = tempfile.mkdtemp()
        self.path = os.path.join(self.tmpdir, 'sqlite_map_test_db.sqlite')
        self.smap = sqlite3dbm.dbm.open(self.path, flag='c')
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

if __name__ == '__main__':
    testify.run()
