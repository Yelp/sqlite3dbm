:mod:`sqlite3dbm.sshelve` --- Shelve extention for a ``sqlite3dbm.dbm`` object
==============================================================================

.. module:: sqlite3dbm.sshelve
   :synopsis: Shelve extention for a ``sqlite3dbm.dbm`` object

.. index:: module: shelve

This module provides a subclass of :class:`shelve.Shelf` that works for a
``sqlite3dbm.dbm`` object.  See the documentation for :class:`shelve.Shelf` to
see the context this module fits into.

Module Contents
----------------

.. function:: open(filename[, flag='c' [, mode=0666[, protocol=None[, writeback=False]]]])

   Open a persistent :mod:`sqlite3`-backed dictionary.  The *filename* specificed is the
   path to the underlying database.

   The *flag* and *mode* parameters have the same semantics as
   :func:`sqlite3dbm.dbm.open` (and, in fact, are directly passed through to
   this function).

   The *protocl* and *writeback* parameters behave as outlined in :func:`shelve.open`.

.. class:: sqlite3dbm.sshelve.SqliteMapShelf

    A subclass of :class:`shelve.Shelf` supporting :mod:`sqlite3dbm.dbm`.

    Exposes :func:`~sqlite3dbm.dbm.SqliteMap.select` and
    :func:`~sqlite3dbm.dbm.SqliteMap.get_many` which are available in
    :mod:`sqlite3dbm.dbm` but none of the other database modules.  The dict
    object passed to the constructor must support these methods, which is
    generally done by calling :func:`sqlite3dbm.dbm.open`.

    The optional `protocol` and `writeback` parameters behave the same as
    they do for :class:`shelve.Shelf`.

Usage Example
-------------
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

.. seealso::

   Module :mod:`shelve`
      General object persistence build on top of ``dbm`` interfaces.
