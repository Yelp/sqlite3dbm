:mod:`sqlite3dbm.dbm` --- Sqlite-backed dbm
=============================================

.. module:: sqlite3dbm.dbm
   :synopsis: Sqlite-backed dbm

.. note::
   Hopefully :mod:`sqlite3dbm.dbm` will be included in Python 3.0, renamed to
   :mod:`dbm.sqlite`.

.. index:: module: dbm

This module is quite similar to the :mod:`dbm` module, but uses ``sqlite3``
instead for a backend store.  It also provides a small extension to the
traditional dictionary interface.

Module Interface
----------------
The module defines the following constant and function:

.. exception:: error

   Raised on ``sqlite3dbm.dbm``\ -specific errors, such as protection errors.
   :exc:`KeyError` is raised for general mapping errors like specifying an
   incorrect key.


.. function:: open(filename, [flag, [mode]])

   Open a ``sqlite3dbm.dbm`` database and return a ``SqliteMap`` object.  The
   *filename* argument is the path to the database file.

   The optional *flag* argument can be:

   +---------+-------------------------------------------+
   | Value   | Meaning                                   |
   +=========+===========================================+
   | ``'r'`` | Open existing database for reading only   |
   |         | (default)                                 |
   +---------+-------------------------------------------+
   | ``'w'`` | Open existing database for reading and    |
   |         | writing                                   |
   +---------+-------------------------------------------+
   | ``'c'`` | Open database for reading and writing,    |
   |         | creating it if it doesn't exist           |
   +---------+-------------------------------------------+
   | ``'n'`` | Always create a new, empty database, open |
   |         | for reading and writing                   |
   +---------+-------------------------------------------+

   The optional *mode* argument is the Unix mode of the file, used only when the
   database has to be created.  It defaults to octal ``0666``.

Extended Object Interface
-------------------------
In addition to the standard dictionary methods, ``SqliteMap`` objects have the
following methods:

.. automethod:: sqlite3dbm.dbm.SqliteMap.__getitem__
.. automethod:: sqlite3dbm.dbm.SqliteMap.select
.. automethod:: sqlite3dbm.dbm.SqliteMap.get_many

.. seealso::

   Module :mod:`dbm`
      Standard Unix database interface.

   Module :mod:`gdbm`
      Similar interface to the GUNU GDBM library.

   Module :mod:`sqlite3dbm.sshelve`
      Extension of :mod:`shelve` for a ``salite3dbm.dbm``
