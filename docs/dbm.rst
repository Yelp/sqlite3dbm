:mod:`sqlite3dbm` --- Sqlite-backed dbm
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

   Raised on ``sqlite3dbm``\ -specific errors, such as protection errors.
   :exc:`KeyError` is raised for general mapping errors like specifying an
   incorrect key.

   Accessible as ``sqlite3dbm.error``.


.. function:: open(filename, [flag, [mode]])

   Open a database and return a ``sqlite3dbm`` object.  The
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
   database has to be created.  It defaults to octal ``0666`` and respects the
   prevailing umask.

   Accessible as ``sqlite3dbm.open``.

Extended Object Interface
-------------------------
The underlying object is a ``SqliteMap``.  In addition to the standard
dictionary methods, such objects have the following methods:

.. automethod:: sqlite3dbm.dbm.SqliteMap.__getitem__
.. automethod:: sqlite3dbm.dbm.SqliteMap.select
.. automethod:: sqlite3dbm.dbm.SqliteMap.get_many

Usage Example
-------------
>>> import sqlite3dbm
>>> db = sqlite3dbm.open('mydb.sqlite3', flag='c')
>>>
>>> # Print doesn't work, you need to do .items()
>>> db
<sqlite3dbm.dbm.SqliteMap object at 0x7f0d6ecac4d0>
>>> db.items()
[]
>>>
>>> # Acts like a regular dict
>>> db['foo'] = 'bar'
>>> db['foo']
'bar'
>>> db.items()
[('foo', 'bar')]
>>> del db['foo']
>>> db.items()
[]
>>>
>>> # Some extentions that allow for batch reads
>>> db.update({'foo': 'one', 'bar': 'two', 'baz': 'three'})
>>> db['foo', 'bar']
['one', 'two']
>>> db.select('foo', 'bar')
['one', 'two']
>>> db.select('foo', 'bar', 'qux')
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "./sqlite3dbm/dbm.py", line 343, in select
    raise KeyError('One of the requested keys is missing!')
KeyError: 'One of the requested keys is missing!'
>>> db.get_many('foo', 'bar', 'qux')
['one', 'two', None]
>>> db.get_many('foo', 'bar', 'qux', default='')
['one', 'two', '']
>>>
>>> # Persistent!
>>> db.items()
[('baz', 'three'), ('foo', 'one'), ('bar', 'two')]
>>> del db
>>> reopened_db = sqlite3dbm.open('mydb.sqlite3')
>>> reopened_db.items()
[('baz', 'three'), ('foo', 'one'), ('bar', 'two')]
>>>
>>> # Be aware that the default flag is 'r'
>>> reopened_db['qux'] = 'four'
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "./sqlite3dbm/dbm.py", line 164, in __setitem__
    raise error('DB is readonly')
sqlite3dbm.dbm.SqliteMapException: DB is readonly
>>> writeable_db = sqlite3dbm.open('mydb.sqlite3', flag='w') # 'c' would be fine too
>>> writeable_db['qux'] = 'four'
>>> reopened_db.items()
[('baz', 'three'), ('foo', 'one'), ('bar', 'two'), ('qux', 'four')]
>>> writeable_db.items()
[('baz', 'three'), ('foo', 'one'), ('bar', 'two'), ('qux', 'four')]
>>>
>>> # Catching sqlite3dbm-specific errors
>>> try:
...   reopened_db['foo'] = 'blargh'
... except sqlite3dbm.error:
...   print 'Caught a module-specific error'
...
Caught a module-specific error


.. seealso::

   Module :mod:`dbm`
      Standard Unix database interface.

   Module :mod:`gdbm`
      Similar interface to the GUNU GDBM library.

   Module :mod:`sqlite3dbm.sshelve`
      Extension of :mod:`shelve` for a ``salite3dbm.dbm``
