.. sqlite3dbm documentation master file, created by
   sphinx-quickstart on Mon Jan 24 18:44:50 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

sqlite3dbm
==========

:mod:`sqlite3dbm` provides a sqlite-backed dictionary conforming to the dbm
interface, along with a shelve class that wraps the dict and provides
serialization for it.

This module was born to provide random-access extra data for Hadoop jobs on
Amazon's Elastic Map Reduce (EMR) cluster.  We used to use :mod:`bsddb` for
this because of its dead-simple dict interface.  Unfortunately, :mod:`bsddb` is
deprecated for removal from the standard library and also has inter-version
compatability problems that make it not work on EMR.  :mod:`sqlite3` is the
obvious alternative for a persistent store, but its powerful SQL interface can
be too complex when you just want a dict.  Thus, :mod:`sqlite3dbm` was born to
provide a simple dictionary API on top of the ubiquitous and easily available
:mod:`sqlite3`.

This module requres no setup or configuration once installed. Its goal is
a stupid-simple solution whenever a persistent dictionary is desired.

This module also provides a shelve class that allows the storage of arbitrary
objects in the db (the dbm interface only handles raw strings).  Using this
interface is also easy: just open your database with
:func:`sqlite3dbm.sshelve.open` instead of :func:`sqlite3dbm.open`.


Standard Usage Example
----------------------
You have some inital job where you populate the db:
    >>> import sqlite3dbm
    >>> db = sqlite3dbm.sshelve.open('mydb.sqlite3')
    >>> db['foo'] = {'count': 100, 'ctr': .3}
    >>> db['bar'] = {'count': 314, 'ctr': .168}
    >>> db.items()
    [('foo', {'count': 100, 'ctr': 0.29999999999999999}), ('bar', {'count': 314, 'ctr': 0.16800000000000001})]

Later, you have some other job that needs to use that data:
    >>> import sqlite3dbm
    >>> db = sqlite3dbm.sshelve.open('mydb.sqlite3')
    >>> db['foo']['count']
    100
    >>> db.items()
    [('foo', {'count': 100, 'ctr': 0.29999999999999999}), ('bar', {'count': 314, 'ctr': 0.16800000000000001})]


Contents
--------

.. toctree::
   :maxdepth: 3
   :numbered:

   dbm.rst
   sshelve.rst
