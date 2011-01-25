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

.. autoclass:: sqlite3dbm.sshelve.SqliteMapShelf
   :members:

.. seealso::

   Module :mod:`shelve`
      General object persistence build on top of ``dbm`` interfaces.
