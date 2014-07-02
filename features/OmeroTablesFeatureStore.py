#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Copyright (C) 2014 University of Dundee & Open Microscopy Environment.
# All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
Use OMERO MapAnnotations for metadata, store only the features in HDF5 tables
"""

from AbstractAPI import AbstractFeatureSetStorage, AbstractFeatureStorage
import OmeroMetadata
import omero
from omero.rtypes import unwrap

import itertools


DEFAULT_NAMESPACE = 'omero.features/0.1'
DEFAULT_COLUMN_SUBSPACE = '/featureset'
DEFAULT_ROW_SUBSPACE = '/sample'


class TableStoreException(Exception):
    """
    Parent class for exceptions occuring in the OMERO.features tables store
    implementation
    """
    pass


class OmeroTableException(TableStoreException):
    """
    Errors whilst using the OMERO.tables API
    """
    pass


class NoTableMatchException(TableStoreException):
    """
    No matching annotation was found when searching for a table
    """
    pass


class TooManyTablesException(TableStoreException):
    """
    Too many matching annotation were found when searching for a table
    """
    pass


class InvalidAnnotationException(TableStoreException):
    """
    Errors in the keys or values of an annotation used for linking features
    and samples
    """
    pass


class TableUsageException(TableStoreException):
    """
    Invalid usage of this implementation of the Features API
    """
    pass


class FeatureSetTableStore(AbstractFeatureSetStorage):
    """
    A single feature set.
    Each element is a fixed width array of doubles
    """

    def __init__(self, session, column_space, row_space, fsmeta, create=None):
        self.session = session
        self.cma = OmeroMetadata.MapAnnotations(session, column_space)
        self.rma = OmeroMetadata.MapAnnotations(session, row_space)
        self.fsmeta = fsmeta
        self.table = None
        self.header = None
        self.chunk_size = None
        self.get_table(create)

    def close(self):
        if self.table:
            self.table.close()
            self.table = None

    def get_table(self, create=None):
        if self.table:
            if create:
                raise TableUsageException(
                    'New table requested but already open: ns:%s %s' % (
                        self.cma.namespace, str(self.fsmeta)))
            assert self.cols
            return self.table
        a = self.cma.query_by_map_ann(
            dict(self.fsmeta.items()), projection=True)
        if create:
            if len(a) != 0:
                raise TooManyTablesException(
                    'Annotation already exists for new table: ns:%s %s' % (
                        self.cma.namespace, str(self.fsmeta)))
            self.new_table(create)
        else:
            if len(a) < 1:
                raise NoTableMatchException(
                    'No annotations found for: ns:%s %s' % (
                        self.cma.namespace, str(self.fsmeta)))
            if len(a) > 1:
                raise TooManyTablesException(
                    'Multiple annotations found for: ns:%s %s' % (
                        self.cma.namespace, str(self.fsmeta)))
            tid = long(a.values()[0]['_tableid'])
            self.open_table(tid)
        return self.table

    def new_table(self, column_desc):
        meta = dict(self.fsmeta.items())
        if '_tableid' in meta:
            raise InvalidAnnotationException(
                'Reserved key already present in fsmeta: %s', '_tableid')

        name = self.desc_to_str(self.fsmeta)
        self.table = self.session.sharedResources().newTable(0, name)
        if not self.table:
            raise OmeroTableException('Failed to create table: %s' % name)
        tid = unwrap(self.table.getOriginalFile().getId())

        typemap = {
            int: omero.grid.LongArrayColumn,
            long: omero.grid.LongArrayColumn,
            float: omero.grid.DoubleArrayColumn,
            str: omero.grid.StringColumn,
            }

        coldef = []
        for d in column_desc:
            type, name, size = d
            coldef.append(typemap[type](name, '', size))

        self.table.initialize(coldef)
        self.cols = self.table.getHeaders()
        if not self.cols:
            raise OmeroTableException(
                'Failed to get columns for table ID:%d' % tid)

        meta['_tableid'] = str(tid)
        self.cma.create_map_ann(meta)

    def open_table(self, tid):
        self.table = self.session.sharedResources().openTable(
            omero.model.OriginalFileI(tid))
        if not self.table:
            raise OmeroTableException('Failed to open table ID:%d' % tid)
        self.cols = self.table.getHeaders()
        if not self.cols:
            raise OmeroTableException(
                'Failed to get columns for table ID:%d' % tid)

    def store1(self, rowmeta, values):
        # TODO Check for existing annotation
        # TODO Check only one row in values
        off = self.table.getNumberOfRows()
        for n in xrange(len(self.cols)):
            self.cols[n].values = [values[n]]
        self.table.addData(self.cols)
        tid = unwrap(self.get_table().getOriginalFile().getId())
        self.rma.create_map_ann(dict(
            [('_tableid', str(tid)), ('_offset', str(off))] + rowmeta.items()))

    def store(self, rowmetas, values):
        for (rowmeta, value) in itertools.izip(rowmetas, values):
            self.store1(rowmeta, value)

    def fetch(self, rowquery):
        tid = unwrap(self.get_table().getOriginalFile().getId())
        anns = self.rma.query_by_map_ann(dict(
            rowquery.items() + [('_tableid', str(tid))]), projection=True)
        # anns is a dict of annotation-id:map-value, drop the id
        mas = anns.values()
        offs = [long(a['_offset']) for a in mas]
        values = self.chunked_table_read(offs, self.get_chunk_size())

        # Convert into row-wise storage
        if len(values) > 1:
            return mas, zip(*values)
        else:
            return mas, [tuple([v]) for v in values[0]]

    def get_chunk_size(self):
        """
        Ice has a maximum message size. Use a very rough heuristic to decide
        how many table rows to read in one go

        Assume only doubles are stored (8 bytes), and keep the table chunk size
        to <16MB
        """
        if not self.chunk_size:
            rowsize = sum(c.size for c in self.cols)
            self.chunk_size = max(16777216 / (rowsize * 8), 1)

        return self.chunk_size

    def chunked_table_read(self, offsets, chunk_size):
        values = None

        print 'Chunk size: %d' % chunk_size
        for n in xrange(0, len(offsets), chunk_size):
            print '  Chunk offset: %d+%d' % (n, chunk_size)
            data = self.table.readCoordinates(offsets[n:(n + chunk_size)])
            if values is None:
                values = [c.values for c in data.columns]
            else:
                for c, v in itertools.izip(data.columns, values):
                    v.extend(c.values)

        return values

    @staticmethod
    def desc_to_str(d):
        def esc(s):
            return s.replace('\\', '\\\\').replace('_', '\\_').replace(
                '=', '\\=')
        s = '_'.join('%s=%s' % (esc(k), esc(d[k])) for k in sorted(d.keys()))
        return s


class LRUCache(object):
    """
    A naive least-recently-used cache. Removal is O(n)
    TODO: Improve efficiency
    """

    def __init__(self, size):
        self.maxsize = size
        self.cache = {}
        self.counter = 0

    def __len__(self):
        return len(self.cache)

    def get(self, key, miss=None):
        try:
            v = self.cache[key]
            return v[0]
        except KeyError:
            return miss

    def insert(self, key, value):
        if key not in self.cache and len(self.cache) >= self.maxsize:
            self.remove_oldest()
        self.counter += 1
        self.cache[key] = [value, self.counter]

    def remove_oldest(self):
        key = None
        c = self.counter
        for k, v in self.cache.iteritems():
            if v[1] <= c:
                c = v[1]
                key = k
        del self.cache[key]


class LRUTableCache(LRUCache):
    """
    Automatically closes a table handle when it is removed from the cache
    """
    def remove_oldest(self):
        key = None
        c = self.counter
        for k, v in self.cache.iteritems():
            if v[1] <= c:
                c = v[1]
                key = k
        self.cache[key][0].close()
        del self.cache[key]

    def close(self):
        while self.cache:
            print 'close', self.cache
            self.remove_oldest()


class FeatureTableStore(AbstractFeatureStorage):
    """
    Manage storage of feature files and feature-set/context metadata
    """

    def __init__(self, session, **kwargs):
        self.session = session
        namespace = kwargs.get('namespace', DEFAULT_NAMESPACE)
        self.column_space = kwargs.get(
            'column_space', namespace + DEFAULT_COLUMN_SUBSPACE)
        self.row_space = kwargs.get(
            'row_space', namespace + DEFAULT_ROW_SUBSPACE)
        self.cachesize = kwargs.get('cachesize', 10)
        self.fss = LRUTableCache(kwargs.get('cachesize', 10))

    def create_feature_set(self, fsmeta, col_desc):
        fskey = tuple(sorted(fsmeta.iteritems()))
        r = FeatureSetTableStore(
            self.session, self.column_space, self.row_space, fsmeta, col_desc)
        self.fss.insert(fskey, r)

    def get_feature_set(self, fsmeta):
        fskey = tuple(sorted(fsmeta.iteritems()))
        r = self.fss.get(fskey)
        if not r:
            r = FeatureSetTableStore(
                self.session, self.column_space, self.row_space, fsmeta)
            self.fss.insert(fskey, r)
        return r

    def store(self, fsmeta, rowmetas, values):
        fs = self.get_feature_set(fsmeta)
        fs.store(rowmetas, values)

    def fetch(self, fsquery, rowquery):
        fs = self.get_feature_set(fsquery)
        return fs.fetch(rowquery)

    def close(self):
        self.fss.close()
