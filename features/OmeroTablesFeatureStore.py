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

from AbstractAPI import AbstractFeatureStorageManager, FeatureRow
import OmeroMetadata
import omero
from omero.rtypes import unwrap

import itertools


DEFAULT_NAMESPACE = 'omero.features/0.1'
DEFAULT_FEATURE_SUBSPACE = 'features'
DEFAULT_ANNOTATION_SUBSPACE = 'source'


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


class FeatureTable(object):
    """
    A feature store.
    Each row is an array of fixed width DoubleArrays
    """

    def __init__(self, session, name, ft_space, ann_space, coldesc=None):
        self.session = session
        self.name = name
        self.ft_space = ft_space
        self.ann_space = ann_space
        self.table = None
        self.header = None
        self.chunk_size = None
        self.get_table(coldesc)

    def close(self):
        if self.table:
            self.table.close()
            self.table = None

    def get_table(self, coldesc=None):
        if self.table:
            if create:
                raise TableUsageException(
                    'New table requested but already open: ns:%s %s' % (
                        self.ft_space, self.name)
            assert self.cols
            return self.table

        f = conn.getObjects('OriginalFile', attributes={
            'name': self.name, 'path': self.ft_space})
        if coldesc:
            if len(f) != 0:
                raise TooManyTablesException(
                    'File path:%s name:%s already exists' % (
                        self.ft_space, self.name))
            self.new_table(coldesc)
        else:
            if len(f) < 1:
                raise NoTableMatchException(
                    'No files found for path:%s name:%s') % (
                        self.ft_space, self.name))
            if len(f) > 1:
                raise TooManyTablesException(
                    'Multiple files found for path:%s name:%s') % (
                        self.ft_space, self.name))
            tid = f.id
            self.open_table(tid)
        return self.table

    def new_table(self, coldesc):
        self.table = self.session.sharedResources().newTable(0, self.name)
        if not self.table:
            raise OmeroTableException('Failed to create table: %s' % self.name)
        tid = unwrap(self.table.getOriginalFile().getId())

        typemap = {
            int: omero.grid.LongArrayColumn,
            long: omero.grid.LongArrayColumn,
            float: omero.grid.DoubleArrayColumn,
            str: omero.grid.StringColumn,
            }

        coldef = [
            omero.grid.ImageColumn('ImageID', ''),
            omero.grid.RoiColumn('RoiID', '')
        ]
        for d in coldesc:
            coldef.append(omero.grid.DoubleArrayColumn(d[0], '', d[1]))

        self.table.initialize(coldef)
        self.cols = self.table.getHeaders()
        if not self.cols:
            raise OmeroTableException(
                'Failed to get columns for table ID:%d' % tid)

        f = self.table.getOriginalFile()
        f.setPath(wrap(self.ft_space))
        f = self.session.getUpdateService().saveAndReturnObject(f)

    def open_table(self, tid):
        self.table = self.session.sharedResources().openTable(
            omero.model.OriginalFileI(tid))
        if not self.table:
            raise OmeroTableException('Failed to open table ID:%d' % tid)
        self.cols = self.table.getHeaders()
        if not self.cols:
            raise OmeroTableException(
                'Failed to get columns for table ID:%d' % tid)

    def store1(self, image_id=0, roi_id=0, values):
        self.cols[0].values = [image_id]
        self.cols[1].values = [roi_id]
        for n in xrange(2:len(self.cols)):
            self.cols[n].values = [values[n - 2]]
        self.table.addData(self.cols)
        self.create_annotation(image_id, roi_id)

    #def store(self, rowmetas, values):
    #    for (rowmeta, value) in itertools.izip(rowmetas, values):
    #        self.store1(rowmeta, value)

    def fetch_by_image(self, image_id):
        nrows, values = self._fetch_by_image_or_roi(self, image_id):
        assert nrows == 1
        return self.feature_row(values)

    def fetch_by_roi(self, roi_id):
        nrows, values = self._fetch_by_image_or_roi(self, roi_id):
        assert nrows == 1
        return self.feature_row(values)

    def fetch_by_image_or_roi(self, image_id=0, roi_id=0):
        cond = []
        if image_id:
            cond.append('(ImageID==%d)' % image_id)
        if roi_id:
            cond.append('(RoiID==%d)' % roi_id)
        cond = '|'.join(cond)
        offs = self.table.getWhereList(
            cond, {}, 0, self.table.getNumberOfRows(), 0)
        values = self.chunked_table_read(offs, self.get_chunk_size())

        # Convert into row-wise storage
        nrows = len(values)
        if nrows > 1:
            return nrows, zip(*values)
        else:
            return nrows, [tuple([v]) for v in values[0]]

    def feature_row(self, values):
        return FeatureRow(
            names=[h.name for h in self.headers[2:]], values=values[2:])

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

    def create_annotation(self, image_id=0, roi_id=0):
        objs = []
        links = []
        if imageid:
            obj = conn.getObject('Image', image_id)
            assert obj
            objs.append(obj)
            links.append(omero.model.ImageAnnotationLink())
        if roiid:
            obj = conn.getObject('ROI', roi_id)
            assert obj
            objs.append(obj)
            links.append(omero.model.RoiAnnotationLink())

        for (obj, link) in zip(objs, links):
            ann = omero.model.FileAnnotationI()
            ann.setNs(wrap(self.ft_space))
            ann.setFile(self.get_table().getOriginalFile())
            link.setParent(obj)
            link.setChild(ann)
            ann = self.session.getUpdateService().saveAndReturnObject(ann)






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

    def create(self, featureset_name, names, widths):
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
