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
Implementation of the OMERO.features AbstractAPI
"""

import AbstractAPI
import omero
from omero.rtypes import unwrap, wrap

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


class TableUsageException(TableStoreException):
    """
    Invalid usage of this implementation of the Features API
    """
    pass


class FeatureRowException(TableStoreException):
    """
    Errors in a FeatureRow object
    """
    pass


class FeatureRow(AbstractAPI.AbstractFeatureRow):

    def __init__(self, widths=None, names=None, values=None):
        if not widths and not values:
            raise FeatureRowException(
                'At least one of widths or values must be provided')

        self._widths = widths
        if names and widths and len(names) != len(widths):
            raise FeatureRowException(
                'names and widths must have the same number of elements')
        self._names = names

        self._values = None
        if values:
            self.values = values
        self._namemap = {}

    def get_index(self, name):
        try:
            return self._namemap[name]
        except KeyError:
            self._namemap = dict(
                ni for ni in zip(self._names, xrange(len(self._names))))
            return self._namemap[name]

    def __getitem__(self, key):
        return self.values[self.get_index(key)]

    def __setitem__(self, key, value):
        i = self.get_index(key)
        if len(value) != self._widths[i]:
            raise FeatureRowException(
                'Expected array of length %d, received %d' % (
                    len(value), self._widths[i]))
        self.values[i] = value

    @property
    def names(self):
        return self._names

    @property
    def widths(self):
        return self._widths

    @property
    def values(self):
        return self._values

    @values.setter
    def values(self, value):
        if self._names and len(self._names) != len(value):
            raise FeatureRowException(
                'Expected %d elements, received %d' % (
                    len(self._names), len(value)))
        widths = [len(v) for v in value]
        if self._widths:
            if self._widths != widths:
                raise FeatureRowException(
                    'Expected elements with widths %s, received %s' % (
                        self._widths, widths))
        else:
            self._widths = widths
        self._values = value

    @values.deleter
    def values(self):
        del self._values

    def __repr__(self):
        return '%s(widths=%r, names=%r, values=%r)' % (
            self.__class__.__name__, self._widths, self._names, self._values)


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
        self.cols = None
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
            if coldesc:
                raise TableUsageException(
                    'New table requested but already open: ns:%s %s' % (
                        self.ft_space, self.name))
            assert self.cols
            return self.table

        tablefile = self.get_objects(
            'OriginalFile', {'name': self.name, 'path': self.ft_space})
        if coldesc:
            if tablefile:
                raise TooManyTablesException(
                    'File path:%s name:%s already exists' % (
                        self.ft_space, self.name))
            self.new_table(coldesc)
        else:
            if len(tablefile) < 1:
                raise NoTableMatchException(
                    'No files found for path:%s name:%s' % (
                        self.ft_space, self.name))
            if len(tablefile) > 1:
                raise TooManyTablesException(
                    'Multiple files found for path:%s name:%s' % (
                        self.ft_space, self.name))
            self.open_table(tablefile[0])
        return self.table

    def new_table(self, coldesc):
        self.table = self.session.sharedResources().newTable(0, self.name)
        if not self.table:
            raise OmeroTableException('Failed to create table: %s' % self.name)
        tof = self.table.getOriginalFile()

        coldef = [
            omero.grid.ImageColumn('ImageID', ''),
            omero.grid.RoiColumn('RoiID', '')
        ]
        for name, width in coldesc:
            coldef.append(omero.grid.DoubleArrayColumn(name, '', width))

        self.table.initialize(coldef)
        self.cols = self.table.getHeaders()
        if not self.cols:
            raise OmeroTableException(
                'Failed to get columns for table ID:%d' % unwrap(tof.getId()))

        tof.setPath(wrap(self.ft_space))
        tof = self.session.getUpdateService().saveAndReturnObject(tof)

    def open_table(self, tablefile):
        tid = unwrap(tablefile.getId())
        self.table = self.session.sharedResources().openTable(tablefile)
        if not self.table:
            raise OmeroTableException('Failed to open table ID:%d' % tid)
        self.cols = self.table.getHeaders()
        if not self.cols:
            raise OmeroTableException(
                'Failed to get columns for table ID:%d' % tid)

    def store_by_image(self, image_id, values):
        self.store_by_object('Image', image_id, values)

    def store_by_roi(self, roi_id, values):
        self.store_by_object('Roi', roi_id, values)

    def store_by_object(self, object_type, object_id, values):
        if object_type == 'Image':
            self.cols[0].values = [object_id]
            self.cols[1].values = [0]
        elif object_type == 'Roi':
            self.cols[1].values = [object_id]
            self.cols[0].values = [0]
        else:
            raise TableUsageException(
                'Invalid object type: %s' % object_type)

        for n in xrange(2, len(self.cols)):
            self.cols[n].values = [values[n - 2]]
        self.table.addData(self.cols)
        self.create_file_annotation(
            object_type, object_id, self.ann_space,
            self.table.getOriginalFile())

    def store(self, object_type, object_ids, valuess):
        for (object_id, values) in itertools.izip(object_ids, valuess):
            self.store_by_object(object_type, object_id, values)

    def fetch_by_image(self, image_id):
        nrows, values = self.fetch_by_object('Image', image_id)
        if nrows != 1:
            raise TableUsageException(
                'Multiple feature rows found for Image %d' % image_id)
        return self.feature_row(values)

    def fetch_by_roi(self, roi_id):
        nrows, values = self.fetch_by_object('Roi', roi_id)
        if nrows != 1:
            raise TableUsageException(
                'Multiple feature rows found for Roi %d' % roi_id)
        return self.feature_row(values)

    def fetch_by_object(self, object_type, object_id):
        if object_type in ('Image', 'Roi'):
            cond = '(%sID==%d)' % (object_type, object_id)
        else:
            raise TableUsageException(
                'Unsupported object type: %s' % object_type)
        offsets = self.table.getWhereList(
            cond, {}, 0, self.table.getNumberOfRows(), 0)
        values = self.chunked_table_read(offsets, self.get_chunk_size())

        # Convert into row-wise storage
        nrows = len(offsets)
        if nrows > 1:
            return nrows, zip(*values)
        else:
            return nrows, [tuple([v]) for v in values[0]]

    def feature_row(self, values):
        return FeatureRow(
            names=[h.name for h in self.header[2:]], values=values[2:])

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

    def get_objects(self, object_type, kvs):
        params = omero.sys.ParametersI()

        qs = self.session.getQueryService()
        conditions = []

        for k, v in kvs.iteritems():
            if isinstance(v, list):
                conditions.append(
                    '%s in (:%s)' % (k, k))
            else:
                conditions.append(
                    '%s = :%s' % (k, k))
            params.add(k, wrap(v))

        q = 'FROM %s' % object_type
        if conditions:
            q += ' WHERE ' + ' AND '.join(conditions)

        results = qs.findAllByQuery(q, params)
        return results

    def create_file_annotation(self, object_type, object_id, ns, ofile):
        obj = self.get_objects(object_type, object_id)
        assert obj
        link = getattr(omero.model, '%sAnnotationLinkI' % object_type)()
        ann = omero.model.FileAnnotationI()
        ann.setNs(wrap(ns))
        ann.setFile(ofile)
        link.setParent(obj)
        link.setChild(ann)
        ann = self.session.getUpdateService().saveAndReturnObject(link)
        return ann





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
