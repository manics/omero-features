#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Copyright (C) 2014 University of Dundee & Open Microscopy Environment
# All Rights Reserved.
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

import pytest

from integration_test_lib import UserAccount

import itertools

import omero
from omero.rtypes import unwrap, wrap

from features import OmeroMetadata
from features import OmeroTablesFeatureStore


class FeatureSetTableStoreProxy(OmeroTablesFeatureStore.FeatureSetTableStore):
    """
    Replaces __init__ so the get_table() isn't called
    """
    def __init__(self, session, namespace, fsmeta):
        self.session = session
        self.ma = OmeroMetadata.MapAnnotations(session, namespace)
        self.fsmeta = fsmeta
        self.table = None
        self.header = None


class TestFeatureSetTableStore(object):

    def setup_class(self):
        self.ua = UserAccount()
        self.user = self.ua.new_user()

    def teardown_class(self):
        self.ua.close()

    def setup_method(self, method):
        self.cli = omero.client()
        un = unwrap(self.user.getOmeName())
        self.sess = self.cli.createSession(un, un)
        self.fsmeta = {'fsname': 'a'}

    def teardown_method(self, method):
        self.cli.closeSession()

    @staticmethod
    def assert_cols_equal(xs, ys):
        for x, y in itertools.izip(xs, ys):
            assert isinstance(x, omero.grid.Column)
            assert isinstance(y, omero.grid.Column)
            assert type(x) == type(y)
            assert x.name == y.name
            assert x.description == y.description
            assert x.size == y.size
            assert (not x.values and not y.values) or (x.values == y.values)

    def create_table(self, ns):
        table = self.sess.sharedResources().newTable(0, 'test.h5')
        cols = [
            omero.grid.LongArrayColumn('test1', '', 2),
            omero.grid.DoubleArrayColumn('test2', '', 1)
            ]
        table.initialize(cols)
        tid = str(unwrap(table.getOriginalFile().getId()))
        table.close()

        m = omero.model.MapAnnotationI()
        m.setNs(wrap(ns))
        d = dict((k, v) for k, v in self.fsmeta.iteritems())
        d['_tableid'] = tid
        m.setMapValue(wrap(d).val)
        m = self.sess.getUpdateService().saveAndReturnObject(m)

        return tid, cols

    @pytest.mark.parametrize('exists', [True, False])
    def test_get_table(self, exists):
        ns = UserAccount.uuid()
        store = FeatureSetTableStoreProxy(self.sess, ns, self.fsmeta)

        if exists:
            tid, tcols = self.create_table(ns)
            table = store.get_table()

            assert table and table == store.table
            self.assert_cols_equal(store.cols, tcols)
        else:
            with pytest.raises(Exception):
                store.get_table()

    def test_new_table(self):

        tcols = [
            omero.grid.LongArrayColumn('Integers', '', 1),
            omero.grid.LongArrayColumn('Longs', '', 2),
            omero.grid.DoubleArrayColumn('Floats', '', 3),
            omero.grid.StringColumn('String', '', 4),
        ]

        ns = UserAccount.uuid()

        desc = [
            (int, 'Integers', 1),
            (long, 'Longs', 2),
            (float, 'Floats', 3),
            (str, 'String', 4),
        ]

        store = FeatureSetTableStoreProxy(self.sess, ns, self.fsmeta)
        store.new_table(desc)
        assert store.table
        self.assert_cols_equal(store.cols, tcols)

        tid = unwrap(store.table.getOriginalFile().getId())

        params = omero.sys.ParametersI()
        params.addString('ns', ns)
        ann = self.sess.getQueryService().findByQuery(
            'from MapAnnotation ann join fetch ann.mapValue where ann.ns=:ns',
            params)

        assert unwrap(ann.getNs()) == ns
        m = unwrap(ann.getMapValue())
        assert len(m) == 2
        assert m['fsname'] == 'a'
        assert m['_tableid'] == str(tid)

        store.close()

    def test_open_table(self):
        ns = UserAccount.uuid()
        tid, tcols = self.create_table(ns)

        store = FeatureSetTableStoreProxy(self.sess, ns, self.fsmeta)
        store.open_table(tid)
        assert store.table
        self.assert_cols_equal(store.cols, tcols)

    def test_store1(self):
        ns = UserAccount.uuid()
        tid, tcols = self.create_table(ns)

        store = OmeroTablesFeatureStore.FeatureSetTableStore(
            self.sess, ns, self.fsmeta)
        self.assert_cols_equal(store.cols, tcols)
        assert store.table.getNumberOfRows() == 0

        rowmeta = {'objectid': 4}
        values = [[1, 2], [-1.0]]
        store.store1(rowmeta, values)

        assert store.table.getNumberOfRows() == 1
        data = store.table.readCoordinates([0])
        assert data.columns[0].values == [values[0]]
        assert data.columns[1].values == [values[1]]

        params = omero.sys.ParametersI()
        params.addString('ns', ns)

        # TODO: Need to have separate namespaces for column and row annotations

        #ann = self.sess.getQueryService().findByQuery(
        #    'from MapAnnotation ann join fetch ann.mapValue where ann.ns=:ns',
        #    params)
        ann = self.sess.getQueryService().findAllByQuery(
            'from MapAnnotation ann join fetch ann.mapValue where ann.ns=:ns '
            'order by ann.id',
            params)
        assert len(ann) == 2
        ann = ann[1]

        assert unwrap(ann.getNs()) == ns
        m = unwrap(ann.getMapValue())
        assert len(m) == 3
        assert m['objectid'] == '4'
        assert m['_tableid'] == str(tid)
        assert m['_offset'] == '0'

        store.close()

    def test_store(self):
        ns = UserAccount.uuid()
        tid, tcols = self.create_table(ns)

        store = OmeroTablesFeatureStore.FeatureSetTableStore(
            self.sess, ns, self.fsmeta)
        self.assert_cols_equal(store.cols, tcols)
        assert store.table.getNumberOfRows() == 0

        rowmetas = [{'objectid': 4}, {'objectid': 5}]
        valuess = [([1, 2], [-1.0]), ([3, 4], [-2.0])]
        store.store(rowmetas, valuess)

        assert store.table.getNumberOfRows() == 2
        data = store.table.readCoordinates([0, 1])
        assert data.columns[0].values == [valuess[0][0], valuess[1][0]]
        assert data.columns[1].values == [valuess[0][1], valuess[1][1]]

        params = omero.sys.ParametersI()
        params.addString('ns', ns)

        # TODO: Need to have separate namespaces for column and row annotations

        #ann = self.sess.getQueryService().findByQuery(
        #    'from MapAnnotation ann join fetch ann.mapValue where ann.ns=:ns',
        #    params)
        ann = self.sess.getQueryService().findAllByQuery(
            'from MapAnnotation ann join fetch ann.mapValue where ann.ns=:ns '
            'order by ann.id',
            params)
        assert len(ann) == 3
        ann1 = ann[1]
        ann2 = ann[2]

        assert unwrap(ann1.getNs()) == ns
        m1 = unwrap(ann1.getMapValue())
        assert len(m1) == 3
        assert m1['objectid'] == '4'
        assert m1['_tableid'] == str(tid)
        assert m1['_offset'] == '0'

        assert unwrap(ann2.getNs()) == ns
        m2 = unwrap(ann2.getMapValue())
        assert len(m2) == 3
        assert m2['objectid'] == '5'
        assert m2['_tableid'] == str(tid)
        assert m2['_offset'] == '1'

        store.close()

    def test_fetch(self):
        ns = UserAccount.uuid()
        tid, tcols = self.create_table(ns)

        store = OmeroTablesFeatureStore.FeatureSetTableStore(
            self.sess, ns, self.fsmeta)
        self.assert_cols_equal(store.cols, tcols)
        assert store.table.getNumberOfRows() == 0

        rowmetas = [{'objectid': '4'}, {'objectid': '5'}]
        valuess = [([1, 2], [-1.0]), ([3, 4], [-2.0])]
        store.store(rowmetas, valuess)

        rowquery = {'objectid': '4'}
        data = store.fetch(rowquery)
        assert data == [valuess[0]]

        rowquery = {'objectid': ['4', '5']}
        data = store.fetch(rowquery)
        assert data == valuess
