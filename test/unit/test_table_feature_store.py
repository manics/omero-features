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
import mox
import itertools

import omero
from omero.rtypes import unwrap, wrap

from features import OmeroMetadata
from features import OmeroTablesFeatureStore


class TestLRUCache(object):

    def test_get_insert_remove_oldest(self):
        c = OmeroTablesFeatureStore.LRUCache(2)
        assert len(c) == 0

        assert c.get('key1') is None
        assert c.get('key1', -1) == -1

        c.insert('key1', 1)
        assert c.get('key1') == 1
        assert len(c) == 1

        c.insert('key1', 'a')
        assert c.get('key1') == 'a'
        assert len(c) == 1

        c.insert('key2', 2)
        assert c.get('key1') == 'a'
        assert c.get('key2') == 2
        assert len(c) == 2

        c.insert('key3', 3)
        assert c.get('key1') is None
        assert c.get('key2') == 2
        assert c.get('key3') == 3
        assert len(c) == 2


class MockMapAnnotation:
    def __init__(self, map):
        self.map = dict((k, wrap(v)) for k, v in map.iteritems())

    def getMapValue(self):
        return self.map


class MockSharedResources:
    def __init__(self, tid, table, tname):
        self.tid = tid
        self.table = table
        self.tname = tname

    def newTable(self, repoid, name):
        assert isinstance(repoid, int)
        assert isinstance(name, str)
        return self.table

    def openTable(self, o):
        assert isinstance(o, omero.model.OriginalFileI)
        assert unwrap(o.id) == self.tid
        return self.table


class MockSession:
    def __init__(self, tid, table, tname):
        self.msr = MockSharedResources(tid, table, tname)

    def sharedResources(self):
        return self.msr


class MockOriginalFile:
    def __init__(self, id):
        self.id = wrap(id)

    def getId(self):
        return self.id


class MockColumn:
    values = None

    def __eq__(self, o):
        return self.values == o.values


class MockTableData:
    columns = None


class MockTable:
    def __init__(self):
        pass

    def addData(self, cols):
        pass

    def close(self):
        pass

    def initialize(self, desc):
        pass

    def getNumberOfRows(self):
        pass

    def getOriginalFile(self):
        pass

    def readCoordinates(self):
        pass

    def getHeaders(self):
        pass


class MockFeatureSetTableStore(OmeroTablesFeatureStore.FeatureSetTableStore):
    def __init__(self, session, namespace, fsmeta):
        self.session = session
        self.ma = OmeroMetadata.MapAnnotations(session, namespace)
        self.fsmeta = {'fsname': 'a'}
        self.cols = None
        self.table = None
        self.header = None


class TestFeatureSetTableStore(object):

    def setup_method(self, method):
        self.mox = mox.Mox()

    def teardown_method(self, method):
        self.mox.UnsetStubs()

    def test_close(self):
        table = self.mox.CreateMock(MockTable)
        table.close()
        store = MockFeatureSetTableStore(None, None, None)
        store.table = table

        self.mox.ReplayAll()

        store.close()
        self.mox.VerifyAll()

    @pytest.mark.parametrize('opened', [True, False])
    def test_get_table(self, opened):
        ann = MockMapAnnotation({'fsname': 'a', '_tableid': 1})
        store = MockFeatureSetTableStore(None, None, None)
        self.mox.StubOutWithMock(store.ma, 'query_by_map_ann')
        self.mox.StubOutWithMock(store, 'open_table')
        table = self.mox.CreateMock(MockTable)

        if opened:
            store.table = table
            store.cols = object()
        else:
            store.ma.query_by_map_ann({'fsname': 'a'}).AndReturn([ann])
            store.open_table(1).AndReturn(table)

        self.mox.ReplayAll()

        assert store.get_table() == table
        self.mox.VerifyAll()

    def test_new_table(self):
        def comparecol(x, y):
            return all([
                type(x) == type(y),
                x.name == y.name,
                x.description == y.description,
                x.size == y.size,
                x.values == y.values
            ])

        def comparecols(xs, ys):
            return all([comparecol(x, y) for x, y in itertools.izip(xs, ys)])

        table = self.mox.CreateMock(MockTable)
        session = MockSession(1, table, 'table-name')
        store = MockFeatureSetTableStore(session, None, None)
        self.mox.StubOutWithMock(store.ma, 'create_map_ann')

        table.getOriginalFile().AndReturn(MockOriginalFile(1))

        tcols = [
            omero.grid.LongArrayColumn('Integers', '', 1),
            omero.grid.LongArrayColumn('Longs', '', 2),
            omero.grid.DoubleArrayColumn('Floats', '', 3),
            omero.grid.StringColumn('String', '', 4),
        ]
        table.initialize(mox.Func(lambda xs: comparecols(xs, tcols)))
        table.getHeaders().AndReturn(tcols)

        store.ma.create_map_ann({'fsname': 'a', '_tableid': '1'})

        desc = [
            (int, 'Integers', 1),
            (long, 'Longs', 2),
            (float, 'Floats', 3),
            (str, 'String', 4),
        ]

        self.mox.ReplayAll()

        store.new_table(desc)
        assert store.table == table
        assert store.cols == tcols
        self.mox.VerifyAll()

    def test_open_table(self):
        table = self.mox.CreateMock(MockTable)
        session = MockSession(1, table, None)
        store = MockFeatureSetTableStore(session, None, None)
        cols = [object]

        table.getHeaders().AndReturn(cols)
        self.mox.ReplayAll()

        store.open_table(1)
        assert store.table == table
        assert store.cols == cols
        self.mox.VerifyAll()

    def test_store1(self):
        table = self.mox.CreateMock(MockTable)
        store = MockFeatureSetTableStore(None, None, None)
        self.mox.StubOutWithMock(store.ma, 'create_map_ann')

        store.table = table
        cols = [MockColumn]
        cols[0].values = [2]
        store.cols = [MockColumn]

        table.getNumberOfRows().AndReturn(1)
        table.addData(cols)
        table.getOriginalFile().AndReturn(MockOriginalFile(3))

        d = {'_tableid': 3, '_offset': 1, 'objectid': 4}
        store.ma.create_map_ann(d)
        rowmeta = {'objectid': 4}
        self.mox.ReplayAll()

        store.store1(rowmeta, [2])
        self.mox.VerifyAll()

    def test_store(self):
        store = MockFeatureSetTableStore(None, None, None)
        self.mox.StubOutWithMock(store, 'store1')
        rowmetas = [{'objectid': 4}, {'objectid': 5}]
        values = [[1], [2]]

        store.store1(rowmetas[0], values[0])
        store.store1(rowmetas[1], values[1])
        self.mox.ReplayAll()

        store.store(rowmetas, values)
        self.mox.VerifyAll()

    def test_fetch(self):
        ann1 = MockMapAnnotation({'name': 'a', '_tableid': 1, '_offset': 1})
        ann2 = MockMapAnnotation({'name': 'a', '_tableid': 1, '_offset': 6})
        table = self.mox.CreateMock(MockTable)
        store = MockFeatureSetTableStore(None, None, None)
        store.table = table
        store.cols = [MockColumn()]

        self.mox.StubOutWithMock(table, 'getOriginalFile')
        table.getOriginalFile().AndReturn(MockOriginalFile(1))
        self.mox.StubOutWithMock(store.ma, 'query_by_map_ann')

        rowquery = {'name': 'a'}
        d = dict(rowquery.items() + [('_tableid', 1)])
        store.ma.query_by_map_ann(d).AndReturn([ann1, ann2])

        self.mox.StubOutWithMock(table, 'readCoordinates')
        values = [10, 20]
        data = MockTableData()
        col = MockColumn()
        col.values = values
        data.columns = [col]
        table.readCoordinates([1, 6]).AndReturn(data)

        self.mox.ReplayAll()

        assert store.fetch(rowquery) == [values]
        self.mox.VerifyAll()

    def test_desc_to_str(self):
        d = {'a': 'b=c', 'd=e': 'f', r'g_\h': r'\=i'}
        s = OmeroTablesFeatureStore.FeatureSetTableStore.desc_to_str(d)
        assert s == r'a=b\=c_d\=e=f_g\_\\h=\\\=i'


class TestFeatureTableStore(object):

    def setup_method(self, method):
        self.mox = mox.Mox()
        self.fsmeta = {'name': 'a', 'version': '1'}
        self.rowmetas = [{'id': '1'}, {'id': '2'}]
        self.valuess = [[1.0, 2.0], [-1.0, -2.0]]

    def teardown_method(self, method):
        self.mox.UnsetStubs()

    @pytest.mark.parametrize('opened', [True, False])
    def test_get_feature_set(self, opened):
        fs = MockFeatureSetTableStore(None, None, None)
        self.mox.StubOutWithMock(
            OmeroTablesFeatureStore, 'FeatureSetTableStore')
        fsmeta = {'version': '1.2.3', 'fsname': 'a'}
        fskey = (('fsname', 'a'), ('version', '1.2.3'))
        fts = OmeroTablesFeatureStore.FeatureTableStore(None)

        self.mox.StubOutWithMock(fts.fss, 'get')
        self.mox.StubOutWithMock(fts.fss, 'insert')

        if opened:
            fts.fss.get(fskey).AndReturn(fs)
        else:
            fts.fss.get(fskey).AndReturn(None)
            OmeroTablesFeatureStore.FeatureSetTableStore(fsmeta).AndReturn(fs)
            fts.fss.insert(fskey, fs)
        self.mox.ReplayAll()

        assert fts.get_feature_set(fsmeta) == fs
        self.mox.VerifyAll()

    def test_store(self):
        fts = OmeroTablesFeatureStore.FeatureTableStore(None)
        self.mox.StubOutWithMock(fts, 'get_feature_set')

        fsmetas = [{'fsname': 'a'}, {'fsname': 'b', 'x': '1'}]
        rowmetas = [{'objectid': 1}, {'objectid': 2}]
        values1 = [[1], [2]]
        values2 = [[3], [4]]

        fs1 = MockFeatureSetTableStore(None, None, fsmetas[0])
        fs2 = MockFeatureSetTableStore(None, None, fsmetas[1])
        self.mox.StubOutWithMock(fs1, 'store')
        self.mox.StubOutWithMock(fs2, 'store')

        fts.get_feature_set(fsmetas[0]).AndReturn(fs1)
        fs1.store(rowmetas, values1)
        fts.get_feature_set(fsmetas[1]).AndReturn(fs2)
        fs2.store(rowmetas, values2)
        self.mox.ReplayAll()

        fts.store(fsmetas, rowmetas, [values1, values2])
        self.mox.VerifyAll()

    def test_fetch(self):
        fts = OmeroTablesFeatureStore.FeatureTableStore(None)
        self.mox.StubOutWithMock(fts, 'get_feature_set')
        fsquery = {'name': 'a'}
        rowquery = {'objectid': 1}
        values = [[1, 2]]
        fs = MockFeatureSetTableStore(None, None, fsquery)
        self.mox.StubOutWithMock(fs, 'fetch')

        fts.get_feature_set(fsquery).AndReturn(fs)
        fs.fetch(rowquery).AndReturn(values)

        self.mox.ReplayAll()

        assert fts.fetch(fsquery, rowquery) == values
        self.mox.VerifyAll()
