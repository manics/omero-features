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
    size = None

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
        self.cma = OmeroMetadata.MapAnnotations(session, namespace)
        self.rma = OmeroMetadata.MapAnnotations(session, namespace)
        self.fsmeta = {'fsname': 'a'}
        self.cols = None
        self.table = None
        self.header = None
        self.chunk_size = None


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
    @pytest.mark.parametrize('create', [True, False])
    def test_get_table(self, opened, create):
        ma = {3: {'fsname': 'a', '_tableid': '1'}}
        store = MockFeatureSetTableStore(None, None, None)
        self.mox.StubOutWithMock(store.cma, 'query_by_map_ann')
        self.mox.StubOutWithMock(store, 'open_table')
        self.mox.StubOutWithMock(store, 'new_table')
        table = self.mox.CreateMock(MockTable)
        col_desc = [(int, 'x', 1)]

        if opened:
            store.table = table
            store.cols = object()
        else:
            if create:
                store.cma.query_by_map_ann(
                    {'fsname': 'a'}, projection=True).AndReturn({})
                store.new_table(col_desc)
            else:
                store.cma.query_by_map_ann(
                    {'fsname': 'a'}, projection=True).AndReturn(ma)
                store.open_table(1)

        self.mox.ReplayAll()

        # open_table is mocked so it won't set store.table
        # assert store.get_table() == table
        if create:
            if opened:
                with pytest.raises(
                        OmeroTablesFeatureStore.TableUsageException):
                    store.get_table(col_desc)
            else:
                store.get_table(col_desc)
        else:
            store.get_table()
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
        self.mox.StubOutWithMock(store.cma, 'create_map_ann')

        table.getOriginalFile().AndReturn(MockOriginalFile(1))

        tcols = [
            omero.grid.LongArrayColumn('Integers', '', 1),
            omero.grid.LongArrayColumn('Longs', '', 2),
            omero.grid.DoubleArrayColumn('Floats', '', 3),
            omero.grid.StringColumn('String', '', 4),
        ]
        table.initialize(mox.Func(lambda xs: comparecols(xs, tcols)))
        table.getHeaders().AndReturn(tcols)

        store.cma.create_map_ann({'fsname': 'a', '_tableid': '1'})

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
        self.mox.StubOutWithMock(store.rma, 'create_map_ann')

        store.table = table
        cols = [MockColumn]
        cols[0].values = [[2]]
        store.cols = [MockColumn]

        table.getNumberOfRows().AndReturn(1)
        table.addData(cols)
        table.getOriginalFile().AndReturn(MockOriginalFile(3))

        d = {'_tableid': '3', '_offset': '1', 'objectid': '4'}
        store.rma.create_map_ann(d)
        rowmeta = {'objectid': '4'}
        self.mox.ReplayAll()

        store.store1(rowmeta, [[2]])
        self.mox.VerifyAll()

    def test_store(self):
        store = MockFeatureSetTableStore(None, None, None)
        self.mox.StubOutWithMock(store, 'store1')
        rowmetas = [{'objectid': '4'}, {'objectid': '5'}]
        values = [[[1]], [[2]]]

        store.store1(rowmetas[0], values[0])
        store.store1(rowmetas[1], values[1])
        self.mox.ReplayAll()

        store.store(rowmetas, values)
        self.mox.VerifyAll()

    @pytest.mark.parametrize('ncols', [1, 2])
    def test_fetch(self, ncols):
        mas = {
            '4': {'name': 'a', '_tableid': '1', '_offset': '1'},
            '5': {'name': 'a', '_tableid': '1', '_offset': '6'}
            }
        table = self.mox.CreateMock(MockTable)
        store = MockFeatureSetTableStore(None, None, None)
        store.table = table
        store.cols = [MockColumn() for n in xrange(ncols)]

        self.mox.StubOutWithMock(table, 'getOriginalFile')
        table.getOriginalFile().AndReturn(MockOriginalFile(1))
        self.mox.StubOutWithMock(store.rma, 'query_by_map_ann')

        rowquery = {'name': 'a'}
        d = dict(rowquery.items() + [('_tableid', '1')])
        store.rma.query_by_map_ann(d, projection=True).AndReturn(mas)

        self.mox.StubOutWithMock(store, 'get_chunk_size')
        self.mox.StubOutWithMock(store, 'chunked_table_read')

        data = []
        for n in xrange(ncols):
            cvals = [[10 + n], [20 + n]]
            data.append(cvals)

        # Need to figure out the order of the dict keys
        ks = mas.keys()
        if ks[0] == '4':
            expected_offsets = [1, 6]
            expected_ras = [mas['4'], mas['5']]
        else:
            expected_offsets = [6, 1]
            expected_ras = [mas['5'], mas['4']]

        store.get_chunk_size().AndReturn(2)
        store.chunked_table_read(expected_offsets, 2).AndReturn(data)

        self.mox.ReplayAll()

        ra, rv = store.fetch(rowquery)
        assert ra == expected_ras
        if ncols == 1:
            assert rv == [([10],), ([20],)]
        else:
            assert rv == [([10], [11]), ([20], [21])]
        self.mox.VerifyAll()

    def test_get_chunk_size(self):
        table = self.mox.CreateMock(MockTable)
        store = MockFeatureSetTableStore(None, None, None)
        store.table = table
        store.cols = [MockColumn() for n in xrange(100)]
        for col in store.cols:
            col.size = 2

        assert store.get_chunk_size() == 10485

    def test_chunked_table_read(self):
        table = self.mox.CreateMock(MockTable)
        store = MockFeatureSetTableStore(None, None, None)
        store.table = table

        self.mox.StubOutWithMock(table, 'readCoordinates')

        offsets = [2, 7, 5]

        data1 = MockTableData()
        data1.columns = [MockColumn()]
        data1.columns[0].values = [[1], [2]]

        data2 = MockTableData()
        data2.columns = [MockColumn()]
        data2.columns[0].values = [[3]]

        table.readCoordinates([2, 7]).AndReturn(data1)
        table.readCoordinates([5]).AndReturn(data2)

        self.mox.ReplayAll()

        d = store.chunked_table_read(offsets, 2)
        assert d == [[[1], [2], [3]]]
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

    def test_init(self):
        fts = OmeroTablesFeatureStore.FeatureTableStore(None)
        assert fts.column_space == 'omero.features/0.1/featureset'
        assert fts.row_space == 'omero.features/0.1/sample'

        fts = OmeroTablesFeatureStore.FeatureTableStore(None, namespace='x')
        assert fts.column_space == 'x/featureset'
        assert fts.row_space == 'x/sample'

        fts = OmeroTablesFeatureStore.FeatureTableStore(
            None, column_space='x', row_space='y')
        assert fts.column_space == 'x'
        assert fts.row_space == 'y'

    def test_create_feature_set(self):
        fs = MockFeatureSetTableStore(None, None, None)
        self.mox.StubOutWithMock(
            OmeroTablesFeatureStore, 'FeatureSetTableStore')
        fsmeta = {'version': '1.2.3', 'fsname': 'a'}
        fskey = (('fsname', 'a'), ('version', '1.2.3'))
        col_desc = [(int, 'x', 1)]
        fts = OmeroTablesFeatureStore.FeatureTableStore(None)

        OmeroTablesFeatureStore.FeatureSetTableStore(
            None, 'omero.features/0.1/featureset', 'omero.features/0.1/sample',
            fsmeta, col_desc).AndReturn(fs)

        self.mox.ReplayAll()
        fts.create_feature_set(fsmeta, col_desc)
        assert len(fts.fss) == 1
        assert fts.fss.get(fskey) == fs
        self.mox.VerifyAll()

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
            OmeroTablesFeatureStore.FeatureSetTableStore(
                None, 'omero.features/0.1/featureset',
                'omero.features/0.1/sample', fsmeta).AndReturn(fs)
            fts.fss.insert(fskey, fs)
        self.mox.ReplayAll()

        assert fts.get_feature_set(fsmeta) == fs
        self.mox.VerifyAll()

    def test_store(self):
        fts = OmeroTablesFeatureStore.FeatureTableStore(None)
        self.mox.StubOutWithMock(fts, 'get_feature_set')

        fsmeta = {'fsname': 'a'}
        rowmetas = [{'objectid': 1}, {'objectid': 2}]
        values = [[1], [2]]

        fs = MockFeatureSetTableStore(None, None, fsmeta)
        self.mox.StubOutWithMock(fs, 'store')

        fts.get_feature_set(fsmeta).AndReturn(fs)
        fs.store(rowmetas, values)
        self.mox.ReplayAll()

        fts.store(fsmeta, rowmetas, values)
        self.mox.VerifyAll()

    def test_fetch(self):
        fts = OmeroTablesFeatureStore.FeatureTableStore(None)
        self.mox.StubOutWithMock(fts, 'get_feature_set')
        fsquery = {'name': 'a'}
        rowquery = {'objectid': 1}
        values = [([1], [2])]
        fs = MockFeatureSetTableStore(None, None, fsquery)
        self.mox.StubOutWithMock(fs, 'fetch')

        fts.get_feature_set(fsquery).AndReturn(fs)
        fs.fetch(rowquery).AndReturn(([rowquery], values))

        self.mox.ReplayAll()

        ra, rv = fts.fetch(fsquery, rowquery)
        assert len(ra) == 1
        assert len(rv) == 1
        assert ra[0] == rowquery
        assert rv == values
        self.mox.VerifyAll()
