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

from features import OmeroTablesFeatureStore


class TestLRUCache(object):

    class MockClosable:
        def __init__(self):
            self.closed = False

        def close(self):
            assert not self.closed
            self.closed = True

    def test_get_insert(self):
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

    def test_remove_oldest(self):
        c = OmeroTablesFeatureStore.LRUCache(2)

        c.insert('key1', 1)
        c.insert('key2', 2)
        assert c.remove_oldest() == 1
        assert c.cache.keys() == ['key2']

        c.insert('key3', 3)
        c.get('key2')
        assert c.remove_oldest() == 3
        assert c.cache.keys() == ['key2']

        c.insert('key3', 3)
        c.get('key2')
        c.insert('key4', 3)
        assert sorted(c.cache.keys()) == ['key2', 'key4']

    def test_lru_closable_cache(self):
        o1 = self.MockClosable()
        o2 = self.MockClosable()
        c = OmeroTablesFeatureStore.LRUClosableCache(1)
        c.insert('key1', o1)
        c.insert('key2', o2)
        assert c.cache.keys() == ['key2']
        assert o1.closed
        assert not o2.closed

        c.close()
        assert o2.closed
        assert c.cache.keys() == []


class MockSharedResources:
    def __init__(self, tid, table):
        self.tid = tid
        self.table = table

    def newTable(self, repoid, name):
        assert isinstance(repoid, int)
        assert isinstance(name, str)
        return self.table

    def openTable(self, o):
        assert unwrap(o.id) == self.tid
        return self.table


class MockUpdateService:
    def saveAndReturnObject(self, o):
        pass

    def deleteObject(self, o):
        pass


class MockQueryService:
    def findAllByQuery(self, q, p):
        pass


class MockSession:
    def __init__(self, tid, table):
        self.us = MockUpdateService()
        self.qs = MockQueryService()
        self.msr = MockSharedResources(tid, table)

    def getUpdateService(self):
        return self.us

    def getQueryService(self):
        return self.qs

    def sharedResources(self):
        return self.msr


class MockOmeroObject:
    def __init__(self, id):
        self.id = wrap(id)

    def getId(self):
        return self.id


class MockOriginalFile:
    def __init__(self, id, name=None, path=None):
        self.id = wrap(id)
        self.name = name
        self.path = path

    def getId(self):
        return self.id

    def getName(self):
        return self.name

    def getPath(self):
        return self.path


class MockColumn:
    def __init__(self, name=None, values=None, size=None):
        self.name = name
        self.values = values
        self.size = size

    def __eq__(self, o):
        return (self.name == o.name and self.values == o.values and
                self.size == o.size)


class MockTableData:
    columns = None


class MockTable:
    def __init__(self):
        pass

    def addData(self, cols):
        pass

    def close(self):
        pass

    def getHeaders(self):
        pass

    def getNumberOfRows(self):
        pass

    def getOriginalFile(self):
        pass

    def getWhereList(self):
        pass

    def initialize(self, desc):
        pass

    def readCoordinates(self):
        pass


class MockFeatureTable(OmeroTablesFeatureStore.FeatureTable):
    def __init__(self, session):
        self.session = session
        self.name = 'table-name'
        self.ft_space = '/test/features/ft_space'
        self.ann_space = '/test/features/ann_space'
        self.cols = None
        self.table = None
        self.header = None
        self.chunk_size = None


class TestFeatureRow(object):

    def test_init(self):
        with pytest.raises(OmeroTablesFeatureStore.FeatureRowException):
            OmeroTablesFeatureStore.FeatureRow(names=['a'], values=[[1], [2]])
        with pytest.raises(OmeroTablesFeatureStore.FeatureRowException):
            OmeroTablesFeatureStore.FeatureRow(names=['a'], widths=[1, 1])
        with pytest.raises(OmeroTablesFeatureStore.FeatureRowException):
            OmeroTablesFeatureStore.FeatureRow(widths=[1], values=[[1, 2]])

        fr = OmeroTablesFeatureStore.FeatureRow(
            names=['a', 'b'], values=[[1], [2, 3]])
        assert fr.names == ['a', 'b']
        assert fr.widths == [1, 2]
        assert fr.values == [[1], [2, 3]]

        fr = OmeroTablesFeatureStore.FeatureRow(
            names=['a', 'b'], widths=[1, 2])
        assert fr.names == ['a', 'b']
        assert fr.widths == [1, 2]
        assert fr.values is None

    def test_values(self):
        fr = OmeroTablesFeatureStore.FeatureRow(
            names=['a', 'b'], widths=[1, 2])

        fr.values = [[1], [2, 3]]
        assert fr.values == [[1], [2, 3]]

        with pytest.raises(OmeroTablesFeatureStore.FeatureRowException):
            fr.values = [[0], [0]]

        assert fr.get_index('a') == 0
        assert fr.get_index('b') == 1
        assert fr['a'] == [1]
        assert fr['b'] == [2, 3]

        fr['a'] = [10]
        assert fr.values == [[10], [2, 3]]

        with pytest.raises(OmeroTablesFeatureStore.FeatureRowException):
            fr['b'] = [0]


class TestFeatureTable(object):

    def setup_method(self, method):
        self.mox = mox.Mox()

    def teardown_method(self, method):
        self.mox.UnsetStubs()

    def parameters_equal(self, a, b):
        return a.map == b.map and (
            (a.theFilter is None and b.theFilter is None) or
            (a.theFilter.__dict__ == b.theFilter.__dict__)) and (
            (a.theOptions is None and b.theOptions is None) or
            (a.theOptions.__dict__ == b.theOptions.__dict__))

    def test_close(self):
        table = self.mox.CreateMock(MockTable)
        table.close()
        store = MockFeatureTable(None)
        store.table = table

        self.mox.ReplayAll()

        store.close()
        assert store.table is None
        self.mox.VerifyAll()

    @pytest.mark.parametrize('opened', [True, False])
    @pytest.mark.parametrize('create', [True, False])
    def test_get_table(self, opened, create):
        mf = MockOriginalFile(1)
        store = MockFeatureTable(None)
        self.mox.StubOutWithMock(store, 'get_objects')
        self.mox.StubOutWithMock(store, 'open_table')
        self.mox.StubOutWithMock(store, 'new_table')
        table = self.mox.CreateMock(MockTable)

        col_desc = [('x', 1)]
        filedesc = {'name': 'table-name', 'path': store.ft_space}

        if opened:
            store.table = table
            store.cols = object()
        else:
            if create:
                store.get_objects('OriginalFile', filedesc).AndReturn(None)
                store.new_table(col_desc)
            else:
                store.get_objects('OriginalFile', filedesc).AndReturn([mf])
                store.open_table(mf)

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
                getattr(x, 'size', None) == getattr(y, 'size', None),
                x.values == y.values
            ])

        def comparecols(xs, ys):
            return all([comparecol(x, y) for x, y in itertools.izip(xs, ys)])

        table = self.mox.CreateMock(MockTable)
        session = MockSession(1, table)
        store = MockFeatureTable(session)

        mf = MockOriginalFile(1, 'table-name', store.ft_space)
        table.getOriginalFile().AndReturn(mf)

        tcols = [
            omero.grid.ImageColumn('ImageID', ''),
            omero.grid.RoiColumn('RoiID', ''),
            omero.grid.DoubleArrayColumn('x', '', 1),
        ]
        table.initialize(mox.Func(lambda xs: comparecols(xs, tcols)))
        table.getHeaders().AndReturn(tcols)

        desc = [
            ('x', 1),
        ]

        self.mox.ReplayAll()

        store.new_table(desc)
        assert store.table == table
        assert store.cols == tcols
        self.mox.VerifyAll()

    def test_open_table(self):
        mf = MockOriginalFile(1)
        table = self.mox.CreateMock(MockTable)
        session = MockSession(1, table)
        store = MockFeatureTable(session)
        cols = [object]

        table.getHeaders().AndReturn(cols)
        self.mox.ReplayAll()

        store.open_table(mf)
        assert store.table == table
        assert store.cols == cols
        self.mox.VerifyAll()

    def test_store_by_image(self):
        store = MockFeatureTable(None)
        self.mox.StubOutWithMock(store, 'store_by_object')
        values = [[34]]
        store.store_by_object('Image', 12, values)

        self.mox.ReplayAll()
        store.store_by_image(12, values)
        self.mox.VerifyAll()

    def test_store_by_roi(self):
        store = MockFeatureTable(None)
        self.mox.StubOutWithMock(store, 'store_by_object')
        values = [[34]]
        store.store_by_object('Roi', 12, values)

        self.mox.ReplayAll()
        store.store_by_roi(12, values)
        self.mox.VerifyAll()

    def test_store_by_object(self):
        table = self.mox.CreateMock(MockTable)
        store = MockFeatureTable(None)
        store.table = table
        store.cols = [MockColumn('a'), MockColumn('b'),
                      MockColumn('c', None, 1), MockColumn('d', None, 2)]

        self.mox.StubOutWithMock(table, 'addData')
        self.mox.StubOutWithMock(table, 'getOriginalFile')
        self.mox.StubOutWithMock(store, 'create_file_annotation')

        mf = MockOriginalFile(3)
        values = [[10], [20, 30]]
        cols = [MockColumn('a', [12]), MockColumn('b', [0]),
                MockColumn('c', [[10]], 1), MockColumn('d', [[20, 30]], 2)]

        table.addData(cols)
        table.getOriginalFile().AndReturn(mf)
        store.create_file_annotation('Image', 12, store.ann_space, mf)
        self.mox.ReplayAll()

        store.store_by_object('Image', 12, values)
        self.mox.VerifyAll()

    def test_store(self):
        store = MockFeatureTable(None)
        self.mox.StubOutWithMock(store, 'store_by_object')
        ids = [1, 2]
        valuess = [[[34]], [[56]]]
        store.store_by_object('Image', 1, [[34]])
        store.store_by_object('Image', 2, [[56]])

        self.mox.ReplayAll()
        store.store('Image', ids, valuess)
        self.mox.VerifyAll()

    def test_fetch_by_image(self):
        store = MockFeatureTable(None)
        self.mox.StubOutWithMock(store, 'fetch_by_object')
        self.mox.StubOutWithMock(store, 'feature_row')
        values = (1, 0, [5])
        r = object()

        store.fetch_by_object('Image', 1).AndReturn([values])
        store.feature_row(values).AndReturn(r)

        self.mox.ReplayAll()
        store.fetch_by_image(1)
        self.mox.VerifyAll()

    def test_fetch_by_roi(self):
        store = MockFeatureTable(None)
        self.mox.StubOutWithMock(store, 'fetch_by_object')
        self.mox.StubOutWithMock(store, 'feature_row')
        values = (1, 0, [5])
        r = object()

        store.fetch_by_object('Roi', 1).AndReturn([values])
        store.feature_row(values).AndReturn(r)

        self.mox.ReplayAll()
        store.fetch_by_roi(1)
        self.mox.VerifyAll()

    @pytest.mark.parametrize('ncols', [1, 2])
    @pytest.mark.parametrize('nrows', [0, 1, 2])
    def test_fetch_by_object(self, ncols, nrows):
        table = self.mox.CreateMock(MockTable)
        store = MockFeatureTable(None)
        store.table = table
        store.cols = [MockColumn() for n in xrange(ncols)]

        offsets = [3, 7][:nrows]

        self.mox.StubOutWithMock(table, 'getWhereList')
        self.mox.StubOutWithMock(table, 'getNumberOfRows')
        self.mox.StubOutWithMock(store, 'get_chunk_size')
        self.mox.StubOutWithMock(store, 'chunked_table_read')

        table.getNumberOfRows().AndReturn(123)
        table.getWhereList('(ImageID==99)', {}, 0, 123, 0).AndReturn(offsets)

        data = []
        for c in xrange(ncols):
            cvals = [[r * 10 + c] for r in xrange(1, nrows + 1)]
            data.append(cvals)

        store.get_chunk_size().AndReturn(2)
        store.chunked_table_read(offsets, 2).AndReturn(data)

        self.mox.ReplayAll()

        rvalues = store.fetch_by_object('Image', 99)

        assert len(rvalues) == len(offsets)
        if ncols == 1 and nrows == 1:
            assert rvalues == [([10],)]
        elif ncols == 1 and nrows == 2:
            assert rvalues == [([10],), ([20],)]
        elif ncols == 2 and nrows == 1:
            assert rvalues == [([10], [11])]
        elif ncols == 2 and nrows == 2:
            assert rvalues == [([10], [11]), ([20], [21])]
        else:
            assert rvalues == []
        self.mox.VerifyAll()

    def test_feature_row(self):
        store = MockFeatureTable(None)
        store.header = [MockColumn('ignore'), MockColumn('ignore'),
                        MockColumn('a'), MockColumn('b')]

        row = [0, 0, [1], [2, 3]]

        self.mox.ReplayAll()
        rv = store.feature_row(row)
        assert rv.names == ['a', 'b']
        assert rv.widths == [1, 2]
        assert rv.values == row[2:]
        self.mox.VerifyAll()

    def test_get_chunk_size(self):
        table = self.mox.CreateMock(MockTable)
        store = MockFeatureTable(None)
        store.table = table
        store.cols = [MockColumn(size=2) for n in xrange(100)]

        self.mox.ReplayAll()
        assert store.get_chunk_size() == 10485
        self.mox.VerifyAll()

    def test_chunked_table_read(self):
        table = self.mox.CreateMock(MockTable)
        store = MockFeatureTable(None)
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

    def test_get_objects(self):
        session = MockSession(None, None)
        store = MockFeatureTable(session)
        self.mox.StubOutWithMock(session.qs, 'findAllByQuery')

        kvs = {'x': 'aaa', 'y': ['bbb', 'ccc']}
        # Need to figure out which order the keys will be read
        if kvs.keys() == ['x', 'y']:
            q = "FROM ObjectType WHERE x = :x AND y in (:y)"
        else:
            q = "FROM ObjectType WHERE y in (:y) AND x = :x"

        params = omero.sys.ParametersI(wrap(kvs).val)
        m = object()

        session.qs.findAllByQuery(q, mox.Func(
            lambda o: self.parameters_equal(params, o))).AndReturn([m])

        self.mox.ReplayAll()

        assert store.get_objects('ObjectType', kvs) == [m]
        self.mox.VerifyAll()

    def test_create_file_annotation(self):
        session = MockSession(None, None)
        store = MockFeatureTable(session)
        self.mox.StubOutWithMock(store, 'get_objects')
        self.mox.StubOutWithMock(session.us, 'saveAndReturnObject')

        ofile = omero.model.OriginalFileI(2)
        image = omero.model.ImageI(2)
        store.get_objects('Image', {'id': 3}).AndReturn([image])
        mocklink = object()

        session.us.saveAndReturnObject(mox.Func(
            lambda o: o.getParent() == image and
            o.getChild().getNs() == wrap('ns') and
            o.getChild().getFile() == ofile)).AndReturn(mocklink)
        self.mox.ReplayAll()

        assert store.create_file_annotation(
            'Image', 3, 'ns', ofile) == mocklink
        self.mox.VerifyAll()

    def test_delete(self):
        table = self.mox.CreateMock(MockTable)
        session = MockSession(1, table)
        store = MockFeatureTable(session)
        store.table = table

        self.mox.StubOutWithMock(store, 'close')
        self.mox.StubOutWithMock(table, 'getOriginalFile')
        self.mox.StubOutWithMock(store, '_get_annotation_link_types')
        self.mox.StubOutWithMock(session.qs, 'findAllByQuery')
        self.mox.StubOutWithMock(session.us, 'deleteObject')

        fid = 123
        mf = MockOriginalFile(fid, 'table-name', store.ft_space)
        table.getOriginalFile().AndReturn(mf)

        store._get_annotation_link_types().AndReturn(
            ['ImageAnnotationLink', 'RoiAnnotationLink'])

        mockimlink = MockOmeroObject(12)
        mockfileann = MockOmeroObject(34)
        params = omero.sys.ParametersI()
        params.addId(fid)
        session.qs.findAllByQuery(
            'SELECT al FROM ImageAnnotationLink al WHERE al.child.file.id=:id',
            mox.Func(lambda o: self.parameters_equal(params, o))).AndReturn(
            [mockimlink])
        session.qs.findAllByQuery(
            'SELECT al FROM RoiAnnotationLink al WHERE al.child.file.id=:id',
            mox.Func(lambda o: self.parameters_equal(params, o))).AndReturn([])
        session.qs.findAllByQuery(
            'SELECT ann FROM FileAnnotation ann WHERE ann.file.id=:id',
            mox.Func(lambda o: self.parameters_equal(params, o))).AndReturn(
            [mockfileann])

        store.close()

        session.us.deleteObject(mockimlink)
        session.us.deleteObject(mockfileann)
        session.us.deleteObject(mf)

        self.mox.ReplayAll()
        store.delete()
        self.mox.VerifyAll()

    def test_get_annotation_link_types(self):
        store = MockFeatureTable(None)
        types = store._get_annotation_link_types()
        assert 'ImageAnnotationLink' in types
        assert 'RoiAnnotationLink' in types


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
