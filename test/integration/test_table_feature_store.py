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
from omero.rtypes import rstring, unwrap, wrap

from features import OmeroTablesFeatureStore


class FeatureTableProxy(OmeroTablesFeatureStore.FeatureTable):
    """
    Replaces __init__ so that get_table() isn't called
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


class TableStoreHelper(object):
    @staticmethod
    def assert_coltypes_equal(xs, ys):
        for x, y in itertools.izip(xs, ys):
            assert isinstance(x, omero.grid.Column)
            assert isinstance(y, omero.grid.Column)
            assert type(x) == type(y)
            assert x.name == y.name
            assert x.description == y.description
            assert getattr(x, 'size', None) == getattr(y, 'size', None)

    @staticmethod
    def get_columns(ncols):
        cols = [
            omero.grid.ImageColumn('ImageID'),
            omero.grid.RoiColumn('RoiID'),
        ]
        for n in xrange(1, ncols + 1):
            cols.append(omero.grid.DoubleArrayColumn('x%d' % n, '', n))
        return cols

    @staticmethod
    def create_table(sess, path, name):
        table = sess.sharedResources().newTable(0, 'name')
        cols = TableStoreHelper.get_columns(2)
        table.initialize(cols)
        ofile = table.getOriginalFile()
        ofile.setPath(wrap(path))
        ofile.setName(wrap(name))
        ofile = sess.getUpdateService().saveAndReturnObject(ofile)
        tid = unwrap(ofile.getId())
        table.close()
        return tid, cols

    @staticmethod
    def create_image(sess):
        im = omero.model.ImageI()
        im.setAcquisitionDate(omero.rtypes.rtime(0))
        im.setName(rstring(None))
        im = sess.getUpdateService().saveAndReturnObject(im)
        return im

    @staticmethod
    def create_roi(sess):
        roi = omero.model.RoiI()
        roi = sess.getUpdateService().saveAndReturnObject(roi)
        return roi


class TestFeatureTable(object):

    def setup_class(self):
        self.ua = UserAccount()
        self.user = self.ua.new_user()

    def teardown_class(self):
        self.ua.close()

    def setup_method(self, method):
        self.cli = omero.client()
        un = unwrap(self.user.getOmeName())
        self.sess = self.cli.createSession(un, un)
        self.name = ns = UserAccount.uuid()
        ns = UserAccount.uuid()
        self.ft_space = ns + '/features'
        self.ann_space = ns + '/source'

    def teardown_method(self, method):
        self.cli.closeSession()

    @pytest.mark.parametrize('exists', [True, False])
    def test_get_table(self, exists):
        store = FeatureTableProxy(
            self.sess, self.name, self.ft_space, self.ann_space)

        if exists:
            tid, tcols = TableStoreHelper.create_table(
                self.sess, self.ft_space, self.name)
            table = store.get_table()

            assert table and table == store.table
            TableStoreHelper.assert_coltypes_equal(store.cols, tcols)
        else:
            with pytest.raises(OmeroTablesFeatureStore.NoTableMatchException):
                store.get_table()

    def test_new_table(self):
        tcols = TableStoreHelper.get_columns(2)
        coldesc = [
            ('x1', 1),
            ('x2', 2),
        ]

        store = FeatureTableProxy(
            self.sess, self.name, self.ft_space, self.ann_space)
        store.new_table(coldesc)
        assert store.table
        TableStoreHelper.assert_coltypes_equal(store.cols, tcols)

        # Need to reload
        # ofile = store.table.getOriginalFile()
        tid = unwrap(store.table.getOriginalFile().getId())
        ofile = self.sess.getQueryService().get('OriginalFile', tid)
        assert unwrap(ofile.getName()) == self.name
        assert unwrap(ofile.getPath()) == self.ft_space

        store.close()

    def test_open_table(self):
        tid, tcols = TableStoreHelper.create_table(
            self.sess, self.ft_space, self.name)

        store = FeatureTableProxy(
            self.sess, self.name, self.ft_space, self.ann_space)
        store.open_table(omero.model.OriginalFileI(tid))
        assert store.table
        TableStoreHelper.assert_coltypes_equal(store.cols, tcols)

    def test_store_by_object(self):
        tid, tcols = TableStoreHelper.create_table(
            self.sess, self.ft_space, self.name)
        imageid = unwrap(TableStoreHelper.create_image(self.sess).getId())
        roiid = unwrap(TableStoreHelper.create_roi(self.sess).getId())

        store = FeatureTableProxy(
            self.sess, self.name, self.ft_space, self.ann_space)
        store.open_table(omero.model.OriginalFileI(tid))

        store.store_by_object('Image', imageid, [[10], [20, 30]])
        assert store.table.getNumberOfRows() == 1
        d = store.table.readCoordinates(range(0, 1)).columns
        assert len(d) == 4
        assert d[0].values == [imageid]
        assert d[1].values == [0]
        assert d[2].values == [[10]]
        assert d[3].values == [[20, 30]]

        store.store_by_object('Roi', roiid, [[90], [80, 70]])
        assert store.table.getNumberOfRows() == 2
        d = store.table.readCoordinates(range(0, 2)).columns
        assert len(d) == 4
        assert d[0].values == [imageid, 0]
        assert d[1].values == [0, roiid]
        assert d[2].values == [[10], [90]]
        assert d[3].values == [[20, 30], [80, 70]]

        qs = self.sess.getQueryService()
        q = 'SELECT l.child FROM %sAnnotationLink l WHERE l.parent.id=%d'

        anns = qs.findAllByQuery(q % ('Image', imageid), None)
        assert len(anns) == 1
        assert unwrap(anns[0].getFile().getId()) == tid

        anns = qs.findAllByQuery(q % ('Roi', roiid), None)
        assert len(anns) == 1
        assert unwrap(anns[0].getFile().getId()) == tid

        store.close()

    def test_fetch_by_object(self):
        tid, tcols = TableStoreHelper.create_table(
            self.sess, self.ft_space, self.name)

        store = FeatureTableProxy(
            self.sess, self.name, self.ft_space, self.ann_space)
        store.open_table(omero.model.OriginalFileI(tid))
        tcols[0].values = [12, 0]
        tcols[1].values = [0, 34]
        tcols[2].values = [[10], [90]]
        tcols[3].values = [[20, 30], [80, 70]]
        store.table.addData(tcols)
        assert store.table.getNumberOfRows() == 2

        rvalues = store.fetch_by_object('Image', 12)
        print rvalues
        assert len(rvalues) == 1
        assert rvalues[0] == (12, 0, [10], [20, 30])

        rvalues = store.fetch_by_object('Roi', 34)
        assert len(rvalues) == 1
        assert rvalues[0] == (0, 34, [90], [80, 70])

        store.close()
