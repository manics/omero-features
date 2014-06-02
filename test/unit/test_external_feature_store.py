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

import os

from features import OmeroExternalFeatureStore
from features import OmeroMetadata


class TestFeatureSetFileStore(object):

    def setup_method(self, method):
        self.mox = mox.Mox()
        self.fsmeta = {'name': 'a'}

    def teardown_method(self, method):
        self.mox.UnsetStubs()

    def test_store1(self):
        rowmeta = {'id': '1'}
        values = [1.0, -2.0]
        ma = self.mox.CreateMock(OmeroMetadata.MapAnnotations)
        ma.create_map_ann(**{'f:name': 'a', 'r:id': '1'})#.AndReturn()
        self.mox.ReplayAll()

        fs = OmeroExternalFeatureStore.FeatureSetFileStore(ma, self.fsmeta)
        fs.store1(rowmeta, values)

        self.mox.VerifyAll()

    def test_store(self):
        rowmetas = [{'id': '1'}, {'id': '2'}]
        valuess = [[1.0, 2.0], [-1.0, -2.0]]
        ma = self.mox.CreateMock(OmeroMetadata.MapAnnotations)
        ma.create_map_ann(**{'f:name': 'a', 'r:id': '1'})#.AndReturn()
        ma.create_map_ann(**{'f:name': 'a', 'r:id': '2'})#.AndReturn()
        self.mox.ReplayAll()

        fs = OmeroExternalFeatureStore.FeatureSetFileStore(ma, self.fsmeta)
        fs.store(rowmetas, valuess)

        self.mox.VerifyAll()

    def test_query(self):
        rowquery = {'id': '1'}
        ma = self.mox.CreateMock(OmeroMetadata.MapAnnotations)
        ma.query_by_map_ann(**{'f:name': 'a', 'r:id': '1'})#.AndReturn()
        self.mox.ReplayAll()

        fs = OmeroExternalFeatureStore.FeatureSetFileStore(ma, self.fsmeta)
        fs.fetch(rowquery)

        self.mox.VerifyAll()


class TestFeatureFileStore(object):

    def setup_method(self, method):
        self.mox = mox.Mox()
        self.fsmeta = {'name': 'a', 'version': '1'}
        self.rowmetas = [{'id': '1'}, {'id': '2'}]
        self.valuess = [[1.0, 2.0], [-1.0, -2.0]]

    def teardown_method(self, method):
        self.mox.UnsetStubs()

    def test_get_feature_set(self, tmpdir):
        ma = self.mox.CreateMock(OmeroMetadata.MapAnnotations)
        mockfs = self.mox.CreateMock(
            OmeroExternalFeatureStore.FeatureSetFileStore)

        self.mox.StubOutWithMock(OmeroMetadata, 'MapAnnotations')
        self.mox.StubOutWithMock(
            OmeroExternalFeatureStore, 'FeatureSetFileStore')
        OmeroMetadata.MapAnnotations('session', 'omero.features').AndReturn(ma)
        OmeroExternalFeatureStore.FeatureSetFileStore(self.fsmeta).AndReturn(
            mockfs)

        self.mox.ReplayAll()

        ffs = OmeroExternalFeatureStore.FeatureFileStore(tmpdir, 'session')
        assert ffs.fss == {}
        fs = ffs.get_feature_set(self.fsmeta)
        assert fs == mockfs
        assert ffs.fss == {(('name', 'a'), ('version', '1')): mockfs}

        # Second call should not create a new FeatureSetFileStore
        fs2 = ffs.get_feature_set(self.fsmeta)
        assert fs2 == mockfs
        assert ffs.fss == {(('name', 'a'), ('version', '1')): mockfs}

        self.mox.VerifyAll()

    def test_store(self, tmpdir):
        ma = self.mox.CreateMock(OmeroMetadata.MapAnnotations)
        mockfs = self.mox.CreateMock(
            OmeroExternalFeatureStore.FeatureSetFileStore)
        mockfs.store(self.rowmetas, self.valuess) #.AndReturn(None)

        self.mox.ReplayAll()

        ffs = OmeroExternalFeatureStore.FeatureFileStore(tmpdir, 'session')
        self.mox.StubOutWithMock(ffs, 'get_feature_set')
        ffs.get_feature_set(self.fsmeta).AndReturn(mockfs)

        self.mox.ReplayAll()
        ffs.store([self.fsmeta], self.rowmetas, self.valuess)

        self.mox.VerifyAll()

    def test_query(self, tmpdir):
        ma = self.mox.CreateMock(OmeroMetadata.MapAnnotations)
        mockfs = self.mox.CreateMock(
            OmeroExternalFeatureStore.FeatureSetFileStore)
        mockfs.fetch(self.rowmetas[0]) #.AndReturn(self.valuess)

        self.mox.ReplayAll()

        ffs = OmeroExternalFeatureStore.FeatureFileStore(tmpdir, 'session')
        self.mox.StubOutWithMock(ffs, 'get_feature_set')
        ffs.get_feature_set(self.fsmeta).AndReturn(mockfs)

        self.mox.ReplayAll()
        ffs.fetch(self.fsmeta, self.rowmetas[0])

        self.mox.VerifyAll()
