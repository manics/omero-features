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

import omero
from omero.rtypes import rstring, rlong, wrap

from features import OmeroMetadata


class MockUpdateService:
    def saveAndReturnObject(self, o):
        pass


class MockQueryService:
    def findAllByQuery(self, q, p):
        pass


class MockSession:
    def __init__(self):
        self.us = MockUpdateService()
        self.qs = MockQueryService()

    def getUpdateService(self):
        return self.us

    def getQueryService(self):
        return self.qs


class TestMapAnnotations(object):

    def setup_method(self, method):
        self.mox = mox.Mox()

    def teardown_method(self, method):
        self.mox.UnsetStubs()

    @pytest.mark.parametrize('ns', [None, 'namespace'])
    def test_create_map_ann(self, ns):
        sess = MockSession()
        self.mox.StubOutWithMock(sess.us, 'saveAndReturnObject')

        map = {'a': rstring('1'), 'bb': rstring('cc')}

        rid = 2
        r = omero.model.MapAnnotationI()
        if ns:
            r.setNs(rstring(ns))
        r.setMapValue(map)
        r.setId(rlong(rid))

        sess.us.saveAndReturnObject(mox.Func(
            lambda o: o.getNs() == wrap(ns) and
            o.getMapValue() == wrap(map).val)).AndReturn(r)

        self.mox.ReplayAll()

        ma = OmeroMetadata.MapAnnotations(sess, namespace=ns)
        assert ma.create_map_ann(a='1', bb='cc') == rid
        self.mox.VerifyAll()

    @pytest.mark.parametrize('ns', [None, 'namespace'])
    def test_query_by_map_ann(self, ns):
        sess = MockSession()
        self.mox.StubOutWithMock(sess.qs, 'findAllByQuery')

        if ns is not None:
            query = (
                'from MapAnnotation ann join fetch ann.mapValue map where '
                'ann.ns = :ns '
                'and ann.mapValue[:k1] = :v1 and ann.mapValue[:k2] in (:v2)')
            params = {'ns': ns,
                      'k1': 'a', 'v1': '1', 'k2': 'bb', 'v2': ['cc', 'dd']}
        else:
            query = (
                'from MapAnnotation ann join fetch ann.mapValue map where '
                'ann.mapValue[:k0] = :v0 and ann.mapValue[:k1] in (:v1)')
            params = {'k0': 'a', 'v0': '1', 'k1': 'bb', 'v1': ['cc', 'dd']}
        params = omero.sys.ParametersI(wrap(params).val)

        r1 = omero.model.MapAnnotationI()
        r2 = omero.model.MapAnnotationI()
        if ns:
            r1.setNs(rstring(ns))
            r2.setNs(rstring(ns))
        r1.setMapValue({'a': rstring('1'), 'bb': rstring('cc')})
        r2.setMapValue({'a': rstring('1'), 'bb': rstring('dd')})

        sess.qs.findAllByQuery(query, mox.Func(
            lambda o: params.map == o.map)).AndReturn([r1, r2])

        self.mox.ReplayAll()

        ma = OmeroMetadata.MapAnnotations(sess, namespace=ns)
        assert ma.query_by_map_ann(a='1', bb=['cc', 'dd']) == [r1, r2]
        self.mox.VerifyAll()

    def test_type_to_str(self):
        ma = OmeroMetadata.MapAnnotations
        assert ma.type_to_str(True) == 'bool:True'
        assert ma.type_to_str(False) == 'bool:False'
        assert ma.type_to_str(1.2) == 'float:1.2'
        assert ma.type_to_str(-2) == 'int:-2'
        assert ma.type_to_str(4L) == 'long:4'
        assert ma.type_to_str('aa') == 'str:aa'

        with pytest.raises(Exception):
            ma.type_to_str(object())

    def test_type_from_str(self):
        ma = OmeroMetadata.MapAnnotations
        assert ma.type_from_str('bool:True') is True
        assert ma.type_from_str('bool:False') is False
        assert ma.type_from_str('float:1.2') == 1.2
        assert ma.type_from_str('int:-2') == -2
        assert ma.type_from_str('long:4') == 4L
        assert ma.type_from_str('str:aa') == 'aa'

        with pytest.raises(Exception):
            ma.type_from_str('xxx')

    def test_create_map_ann_multitype(self):
        ma = OmeroMetadata.MapAnnotations(None)
        self.mox.StubOutWithMock(ma, 'create_map_ann')

        r = 3
        ma.create_map_ann(a='int:1', bb='str:cc').AndReturn(r)

        self.mox.ReplayAll()

        assert ma.create_map_ann_multitype(a=1, bb='cc') == r
        self.mox.VerifyAll()

    def test_query_by_map_ann_multitype(self):
        ma = OmeroMetadata.MapAnnotations(None)
        self.mox.StubOutWithMock(ma, 'query_by_map_ann')

        r = [omero.model.MapAnnotationI()]
        ma.query_by_map_ann(a='int:1', bb='str:cc').AndReturn(r)

        self.mox.ReplayAll()

        assert ma.query_by_map_ann_multitype(a=1, bb='cc') == r
        self.mox.VerifyAll()
