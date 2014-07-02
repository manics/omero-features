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

import omero
from omero.rtypes import wrap, unwrap

from features import OmeroMetadata


class TestMapAnnotations(object):

    def setup_class(self):
        self.ua = UserAccount()
        self.user = self.ua.new_user()

    def teardown_class(self):
        self.ua.close()

    def setup_method(self, method):
        self.cli = omero.client()
        un = unwrap(self.user.getOmeName())
        self.sess = self.cli.createSession(un, un)

    def teardown_method(self, method):
        self.cli.closeSession()

    @staticmethod
    def assert_equal_any_order(a, b):
        assert ((a[0] == b[0] and a[1] == b[1]) or
                (a[0] == b[1] and a[1] == b[0]))

    def test_create_map_ann(self):
        ns = UserAccount.uuid()
        ma = OmeroMetadata.MapAnnotations(self.sess, namespace=ns)
        d = {'a': '1', 'bb': 'cc'}
        aid = ma.create_map_ann({'a': '1', 'bb': 'cc'})

        params = omero.sys.ParametersI()
        params.addLong('id', aid)
        mr = self.sess.getQueryService().findByQuery(
            'from MapAnnotation ann join fetch ann.mapValue where id=:id',
            params)

        assert unwrap(mr.getNs()) == ns
        assert unwrap(mr.getMapValue()) == d

    @pytest.mark.parametrize('projection', [True, False, ('bb',)])
    def test_query_by_map_ann(self, projection):
        ns = UserAccount.uuid()
        ma = OmeroMetadata.MapAnnotations(self.sess, namespace=ns)

        d1 = {'a': '1', 'bb': 'cc'}
        d1f = {'bb': 'cc'}
        d2 = {'a': '1', 'bb': 'd'}
        d2f = {'bb': 'd'}

        m1 = omero.model.MapAnnotationI()
        m1.setNs(wrap(ns))
        m1.setMapValue(wrap(d1).val)
        m2 = omero.model.MapAnnotationI()
        m2.setNs(wrap(ns))
        m2.setMapValue(wrap(d2).val)

        m1 = self.sess.getUpdateService().saveAndReturnObject(m1)
        m2 = self.sess.getUpdateService().saveAndReturnObject(m2)

        rs = ma.query_by_map_ann(d1, projection)
        assert len(rs) == 1
        if projection:
            if projection is True:
                assert rs.values()[0] == d1
            else:
                assert rs.values()[0] == d1f
        else:
            assert unwrap(rs[0].getNs()) == ns
            assert unwrap(rs[0].getMapValue()) == d1

        rs = ma.query_by_map_ann({'a': '1'}, projection)
        assert len(rs) == 2
        if projection:
            rmv1, rmv2 = rs.values()
            if projection is True:
                self.assert_equal_any_order((rmv1, rmv2), (d1, d2))
            else:
                self.assert_equal_any_order((rmv1, rmv2), (d1f, d2f))
        else:
            assert unwrap(rs[0].getNs()) == ns
            assert unwrap(rs[1].getNs()) == ns
            rmv1 = unwrap(rs[0].getMapValue())
            rmv2 = unwrap(rs[1].getMapValue())
            self.assert_equal_any_order((rmv1, rmv2), (d1, d2))
