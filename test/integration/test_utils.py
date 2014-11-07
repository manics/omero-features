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

from integration_test_lib import UserAccount

import pytest

import numpy

import omero
import omero.gateway
from omero.rtypes import rdouble, unwrap

from features import utils


class TestUtils(object):

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

    def create_image(self):
        szx, szy, szz, szc, szt = 4, 5, 6, 7, 8

        def planegen():
            for count in xrange(szz * szc * szt):
                yield numpy.ones((szy, szx), numpy.uint16) * count

        conn = omero.gateway.BlitzGateway(client_obj=self.cli)
        im = conn.createImageFromNumpySeq(planegen(), __name__, szz, szc, szt)
        return im._obj

    @pytest.mark.parametrize('robject', [True, False])
    def test_create_roi_for_plane(self, robject):
        im = self.create_image()

        r = utils.create_roi_for_plane(
            self.sess, unwrap(im.getId()), 2, 4, 6, robject=robject)
        if not robject:
            p = omero.sys.ParametersI()
            p.addId(r)
            r = self.sess.getQueryService().findByQuery(
                'FROM Roi r JOIN FETCH r.shapes where r.id=:id', p)

        assert r.getImage().getId() == im.getId()
        assert r.sizeOfShapes() == 1

        s = r.getShape(0)
        assert unwrap(s.getX()) == 0
        assert unwrap(s.getY()) == 0
        assert unwrap(s.getWidth()) == 4
        assert unwrap(s.getHeight()) == 5
        assert unwrap(s.getTheZ()) == 2
        assert unwrap(s.getTheC()) == 4
        assert unwrap(s.getTheT()) == 6

    @pytest.mark.parametrize('projection', [True, False])
    def test_find_rois_for_plane(self, projection):
        def getIds(objs, robject=True):
            if robject:
                return sorted(unwrap(o.getId()) for o in objs)
            return sorted(objs)

        im = self.create_image()
        iid = unwrap(im.getId())

        r1 = utils.create_roi_for_plane(
            self.sess, unwrap(im.getId()), 2, 3, 4, robject=True)
        r2 = utils.create_roi_for_plane(
            self.sess, unwrap(im.getId()), 2, 4, 6, robject=True)

        r3 = utils.create_roi_for_plane(
            self.sess, unwrap(im.getId()), 2, 4, 8, robject=True)
        r3.getShape(0).setWidth(rdouble(1))
        r3 = self.sess.getUpdateService().saveAndReturnObject(r3)

        r4 = utils.create_roi_for_plane(
            self.sess, unwrap(im.getId()), 2, 4, 8, robject=True)
        r4.addShape(omero.model.RectI())
        r4 = self.sess.getUpdateService().saveAndReturnObject(r4)

        rs = utils.find_rois_for_plane(self.sess, iid=iid, z=1000)
        assert rs == []

        rs = utils.find_rois_for_plane(
            self.sess, iid=iid, fullplane=True, projection=projection)
        assert getIds(rs, not projection) == getIds([r1, r2])

        rs = utils.find_rois_for_plane(
            self.sess, iid=iid, singleshape=False, projection=projection)
        assert getIds(rs, not projection) == getIds([r1, r2, r4])

        rs = utils.find_rois_for_plane(
            self.sess, iid=iid, fullplane=False, projection=projection)
        assert getIds(rs, not projection) == getIds([r1, r2, r3])

        rs = utils.find_rois_for_plane(
            self.sess, iid=iid, c=4, fullplane=False, singleshape=False,
            projection=projection)
        assert getIds(rs, not projection) == getIds([r2, r3, r4])
