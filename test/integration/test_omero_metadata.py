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
import uuid

import omero
from omero.rtypes import rstring, wrap, unwrap

from features import OmeroMetadata


class UserAccount(object):
    """
    Copied from components/tools/OmeroPy/test/integration/library.py
    """

    def __init__(self):
        self.root = omero.client()
        self.root.setAgent(__name__)
        rootpass = self.root.getProperty('omero.rootpass')
        self.root.createSession('root', rootpass)

    def close(self):
        self.root.closeSession()

    @staticmethod
    def uuid():
        return str(uuid.uuid4())

    def new_user(self, group=None, perms=None,
                 admin=False, system=False):
        """
        admin: If user is to be an admin of the created group
        system: If user is to be a system admin
        """

        if not self.root:
            raise Exception('No root client. Cannot create user')

        adminService = self.root.getSession().getAdminService()
        name = self.uuid()

        # Create group if necessary
        if not group:
            g = self.new_group(perms=perms)
            group = g.name.val
        else:
            g, group = self.group_and_name(group)

        # Create user
        e = omero.model.ExperimenterI()
        e.omeName = rstring(name)
        e.firstName = rstring(name)
        e.lastName = rstring(name)
        uid = adminService.createUser(e, group)
        e = adminService.lookupExperimenter(name)
        if admin:
            adminService.setGroupOwner(g, e)
        if system:
            adminService.addGroups(e,
                                   [omero.model.ExperimenterGroupI(0, False)])

        return adminService.getExperimenter(uid)

    def new_group(self, experimenters=None, perms=None):
        admin = self.root.sf.getAdminService()
        gname = self.uuid()
        group = omero.model.ExperimenterGroupI()
        group.name = rstring(gname)
        if perms:
            group.details.permissions = omero.model.PermissionsI(perms)
        gid = admin.createGroup(group)
        group = admin.getGroup(gid)
        self.add_experimenters(group, experimenters)
        return group

    def add_experimenters(self, group, experimenters):
        admin = self.root.sf.getAdminService()
        if experimenters:
            for exp in experimenters:
                user, name = self.user_and_name(exp)
                admin.addGroups(user, [group])


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

    @pytest.mark.parametrize('projection', [True, False])
    def test_query_by_map_ann(self, projection):
        ns = UserAccount.uuid()
        ma = OmeroMetadata.MapAnnotations(self.sess, namespace=ns)

        d1 = {'a': '1', 'bb': 'cc'}
        d2 = {'a': '1', 'bb': 'd'}

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
            assert rs.values()[0] == d1
        else:
            assert unwrap(rs[0].getNs()) == ns
            assert unwrap(rs[0].getMapValue()) == d1

        rs = ma.query_by_map_ann({'a': '1'}, projection)
        assert len(rs) == 2
        if projection:
            rmv1, rmv2 = rs.values()
        else:
            assert unwrap(rs[0].getNs()) == ns
            assert unwrap(rs[1].getNs()) == ns
            rmv1 = unwrap(rs[0].getMapValue())
            rmv2 = unwrap(rs[1].getMapValue())
        assert (rmv1 == d1 and rmv2 == d2) or (rmv1 == d2 and rmv2 == d1)
