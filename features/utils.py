#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Copyright (C) 2014 University of Dundee & Open Microscopy Environment.
# All rights reserved.
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

"""
OMERO.features utility methods
"""

import omero
from omero.rtypes import rdouble, rint, rstring, unwrap


def create_roi_for_plane(session, iid, z, c, t):
    """
    Create a ROI consisting of an entire single plane

    :param session: An active session
    :param iid: Image ID
    :param z: Z index
    :param c: C index
    :param t: T index
    :return: The new ROI
    """
    qs = session.getQueryService()
    us = session.getUpdateService()
    p = omero.sys.ParametersI()
    p.addId(iid)
    im = qs.findByQuery('FROM Image i join fetch i.pixels where i.id=:id', p)
    assert im
    px = im.getPrimaryPixels()

    rect = omero.model.RectI()
    rect.setX(rdouble(0))
    rect.setY(rdouble(0))
    rect.setWidth(rdouble(px.getSizeX().val))
    rect.setHeight(rdouble(px.getSizeY().val))
    rect.setTheZ(rint(z))
    rect.setTheC(rint(c))
    rect.setTheT(rint(t))

    roi = omero.model.RoiI()
    roi.addShape(rect)
    roi.setImage(im)

    roi = us.saveAndReturnObject(roi)
    return roi


def find_rois_for_plane(session, iid=None, z=None, c=None, t=None,
                        chname=None, shapetype=None, fullplane=True,
                        singleshape=True, projection=False,
                        load_shapes=False, load_image=False, load_pixels=False,
                        load_channels=False):
    """
    Query ROIs by properties, for instance to find all ROIs for a given plane

    :param session: An active session
    :param iid: Image ID
    :param z: Z index
    :param c: C index
    :param t: T index
    :param chname: Channel name
    :param shapetype: The ROI Shape type
    :param fullplane: If True ROI must be a Rect covering an entire image plane
    :param singleshape: If True ROI must only contain one shape, if False any
        ROI containing at least one shape that matches the other criteria will
        be returned
    :param projection: If True return just the ROI ids, otherwise return the
        ROI objects
    :return: The new ROI

    """
    qs = session.getQueryService()
    params = omero.sys.ParametersI()

    if projection:
        q = 'SELECT r.id FROM Roi r'
    else:
        q = 'FROM Roi r'
    conds = []

    if iid is not None:
        load_image = True
        conds.append('i.id=:iid')
        params.addLong('iid', iid)

    if z is not None:
        load_shapes = True
        conds.append('s.theZ=:z')
        params.add('z', rint(z))
    if c is not None:
        load_shapes = True
        conds.append('s.theC=:c')
        params.add('c', rint(c))
    if t is not None:
        load_shapes = True
        conds.append('s.theT=:t')
        params.add('t', rint(c))

    if chname:
        load_channels = True
        conds.append('lc.name=:lc')
        params.add('lc', rstring(chname))

    if shapetype:
        load_shapes = True
        conds.append('s.class=%s' % shapetype)

    if fullplane:
        load_shapes = True
        load_pixels = True
        conds.extend(['s.x=0', 's.y=0', 's.width=p.sizeX', 's.height=p.sizeY',
                      's.class=Rect'])

    if singleshape:
        conds.append(
            '(r.id, 1) IN '
            '(SELECT r.id, size(r.shapes) FROM Roi r GROUP BY r.id)')

    if load_shapes:
        if projection:
            q += ', Shape s'
            conds.append('s.roi=r')
        else:
            q += ' JOIN FETCH r.shapes s'
    if load_image or load_pixels or load_channels:
        if projection:
            q += ', Image i'
            conds.append('r.image=i')
        else:
            q += ' JOIN FETCH r.image i'
    if load_pixels or load_channels:
        if projection:
            q += ', Pixels p'
            conds.append('p.image=i')
        else:
            q += ' JOIN FETCH i.pixels p'
    if load_channels:
        if projection:
            q += ', Channel c, LogicalChannel lc'
            conds.extend(['c.pixels=p', 'c.logicalChannel=lc'])
        else:
            q += ' JOIN FETCH p.channels c JOIN FETCH c.logicalChannel lc'

    if conds:
        q += ' WHERE ' + ' AND '.join(conds)

    print q
    if projection:
        rois = qs.projection(q, params)
        if rois:
            rois = [r[0] for r in unwrap(rois)]
    else:
        rois = qs.findAllByQuery(q, params)
    return rois
