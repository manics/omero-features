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
Blitz wrapper for MapAnnotations
"""

import omero.gateway
from omero.gateway import AnnotationWrapper
from omero.rtypes import wrap, unwrap


class MapAnnotationWrapper(AnnotationWrapper):
    """
    omero_model_MapAnnotation class wrapper extends AnnotationWrapper.
    """

    OMERO_TYPE = omero.model.MapAnnotationI

    def _getQueryString(self):
        """
        Used for building queries in generic methods such as getObjects("MapAnnotation")
        """
        return ("select obj from MapAnnotation obj "
                 "join fetch obj.details.owner as owner "
                 "join fetch obj.details.group "
                 "join fetch obj.details.creationEvent "
                 "join fetch obj.mapValue")

    def getValue (self):
        """
        Gets map value
        """
        return unwrap(self._obj.mapValue)

    def setValue (self, val):
        """
        Sets map value
        """
        self._obj.mapValue = wrap(val)

AnnotationWrapper._register(MapAnnotationWrapper)
omero.gateway.KNOWN_WRAPPERS['mapannotation'] = MapAnnotationWrapper
omero.gateway.refreshWrappers()
