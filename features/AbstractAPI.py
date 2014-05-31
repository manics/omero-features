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
OMERO.features abstract API
"""

class AbstractFeatureSetStorage(object):
    """
    A single feature set.
    Each element is a fixed width array of doubles
    """

    def store1(self, rowmeta, values):
        raise Exception('Not implemented')

    def fetch(self, rowquery):
        raise Exception('Not implemented')


class AbstractFeatureStorage(object):
    """
    Multiple feature sets.
    """

    def store1(self, rowmeta, fsmeta, values):
        raise Exception('Not implemented')

    def fetch(self, rowmeta, fsmeta):
        raise Exception('Not implemented')
