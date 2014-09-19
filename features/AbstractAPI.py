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


class FeatureRow(object):
    """
    A featureset row

    Each row consists of a list of arrays of doubles
    """

    def __init__(self, widths=None, names=None, values=None):
        assert widths or values

        self._widths = widths
        self._names = None
        self._values = None
        if values:
            self.values = values
        if names:
            assert len(names) == len(self._widths)
        self._names = names
        self._namemap = {}

    def get_index(self, name):
        try:
            return self._namemap[name]
        except KeyError:
            self._namemap = dict(
                ni for ni in zip(self._names, xrange(len(self._names))))
            return self._namemap[name]

    def __getitem__(self, key):
        return self.values[self.get_index(key)]

    def __setitem__(self, key, value):
        i = self.get_index(key)
        assert len(value) == self._widths[i]
        self.values[i] = value

    @property
    def names(self):
        return self._names

    @property
    def widths(self):
        return self._widths

    @property
    def values(self):
        return self._values

    @values.setter
    def values(self, value):
        if self._names:
            assert len(self.names) == len(self._values)
        widths = [len(v) for v in value]
        if self._widths:
            assert self._widths == widths
        else:
            self._widths = widths
        self._values = value

    @values.deleter
    def values(self):
        del self._values

    def __repr__(self):
        return '%s(widths=%r, names=%r, values=%r)' % (
            self.__class__.__name__, self._widths, self._names, self._values)





class AbstractFeatureStorageManager(object):
    """
    Manages multiple feature stores

    Each entry in a feature store consists of a FeatureRow
    """

    def create(self, featureset_name, names, widths):
        """
        Create a new feature store

        :param featureset_name: The featureset identifier
        :param names: A list of feature names
        :param widths: A list of widths of each feature
        """
        raise Exception('Not implemented')

    def store(self, featureset_name, image_id, roi_id, values):
        """
        Store a row of features identified by Image ID and/or ROI ID

        :param featureset_name: The featureset identifier
        :param image_id: The Image ID
        :param roi_id: The ROI ID, may be None
        :params values: A list of FeatureRows
        """
        raise Exception('Not implemented')

    def fetch_by_image(self, featureset_name, image_id):
        """
        Retrieve a single FeatureRow by Image ID

        :param featureset_name: The featureset identifier
        :param image_id: The Image ID
        :return: A FeatureRow
        """
        raise Exception('Not implemented')

    def fetch_by_roi(self, featureset_name, roi_id):
        """
        Retrieve a single FeatureRow by ROI ID

        :param featureset_name: The featureset identifier
        :param roi_id: The ROI ID
        :return: A FeatureRow
        """
        raise Exception('Not implemented')

    def fetch_all(self, featureset_name, image_id):
        """
        Retrieve all rows of features identified by Image ID

        :param featureset_name: The featureset identifier
        :param image_id: The Image ID
        :return: A list of FeatureRows
        """
        raise Exception('Not implemented')


    def filter(self, featureset_name, conditions):
        """
        Retrieve the features and Image/ROI IDs which fulfill the conditions

        :param featureset_name: The featureset identifier
        :param conditions: The feature query conditions
        :return: A list of (Image-ID, ROI-ID, FeatureRow) triplets
        """
        raise Exception('Not implemented')
