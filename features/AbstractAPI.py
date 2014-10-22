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

from abc import ABCMeta, abstractmethod


class AbstractFeatureRow(object):
    """
    A featureset row

    Each row consists of a list of arrays of doubles
    """

    __metaclass__ = ABCMeta

    def __init__(self, names=None, values=None,
                 infonames=None, infovalues=None):
        self._names = names
        self._values = values
        self._infonames = None
        self._infovalues = None

    @abstractmethod
    def __getitem__(self, key):
        pass

    @abstractmethod
    def __setitem__(self, key, value):
        pass

    @property
    def names(self):
        return self._names

    @property
    def values(self):
        return self._values


class AbstractFeatureStore(object):
    """
    A single feature store

    Each entry in a feature store consists of a FeatureRow
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def store_by_image(self, image_id, values):
        """
        Store a single FeatureRow by Image ID

        :param image_id: The Image ID
        :param values: The feature values
        :return: A FeatureRow
        """
        pass

    @abstractmethod
    def store_by_roi(self, roi_id, values):
        """
        Store a single FeatureRow by Image ID

        :param image_id: The Image ID
        :param values: The feature values
        :return: A FeatureRow
        """
        pass

    @abstractmethod
    def fetch_by_image(self, image_id):
        """
        Retrieve a single FeatureRow by Image ID

        :param image_id: The Image ID
        :return: A FeatureRow
        """
        pass

    @abstractmethod
    def fetch_by_roi(self, roi_id):
        """
        Retrieve a single FeatureRow by ROI ID

        :param roi_id: The ROI ID
        :return: A FeatureRow
        """
        pass

    @abstractmethod
    def fetch_all(self, featureset_name, image_id):
        """
        Retrieve all rows of features identified by Image ID

        :param featureset_name: The featureset identifier
        :param image_id: The Image ID
        :return: A list of FeatureRows
        """
        pass

    @abstractmethod
    def filter(self, conditions):
        """
        Retrieve the features and Image/ROI IDs which fulfill the conditions

        :param conditions: The feature query conditions
        :return: A list of FeatureRows

        TODO: Decide on the query syntax
        """
        pass


class AbstractFeatureStoreManager(object):
    """
    Manages multiple feature stores
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def create(self, featureset_name, names, widths):
        """
        Create a new feature store

        :param featureset_name: The featureset identifier
        :param names: A list of feature names
        :param widths: A list of widths of each feature
        """
        pass

    @abstractmethod
    def get(self, featureset_name):
        """
        Get an existing feature store

        :param featureset_name: The featureset identifier
        :return: An AbstractFeatureStore
        """
        pass
