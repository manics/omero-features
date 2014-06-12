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
Use OMERO for metadata, but store the feature files externally
"""

from AbstractAPI import AbstractFeatureSetStorage, AbstractFeatureStorage
import OmeroMetadata


import itertools
import numpy
import os


def prefix_fields(prefix, d):
    return dict((prefix + k, v) for k, v in d.iteritems())


class FileStoreMapper(object):
    """
    Converts feature-set annotations into feature file-paths
    """

    def __init__(self, rootdir):
        self.rootdir = rootdir
        self.ext = '.npy'

    def get(self, fsann, rowann):
        fspath = os.path.join(self.rootdir, '%08d' % fsann.id)
        fname = '%08d%s' % (rowann.id, self.ext)
        p = os.path.join(fspath, fname)
        if not os.path.isdir(fspath):
            os.mkdir(fspath)
        return p

    def delete(self, fsann, rowann):
        p = self.get(fsann, rowann)
        os.unlink(p)

    def load(self, fsann, rowann):
        p = self.get(fsann, rowann)
        return numpy.load(p)

    def save(self, fsann, rowann, values):
        # TODO: Check format/shape of values
        p = self.get(fsann, rowann)
        numpy.save(values)


class FeatureSetFileStore(AbstractFeatureSetStorage):
    """
    A single feature set.
    Each element is a fixed width array of doubles
    """

    def __init__(self, ma, fsmeta):
        self.ma = ma
        self.fsmeta = prefix_fields('f:', fsmeta)

    def store1(self, rowmeta, values):
        self.ma.create_map_ann(dict(
            self.fsmeta.items() + [
                ('r:' + k, v) for k, v in rowmeta.iteritems()]))

    def store(self, rowmetas, values):
        for (rowmeta, value) in itertools.izip(rowmetas, values):
            self.store1(rowmeta, value)

    def fetch(self, rowquery):
        self.ma.query_by_map_ann(dict(
            self.fsmeta.items() + [
                ('r:' + k, v) for k, v in rowquery.iteritems()]))


class FeatureFileStore(AbstractFeatureStorage):
    """
    Manage local storage of feature files
    """

    def __init__(self, rootdir, session, **kwargs):
        self.rootdir = rootdir
        self.session = session
        self.namespace = kwargs.get('namespace', 'omero.features')
        self.ma = OmeroMetadata.MapAnnotations(session, self.namespace)
        self.fss = {}

    def get_feature_set(self, fsmeta):
        fskey = tuple(sorted(fsmeta.iteritems()))
        if fskey not in self.fss:
            self.fss[fskey] = FeatureSetFileStore(fsmeta)
        return self.fss[fskey]

    def store(self, fsmetas, rowmetas, values):
        for fsmeta in fsmetas:
            fs = self.get_feature_set(fsmeta)
            fs.store(rowmetas, values)

    def fetch(self, fsquery, rowquery):
        fs = self.get_feature_set(fsquery)
        fs.fetch(rowquery)
