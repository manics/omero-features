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
OMERO.features metadata storage

At present only strings are supported for keys and values
"""

import itertools

import omero
import omero.gateway
from omero.rtypes import wrap, unwrap


class MapAnnotations(object):

    def __init__(self, session, namespace=None):
        self.session = session
        self.namespace = namespace
        if namespace is not None and not isinstance(namespace, str):
            raise Exception('namespace must be a string')

    def create_map_ann(self, kvs):
        d = dict((k, wrap(str(v))) for k, v in kvs.iteritems())
        m = omero.model.MapAnnotationI()
        m.setNs(wrap(self.namespace))
        m.setMapValue(d)
        m = self.session.getUpdateService().saveAndReturnObject(m)
        return unwrap(m.id)

    def query_by_map_ann(self, kvs, projection=False):
        params = omero.sys.ParametersI()

        qs = self.session.getQueryService()
        conditions = []

        if self.namespace is not None:
            conditions.append('ann.ns = :ns')
            params.addString('ns', self.namespace)

        for k, v in kvs.iteritems():
            paramk = 'k%d' % len(conditions)
            paramv = 'v%d' % len(conditions)
            params.addString(paramk, k)

            if isinstance(v, list):
                conditions.append(
                    'ann.mapValue[:%s] in (:%s)' % (paramk, paramv))
            else:
                conditions.append(
                    'ann.mapValue[:%s] = :%s' % (paramk, paramv))
            params.add(paramv, wrap(v))

        # join fetch mapValue is only needed if we need to return the map, the
        # query should run without it
        if projection:
            q = ('select ann.id, index(map), map from MapAnnotation ann '
                 'join ann.mapValue map')
        else:
            q = 'from MapAnnotation ann join fetch ann.mapValue map'

        if conditions:
            q += ' where ' + ' and '.join(conditions)

        if projection:
            # Each [id, key, value] is returned separately, use order by to
            # ensure all keys/values for an annotation are consecutive
            q += ' order by ann.id'
            anns = qs.projection(q, params)

            # iikvs: A map of ids:((id, key, value), ...)
            results = dict(
                (unwrap(iikvs[0]), dict((unwrap(ikv[1]), unwrap(ikv[2]))
                                        for ikv in iikvs[1]))
                for iikvs in itertools.groupby(anns, lambda x: x[0]))
        else:
            print q
            results = qs.findAllByQuery(q, params)

        return results

    @staticmethod
    def type_to_str(x):
        t = type(x)
        if t in [bool, float, int, long, str]:
            return '%s:%s' % (t.__name__, x)
        else:
            raise Exception('Unsupported type: %s' % t)

    @staticmethod
    def type_from_str(s):
        t, x = s.split(':', 1)
        if t == 'bool':
            if x == 'True':
                return True
            if x == 'False':
                return False
            raise Exception('Invalid bool: %s' % x)
        if t == 'float':
            return float(x)
        if t == 'int':
            return int(x)
        if t == 'long':
            return long(x)
        if t == 'str':
            return x
        raise Exception('Unsupported type: %s' % s)

    def create_map_ann_multitype(self, kvs):
        d = dict((k, self.type_to_str(v))
                 for k, v in kvs.iteritems())
        return self.create_map_ann(d)

    def query_by_map_ann_multitype(self, kvs):
        d = dict((k, self.type_to_str(v))
                 for k, v in kvs.iteritems())
        return self.query_by_map_ann(d)

    def create_map_annkw(self, **kwargs):
        return self.create_map_ann(kwargs)

    def query_by_map_annkw(self, **kwargs):
        return self.query_by_map_ann(kwargs)

    def create_map_annkw_multitype(self, **kwargs):
        return self.create_map_ann_multitype(kwargs)

    def query_by_map_annkw_multitype(self, **kwargs):
        return self.query_by_map_ann_multitype(kwargs)


# E.g.
# z=query_by_map_ann(channel1=['0','5'],channel2='3')
# set((y.getMapValue()['channel1'].val,y.getMapValue()['channel2'].val) for y in z)

def print_key_values(map):
    print ' '.join(['%s=%s' % (k, unwrap(map[k])) for k in map])

#for b in a:
#    print_key_values(b)
