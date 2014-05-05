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

import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

import numpy
from itertools import product
from features.storage import Storage

#size = (2, 3, 4)
size = (3, )
filename = 'test.h5'

def test_desc_str():
    d = {'as_ds':'345', 'bb':'^^^\\', '_==c\_':'43'}
    s = Storage.desc_to_str(d)
    sd = Storage.str_to_desc(s)
    assert d == sd

def example_create():
    with Storage(filename, 'w') as s:
        #rowdesc = {'type': str, 'id': int, 'c': int, 'z': int, 't': int}
        rowdesc = [
            ('type', str), ('id', int), ('c', int), ('z', int), ('t', int)
            ]
        featuredesc = {'name': 'Test Featureset', 'version': '0.1.0'}
        s.newFeatureGroup(rowdesc, size, featuredesc)

def example_write():
    with Storage(filename, 'a') as s:
        iczts = product(xrange(4), xrange(3), xrange(2), xrange(2))
        for i, c, z, t in iczts:
            #values = numpy.random.rand(*size)
            values = numpy.array(range(numpy.prod(size))).reshape(size) + i
            s.store1(
                {'type': 'Object', 'id': i, 'c': c, 'z': z, 't': t}, values)

def example_read():
    with Storage(filename, 'r') as s:
        print s.feature_desc()
        # Queries: lists elements are ORed, dict fields are ANDed
        x = s.fetch([{'c': 1, 'id': 0}, {'c': 2, 'id': [1, 2]}])
    return x

def example():
    example_create()
    example_write()
    print example_read()



bigsize = 1000

def big_create():
    with Storage(filename, 'w') as s:
        rowdesc = [
            ('type', str), ('id', int), ('c', int), ('z', int), ('t', int)]
        featuredesc = {'name': 'Test Big Featureset', 'version': '0.1.0'}
        s.newFeatureGroup(rowdesc, bigsize, featuredesc)

def big_write():
    with Storage(filename, 'a') as s:
        iczts = product(xrange(2000), xrange(4), xrange(5), xrange(6))
        for i, c, z, t in iczts:
            #values = numpy.random.rand(*size)
            values = numpy.array(range(numpy.prod(bigsize))).reshape(bigsize) + i
            s.store1(
                {'type': 'Object', 'id': i, 'c': c, 'z': z, 't': t}, values)

def big_create_indices():
    with Storage(filename, 'a') as s:
        s.create_indices()

def big_read1():
    with Storage(filename, 'r') as s:
        print s.feature_desc()
        x = s.fetch([
            {'c': 1, 'id': range(0, 2000, 20)},
            {'c': 2, 'id': range(5, 2000, 20)}
            ])
    return x

def big_read2():
    with Storage(filename, 'r') as s:
        print s.feature_desc()
        x = s.fetch([
            {'c': 1, 'z': [0, 2, 4], 't': [0, 2, 4]},
            {'c': 2, 'z': [1, 3], 't': [1, 3, 5]}
            ])
    return x
