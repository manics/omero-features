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
OMERO.features
"""

import re
import tables


class Storage(object):
    def __init__(self, filename, mode):
        self.filename = filename
        self.fh = tables.openFile(filename, mode)
        self.tablename = 'features'
        self.valuefield = '_features'
        self.stringwidth = 256

    def newFeatureGroup(self, rowdesc, size, featuredesc):
        for k, v in featuredesc.iteritems():
            assert self.checkname(k)
            assert isinstance(v, str)
        definition = self.createDefinition(rowdesc, size)

        title = self.desc_to_str(featuredesc)
        self.fh.createTable(where='/', name=self.tablename, title=title,
                            description=definition)

    def store1(self, rowdesc, values):
        t = self.fh.getNode('/' + self.tablename)
        assert set(t.colnames) == set(rowdesc.keys() + [self.valuefield])
        cols = [rowdesc[k] for k in t.colnames[:-1]]
        cols.append(values)
        t.append([cols])
        # t.close()

    def fetch(self, rowdesc):
        t = self.fh.getNode('/' + self.tablename)
        q = self.build_query(rowdesc, t.colnames)
        print 'Query:', q
        if not q:
            return t.read()
        # pytables 2 vs 3?
        # return t.read_where(q)
        return t.readWhere(q)
        # t.close()

    def feature_desc(self):
        t = self.fh.getNode('/' + self.tablename)
        return self.str_to_desc(t.title)

    def create_indices(self):
        t = self.fh.getNode('/' + self.tablename)
        for name in t.colnames:
            if name != self.valuefield:
                col = getattr(t.cols, name)
                col.removeIndex()
                # What type of index?
                # http://www.pytables.org/moin/HintsForSQLUsers#Creatinganindex
                # col.create_index()
                col.createCSIndex()

    @staticmethod
    def build_query(query, colnames=None):
        def subquery(k, val):
            t = type(val)
            if t in [int, long, float]:
                return '(%s==%d)' % (k, val)
            elif t in [str, unicode]:
                return '(%s==u"%s")' % (k, val)
            elif issubclass(t, list):
                return '(%s)' % ' | ' .join(subquery(k, v) for v in val)
            else:
                raise Exception('Unexpected type: %s' % t)

        t = type(query)
        if issubclass(t, list):
            return '(%s)' % ' | ' .join(Storage.build_query(q) for q in query)
        elif issubclass(t, dict):
            if colnames:
                assert all(k in colnames for k in query)
            return '(%s)' % ' & ' .join(
                subquery(k, v) for k, v in query.iteritems())
        else:
            raise Exception('Unexpected type: %s' % t)

    @staticmethod
    def checkname(s):
        return re.match('[A-Za-z][A-Za-z0-9_\.]*$', s) is not None

    @staticmethod
    def desc_to_str(d):
        def esc(s):
            return s.replace('_', '\\_').replace('=', '\\=')
        s = '_'.join('%s=%s' % (esc(k), esc(d[k])) for k in sorted(d.keys()))
        return s

    @staticmethod
    def str_to_desc(s):
        def split_and_unesc(c, s):
            tokens = []
            p = 0
            for m in re.finditer(r'[^\\]%c' % c, s):
                tokens.append(s[p:m.start() + 1].replace('\\%c' % c, c))
                p = m.end()
            tokens.append(s[p:].replace('\\%c' % c, c))
            return tokens

        splits = split_and_unesc('_', s)
        d = dict(split_and_unesc('=', kv) for kv in splits)
        return d

    def createDefinition(self, rowdesc, size):
        typemap = {
            int: tables.IntCol, float: tables.FloatCol, str: tables.StringCol
            }
        definition = {}
        n = 0
        # for k, v in rowdesc.iteritems():
        for k, v in rowdesc:
            Storage.checkname(k)
            ctype = typemap[v]
            if ctype == tables.StringCol:
                definition[k] = ctype(pos=n, itemsize=self.stringwidth)
            else:
                definition[k] = ctype(pos=n)
            n += 1
        definition[self.valuefield] = tables.FloatCol(shape=size, pos=n)
        return definition

    def close(self):
        self.fh.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
