#!/usr/bin/env python
# -*- coding: utf-8 -*-


import datetime
import glob
import itertools
import numpy as np
import os
import re

import features
import features.OmeroTablesFeatureStore


combinefs = True
dryrun = True

files = glob.glob('/Users/simon/machine_learning/standalone-pychrm-ns-p23153/SmallFeatureSet/*/*npz')

def loadnp(f, create=False):
    d = np.load(f)
    if d['version'] != '2.1':
        raise Exception('Unexpected version: %s' % d['version'])
    names = d['names']
    values = d['values']
    version = d['version'].item()
    if len(names) != 1059:
        raise Exception('Expected 1059 feature names, found: %d' % len(names))
    if len(values) != 1059:
        raise Exception('Expected 1059 feature names, found: %d' % len(values))

    fre = re.search(
        '-p(?P<projectid>[\d+]+)/'
        '(?P<fsname>[^/]+)/[^/]+/'
        'image(?P<imageid>\d+)-c(?P<c>\d+)-z(?P<z>\d+)-t(?P<t>\d+).npz$',
        f).groupdict()
    fsname = fre.pop('fsname')
    sample = dict((k, int(v)) for k, v in fre.iteritems())
    timestamp = datetime.datetime.utcfromtimestamp(
        os.stat(f).st_mtime).isoformat()
    sample['datetime-calculated'] = timestamp

    fss = split_into_featuresets(names, values)
    if combinefs:
        store_featuresets_combined(
            fss, version, sample, timestamp, fsname, create)
    else:
        store_featuresets(fss, version, sample, timestamp, create)


def parse_feature_name(name):
    """
    Convert a single value feature name in the form
    'ABC ... [NN]'
    to a feature group name and size in the form
    'ABC ...', NN
    """
    ft, idx = name.split(' [')
    idx = int(idx[:-1])
    return ft, idx


def create_feature_name(ft, idx):
    """
    The inverse of parse_feature_name
    """
    name = '%s [%d]' % (ft, idx)
    return name


def split_into_featuresets(names, values):
    featuresets = {}
    nivs = (list(parse_feature_name(n)) + [v] for n, v in itertools.izip(
        names, values))
    for n, niv in itertools.groupby(nivs, lambda x: x[0]):
        ns, idxs, vals = zip(*niv)
        if idxs != tuple(xrange(0, len(idxs))):
            raise Exception(
                'Expected indices for %s to be consecutive' % n)
        featuresets[n] = vals
    return featuresets


def store_featuresets_combined(
        featuresets, version, sample, timestamp, fsname, create=False):
    names = sorted(featuresets.keys())
    values = tuple(featuresets[name] for name in names)
    colmetas = zip(names, [len(v) for v in values])

    fsmeta = {
        'name': fsname,
        'version': version,
    }
    rowmeta = sample

    print
    print 'Feature dict: %s' % fsmeta
    print 'Sample dict: %s ' % sample
    print 'Feature values: %s...' % str(values)[:100]

    if dryrun:
        return

    if create:
        col_desc = [(float, n, v) for n, v in colmetas]
        store.create_feature_set(fsmeta, col_desc)
    store.store(fsmeta, [rowmeta], [(values)])


def store_featuresets(featuresets, version, sample, timestamp, create=False):
    for name, values in featuresets.iteritems():
        fsmeta = {
            'name': name,
            'version': version,
        }
        rowmeta = sample

        print
        print 'Feature dict: %s' % fsmeta
        print 'Sample dict: %s ' % sample
        print 'Feature values: %s...' % str(values)[:100]

        if dryrun:
            continue

        if create:
            col_desc = [(float, name, len(values))]
            store.create_feature_set(fsmeta, col_desc)
        store.store(fsmeta, [rowmeta], [(values,)])


def list_samples(q=None, projection=True):
    if not q:
        q = {}
    ma = features.OmeroMetadata.MapAnnotations(
        client.getSession(), store.row_space)
    return ma.query_by_map_ann(q, projection=projection)


def list_featuresets(q=None, projection=True):
    if not q:
        q = {}
    ma = features.OmeroMetadata.MapAnnotations(
        client.getSession(), store.column_space)
    return ma.query_by_map_ann(q, projection=projection)


def delete_feature_table(tableid):
    #def delete(otype, oids):
        #h = conn.deleteObjects('OriginalFile', oids)
        #print 'Deleting %s: %s' % (otype, oids)
        #try:
        #    conn._waitOnCmd(h)
        #    for r in h.getResponse().responses:
        #        print 'Scheduled: %d Actual: %d' % (
        #            r.scheduledDeletes, r.actualDeletes)
        #except omero.CmdError as e:
        #    print 'omero.CmdError: %s' % e
        #finally:
        #    h.close()
    rowanns = list_samples({'_tableid': str(tableid)}, False)
    colanns = list_featuresets({'_tableid': str(tableid)}, False)
    conn = omero.gateway.BlitzGateway(client_obj=client)

    #delete('MapAnnotation', rowanns.keys() + colanns.keys())
    #delete('OriginalFile', [tableid])
    #dcs = []
    #dcs.append(omero.cmd.Delete('/OriginalFile', tableid))
    #for aid in rowanns.keys() + colanns.keys():
    #    dcs.append(omero.cmd.Delete('/Annotation', aid))
    #doall = omero.cmd.DoAll()
    #doall.requests = dcs
    #h = client.sf.submit(doall)
    #try:
    #    conn._waitOnCmd(h)
    #    for r in h.getResponse().responses:
    #        print 'Scheduled: %d Actual: %d' % (
    #            r.scheduledDeletes, r.actualDeletes)
    #except omero.CmdError as e:
    #    print h.getResponse()

    for obj in rowanns + colanns + [omero.model.OriginalFileI(tableid)]:
        print 'Deleting: %s %s' % (obj.ice_id(), obj.getId().val)
        conn.deleteObjectDirect(obj)


def init_store():
    store = features.OmeroTablesFeatureStore.FeatureTableStore(
        client.getSession(), namespace='test-20140627', cachesize=40)
    return store
