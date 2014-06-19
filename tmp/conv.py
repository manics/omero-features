#!/usr/bin/env python
# -*- coding: utf-8 -*-


import datetime
import itertools
import numpy as np
import os
import re


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

    fre = re.match(
        'image(?P<imageid>\d+)-c(?P<c>\d+)-z(?P<z>\d+)-t(?P<t>\d+).npz$',
        os.path.basename(f)).groupdict()
    sample = dict((k, int(v)) for k, v in fre.iteritems())
    timestamp = datetime.datetime.utcfromtimestamp(
        os.stat(f).st_mtime).isoformat()
    sample['datetime-calculated'] = timestamp

    fss = split_into_featuresets(names, values)
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


def store_featuresets(featuresets, version, sample, timestamp, create):
    for name, values in featuresets.iteritems():
        fsmeta = {
            'name': name,
            'version': version,
            #'datetime-calculated': timestamp
        }

        print
        print 'Feature dict: %s' % fsmeta
        print 'Sample dict: %s ' % sample
        print 'Feature values: %s' % str(values)

        if create == 'individual':
            col_desc = [double, name, len(values)]
            # store.


    store.store(fsmeta, sample, [values])

def init_store():
    store = features.OmeroTablesFeatureStore.FeatureTableStore(
        client.getSession(), namespace='test-20140618')
    return store
