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
Utilities for reading/writing image planes and features
"""

import argparse
import errno
import getpass
import logging
import os
from PIL import Image

import omero
import omero.gateway

log = logging.getLogger('ExportImagePlanes')
log.setLevel(logging.INFO)


class Connection(object):

    def __init__(self, host=None, port=None, user=None, password=None,
                 sessionid=None, client=None, groupid=-1, detach=False):
        if not host:
            host = 'localhost'
        if not port:
            port = 4064

        self.client = client
        try:
            self.session = client.getSession()
            log.info('Using existing session: %s', self.session)
        except (AttributeError, omero.ClientError):
            self.session = None
        if not self.session and not sessionid:
            if not user:
                user = raw_input('User: ')
            if not password:
                password = getpass.getpass()

        if not self.client:
            log.debug('Creating client')
            self.client = omero.client(host, port)

        if not self.session:
            if sessionid:
                self.session = self.client.joinSession(sessionid)
                log.info('Joined session as: %s', self.session)
            else:
                self.session = self.client.createSession(user, password)
                log.info('Created session: %s', self.session)
                self.client.enableKeepAlive(60)
        self.conn = omero.gateway.BlitzGateway(client_obj=self.client)
        self.conn.SERVICE_OPTS.setOmeroGroup(groupid)
        self.detach = detach
        if self.detach:
            log.info('Detaching session: %s', self.session)
            self.session.detachOnDestroy()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        if not self.detach:
            log.info('Closing session: %s', self.session)
            self.client.closeSession()

    def imageGenerator(self, objs):
        for o in objs:
            if isinstance(o, omero.gateway._ImageWrapper):
                yield o
            else:
                children = o.listChildren()
                for im in self.imageGenerator(children):
                    yield im

    def objectGenerator(self, typeids):
        for (t, i) in typeids:
            o = self.conn.getObject(t, i)
            if not o:
                raise Exception('Unable to get object: %s %d' % (t, i))
            yield o


def list_unique_image_ids(imgen):
    iids = set()
    for im in imgen:
        iids.add(im.id)
    return sorted(iids)


def get_output_filename(basedir, iid, z, c, t, ext, args):
    """
    Creates a filename for the output plane file, creates parent directories
    if necessary
    """
    imdir = os.path.join(basedir, 'image-%08d' % iid)
    try:
        if not args.dry_run:
            os.makedirs(imdir)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(imdir):
            raise

    filename = os.path.join(
        imdir, 'image-%08d-z%04d-c%04d-t%04d.%s' % (iid, z, c, t, ext))
    return filename


def writeplanes(client, im, outdir, args):
    """
    Save individual image planes as single tif files
    """
    if not im:
        raise Exception('Image is None')
    iid = im.id

    zcts = [(z, c, t) for t in xrange(im.getSizeT())
            for c in xrange(im.getSizeC()) for z in xrange(im.getSizeZ())]
    n = 0
    pixels = im.getPrimaryPixels()
    for plane in pixels.getPlanes(zcts):
        z, c, t = zcts[n]
        filename = get_output_filename(outdir, iid, z, c, t, 'tif', args)
        log.debug('Saving Image:%d z:%d c:%d t:%d (%dx%d) to %s',
                  iid, z, c, t, im.getSizeX(), im.getSizeY(), filename)
        if not args.dry_run:
            pim = Image.fromarray(plane)
            pim.save(filename, 'TIFF')
        n += 1


# If run from omero shell we may already have a global client variable
try:
    omero_client = client
except NameError:
    omero_client = None


def main(args):
    logging.basicConfig(format='%(asctime)s %(levelname)-5.5s %(message)s')

    log.info(args)

    with Connection(args.server, args.port, args.user, password=args.password,
                    client=omero_client, detach=False) as c:
        objects = c.objectGenerator(args.ordered_arguments)
        for im in c.imageGenerator(objects):
            log.info('Image: %d', im.id)
            writeplanes(c.client, im, args.output_dir, args)


def parse_args(args=None):
    class StoreOrdered(argparse.Action):
        """
        Based on http://stackoverflow.com/a/9028031
        """
        ORDERED_DEST = 'ordered_arguments'

        def __call__(self, parser, namespace, values, option_string=None):
            v = getattr(namespace, self.ORDERED_DEST, [])
            v.append((self.dest, values))
            setattr(namespace, self.ORDERED_DEST, v)

    parser = argparse.ArgumentParser()

    parser.add_argument('--server', default=None, help='Server hostname')
    parser.add_argument('--port', default=None, type=int, help='Server port')
    parser.add_argument('--user', default=None, help='Username')
    parser.add_argument('--password', default=None, help='Password')
    parser.add_argument('--group', default=None, type=int, help='Group ID')

    parser.add_argument('-n', '--dry-run', action='store_true', help='Dry run')
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Verbose logging')

    parser.add_argument(
        '-o', '--output-dir', default='.', help='Output directory')

    parser.add_argument('-p', action=StoreOrdered, dest='Project', type=int)
    parser.add_argument('-d', action=StoreOrdered, dest='Dataset', type=int)
    parser.add_argument('-i', action=StoreOrdered, dest='Image', type=int)
    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)
    main(args)
