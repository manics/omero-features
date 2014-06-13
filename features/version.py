# -*- coding: utf-8 -*-
# Author: Douglas Creager <dcreager@dcreager.net>
# This file is placed into the public domain.

# Calculates the current version number.  If possible, this is the
# output of “git describe”, modified to conform to the versioning
# scheme that setuptools uses.  If “git describe” returns an error
# (most likely because we're in an unpacked copy of a release tarball,
# rather than in a git working copy), then we fall back on reading the
# contents of the RELEASE-VERSION file.
#
# To use this script, simply import it your setup.py file, and use the
# results of get_git_version() as your package version:
#
# from version import *
#
# setup(
#     version=get_git_version(),
#     .
#     .
#     .
# )
#
# This will automatically update the RELEASE-VERSION file, if
# necessary.  Note that the RELEASE-VERSION file should *not* be
# checked into git; please add it to your top-level .gitignore file.
#
# You'll probably want to distribute the RELEASE-VERSION file in your
# sdist tarballs; to do this, just create a MANIFEST.in file that
# contains the following line:
#
#   include RELEASE-VERSION

__all__ = ("get_git_version")

from features import __file__ as module_file
from subprocess import Popen, PIPE
from os import path, getcwd, chdir
import re


def _lookup_version(module_file):
    """
    For the given module file (usually found by:

        from package import __file__ as module_file

    in the caller, return the location of
    the current RELEASE-VERSION file and the file
    itself.
    """
    version_dir = path.abspath(path.dirname(module_file))
    version_file = path.join(version_dir, "RELEASE-VERSION")
    return version_dir, version_file


def call_git_describe(abbrev=4):
    try:
        p = Popen(['git', 'describe', '--match=[v0-9][.0-9]*',
                   '--abbrev=%d' % abbrev], stdout=PIPE, stderr=PIPE)
        p.stderr.close()
        line = p.stdout.readlines()[0]
        return line.strip()

    except:
        return None


def read_release_version(module_file):
    try:
        version_dir, version_file = _lookup_version(module_file)
        with open(version_file, "r") as f:
            version = f.readlines()[0]
            return version.strip()
    except:
        return None


def write_release_version(module_file, version):
    version_dir, version_file = _lookup_version(module_file)
    with open(version_file, "w") as f:
        f.write("%s\n" % version)

version_pattern = '^(v)?(?P<version>[0-9]+[\.][0-9]+[\.][0-9]+(\-.+)*)$'
version_pattern = re.compile(version_pattern)


def get_git_version(module_file, abbrev=4):
    # Read in the version that's currently in RELEASE-VERSION.
    release_version = read_release_version(module_file)

    # First try to get the current version using “git describe”.
    cwd = getcwd()
    version_dir, version_file = _lookup_version(module_file)
    git_version = None
    try:
        chdir(version_dir)
        git_version = call_git_describe(abbrev)
    finally:
        chdir(cwd)

    # Extract version number
    version = None
    if git_version:
        m = version_pattern.match(git_version)
        if m:
            version = m.group('version')

    # If that doesn't work, fall back on the value that's in
    # RELEASE-VERSION.

    if version is None:
        version = release_version

    # If we still don't have anything, that's an error.

    if version is None:
        import logging
        logger = logging.getLogger("features.version")
        logger.warn((
            "Cannot find the version number! Looking "
            "in %s while in %s"), version_dir, cwd)
        version = "UNKNOWN"

    # If the current version is different from what's in the
    # RELEASE-VERSION file, update the file to be current.

    if version != release_version:
        write_release_version(module_file, version)

    # Finally, return the current version.

    return version


if __name__ == "__main__":
    print get_git_version(module_file)
