# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ctypes
import os
import os.path
import pwd
import re

from swift.common.utils import load_libc_function
from swift.obj.fmgr_pb2 import VOLUME_DEFAULT, VOLUME_TOMBSTONE, \
    VOLUME_X_DELETE_AT


fallocate_sys = load_libc_function('fallocate', fail_if_missing=True,
                                   errcheck=True)

# regex to extract policy from path (one KV per policy)
policy_re = re.compile(r'^objects(-\d)?$')


class VFileUtilException(Exception):
    pass


def get_volume_type(extension):
    map = {
        ".ts": VOLUME_TOMBSTONE
    }

    return map.get(extension, VOLUME_DEFAULT)


# used by "fsck" to get the socket path from the volume path
def get_socket_path_from_volume_path(volume_path):
    volume_path = os.path.normpath(volume_path)
    dirname = os.path.dirname
    # the socket is two levels up from the volume :
    # /srv/1/node/sdb1/sofs/volumes/v0001 -> /srv/1/node/sdb1/sofs/rpc.socket
    socket_dir = dirname(dirname(volume_path))
    socket_path = os.path.join(socket_dir, "rpc.socket")
    return socket_path


def get_mountpoint_from_volume_path(volume_path):
    volume_path = os.path.normpath(volume_path)
    dirname = os.path.dirname
    # /srv/1/node/sdb1/sofs/volumes/v0001 -> /srv/1/node/sdb1
    mountpoint = dirname(dirname(dirname(volume_path)))
    return mountpoint


class SwiftPathInfo(object):
    def __init__(self, type, socket_path=None, volume_dir=None,
                 policy_idx=None, partition=None, suffix=None, ohash=None,
                 filename=None):
        self.type = type
        self.socket_path = socket_path
        self.volume_dir = volume_dir
        self.policy_idx = policy_idx
        self.partition = partition
        self.suffix = suffix
        self.ohash = ohash
        self.filename = filename

    # parses a swift path, returns a SwiftPathInfo instance
    @classmethod
    def from_path(cls, path):
        count_to_type = {
            4: 'file',
            3: 'ohash',
            2: 'suffix',
            1: 'partition',
            0: 'partitions'  # "objects" directory
        }

        clean_path = os.path.normpath(path)
        ldir = clean_path.split(os.sep)

        try:
            obj_idx = \
            [i for i, elem in enumerate(ldir) if elem.startswith('objects')][0]
        except ValueError:
            raise VFileUtilException('cannot parse object directory')

        elements = ldir[(obj_idx + 1):]
        count = len(elements)

        if count > 4:
            raise VFileUtilException("cannot parse swift file path")

        try:
            policy_idx = int(ldir[obj_idx].split('-')[1])
        except IndexError:
            policy_idx = 0

        prefix = os.path.join('/', *ldir[0:obj_idx])
        m = policy_re.match(ldir[obj_idx])
        if not m:
            raise VFileUtilException('cannot parse object element of directory')
        if m.group(1):
            sofsdir = "losf{}".format(m.group(1))
        else:
            sofsdir = "losf"
        socket_path = os.path.join(prefix, sofsdir, "rpc.socket")
        volume_dir = None
        volume_dir = os.path.join(prefix, sofsdir, "volumes")

        type = count_to_type[count]
        return cls(type, socket_path, volume_dir, policy_idx, *elements)


def get_volume_index(volume_path):
    """
    returns the volume index, either from its basename, or full path
    """
    name = os.path.split(volume_path)[1]
    index = int(name[1:8])
    return index


# given an offset and alignement, returns the next aligned offset
def next_aligned_offset(offset, alignment):
    if offset % alignment != 0:
        return (offset + (alignment - offset % alignment))
    else:
        return offset


def punch_hole(fd, offset, size):
    """
    Punches a hole in the underlying file, freeing space on the filesystem.
    :param fd: file descriptor
    :param offset: offset at which to punch hole
    :param size: length of hole
    """
    # 3 is FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE
    fallocate_sys(fd, 3, ctypes.c_uint64(offset), ctypes.c_uint64(size))


def change_user(username):
    pw = pwd.getpwnam(username)
    uid = pw.pw_uid
    os.setuid(uid)