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

import struct

from swift.common.utils import fdatasync

PICKLE_PROTOCOL = 2

DATA_VERSION = 1
META_VERSION = 1

OBJECT_START_MARKER = "SWIFTOBJ"
VOLUME_START_MARKER = "SWIFTVOL"

# object alignment within a volume.
# this is needed so that FALLOC_FL_PUNCH_HOLE can actually return space back
# to the filesystem (tested on XFS and ext4)
# we may not need to align files in volumes dedicated to short-lived files,
# such as tombstones (.ts extension),
# but currently we do align for all volume types.
ALIGNMENT = 4096

# constants
STATE_OBJ_FILE = 0
STATE_OBJ_QUARANTINED = 1


class HeaderException(Exception):
    def __init__(self, message):
        self.message = message
        super(HeaderException, self).__init__(message)


class ObjectHeader(object):
    """
    Version 1:
        Magic string (8 bytes)
        Header version (1 byte)
        Policy index (8 bytes)
        Object hash (16 bytes) (__init__)
        Filename (30 chars)
        Metadata offset (8 bytes)
        Metadata size (8 bytes)
        Data offset (8 bytes)
        Data size (8 bytes)
        Total object size (8 bytes)

    Version 2: similar but 64 chars for the filename
    Version 3: Adds a "state" field (unsigned char)
    """
    FORMATS_V1 = {
        'base': '8sBQQQ30sQQQQQ'
    }
    # turns out 30 characters for the filename is not enough in some cases
    FORMATS_V2 = {
        'base': '8sBQQQ64sQQQQQ'
    }

    # Add a state field
    FORMATS_V3 = {
        'base': '8sBQQQ64sQQQQQB'
    }

    # Add metadata checksum
    FORMATS_V4 = {
        'base': '8sBQQQ64sQQQQQB32s'
    }

    def __init__(self, version=4):
        if version not in [1, 2, 3, 4]:
            raise IOError('Unsupported object header version')
        self.magic_string = bytes(OBJECT_START_MARKER)
        self.version = version

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<ObjectHeader ohash: {} filename: {}>".format(self.ohash,
                                                              self.filename)

    @staticmethod
    # def __get_format(version, flags):
    def __get_format(version):
        formats = None
        if version == 1:
            formats = ObjectHeader.FORMATS_V1
        if version == 2:
            formats = ObjectHeader.FORMATS_V2
        if version == 3:
            formats = ObjectHeader.FORMATS_V3
        if version == 4:
            formats = ObjectHeader.FORMATS_V4

        return formats['base']

    def __len__(self):
        fmt = ObjectHeader.__get_format(self.version)
        return struct.calcsize(fmt)

    def pack(self):
        if self.version == 1:
            return self.__pack_v1()
        elif self.version == 2:
            return self.__pack_v2()
        elif self.version == 3:
            return self.__pack_v3()
        elif self.version == 4:
            return self.__pack_v4()
        else:
            raise IOError('Unsupported header version')

    def __pack_v1(self):
        fmt = ObjectHeader.__get_format(1)
        ohash_h = int(self.ohash, 16) >> 64
        ohash_l = int(self.ohash, 16) & 0x0000000000000000ffffffffffffffff

        args = (self.magic_string, self.version,
                self.policy_idx, ohash_h, ohash_l,
                str(self.filename),
                self.metadata_offset, self.metadata_size,
                self.data_offset, self.data_size, self.total_size)

        return struct.pack(fmt, *args)

    def __pack_v2(self):
        fmt = ObjectHeader.__get_format(2)
        ohash_h = int(self.ohash, 16) >> 64
        ohash_l = int(self.ohash, 16) & 0x0000000000000000ffffffffffffffff

        args = (self.magic_string, self.version,
                self.policy_idx, ohash_h, ohash_l,
                str(self.filename),
                self.metadata_offset, self.metadata_size,
                self.data_offset, self.data_size, self.total_size)

        return struct.pack(fmt, *args)

    def __pack_v3(self):
        fmt = ObjectHeader.__get_format(3)
        ohash_h = int(self.ohash, 16) >> 64
        ohash_l = int(self.ohash, 16) & 0x0000000000000000ffffffffffffffff

        args = (self.magic_string, self.version,
                self.policy_idx, ohash_h, ohash_l,
                str(self.filename),
                self.metadata_offset, self.metadata_size,
                self.data_offset, self.data_size, self.total_size, self.state)

        return struct.pack(fmt, *args)

    def __pack_v4(self):
        fmt = ObjectHeader.__get_format(4)
        ohash_h = int(self.ohash, 16) >> 64
        ohash_l = int(self.ohash, 16) & 0x0000000000000000ffffffffffffffff

        args = (self.magic_string, self.version,
                self.policy_idx, ohash_h, ohash_l,
                str(self.filename),
                self.metadata_offset, self.metadata_size,
                self.data_offset, self.data_size, self.total_size, self.state,
                self.metastr_md5)

        return struct.pack(fmt, *args)

    @staticmethod
    def unpack(buf):
        if buf[0:8] != OBJECT_START_MARKER:
            raise HeaderException('Not a header')
        version = struct.unpack('<B', buf[8])[0]
        if version == 1:
            return ObjectHeader.__unpack_v1(buf)
        elif version == 2:
            return ObjectHeader.__unpack_v2(buf)
        elif version == 3:
            return ObjectHeader.__unpack_v3(buf)
        elif version == 4:
            return ObjectHeader.__unpack_v4(buf)
        else:
            raise IOError('Unsupported header version')

    @staticmethod
    def __unpack_v1(buf):
        fmt = ObjectHeader.__get_format(1)
        raw_header = struct.unpack(fmt, buf[0:struct.calcsize(fmt)])
        header = ObjectHeader()
        header.magic_string = raw_header[0]
        header.version = raw_header[1]
        header.policy_idx = raw_header[2]
        header.ohash = "{:032x}".format((raw_header[3] << 64) + raw_header[4])
        header.filename = raw_header[5].rstrip('\0')
        header.metadata_offset = raw_header[6]
        header.metadata_size = raw_header[7]
        header.data_offset = raw_header[8]
        header.data_size = raw_header[9]
        # currently, total_size gets padded to the next 4k boundary, so that
        # fallocate can reclaim the block when hole punching.
        header.total_size = raw_header[10]

        return header

    @staticmethod
    def __unpack_v2(buf):
        fmt = ObjectHeader.__get_format(2)
        raw_header = struct.unpack(fmt, buf[0:struct.calcsize(fmt)])
        header = ObjectHeader()
        header.magic_string = raw_header[0]
        header.version = raw_header[1]
        header.policy_idx = raw_header[2]
        header.ohash = "{:032x}".format((raw_header[3] << 64) + raw_header[4])
        header.filename = raw_header[5].rstrip('\0')
        header.metadata_offset = raw_header[6]
        header.metadata_size = raw_header[7]
        header.data_offset = raw_header[8]
        header.data_size = raw_header[9]
        # currently, total_size gets padded to the next 4k boundary, so that
        # fallocate can reclaim the block when hole punching.
        header.total_size = raw_header[10]

        return header

    @staticmethod
    def __unpack_v3(buf):
        fmt = ObjectHeader.__get_format(3)
        raw_header = struct.unpack(fmt, buf[0:struct.calcsize(fmt)])
        header = ObjectHeader()
        header.magic_string = raw_header[0]
        header.version = raw_header[1]
        header.policy_idx = raw_header[2]
        header.ohash = "{:032x}".format((raw_header[3] << 64) + raw_header[4])
        header.filename = raw_header[5].rstrip('\0')
        header.metadata_offset = raw_header[6]
        header.metadata_size = raw_header[7]
        header.data_offset = raw_header[8]
        header.data_size = raw_header[9]
        # currently, total_size gets padded to the next 4k boundary, so that
        # fallocate can reclaim the block when hole punching.
        header.total_size = raw_header[10]
        header.state = raw_header[11]

        return header

    @staticmethod
    def __unpack_v4(buf):
        fmt = ObjectHeader.__get_format(4)
        raw_header = struct.unpack(fmt, buf[0:struct.calcsize(fmt)])
        header = ObjectHeader()
        header.magic_string = raw_header[0]
        header.version = raw_header[1]
        header.policy_idx = raw_header[2]
        header.ohash = "{:032x}".format((raw_header[3] << 64) + raw_header[4])
        header.filename = raw_header[5].rstrip('\0')
        header.metadata_offset = raw_header[6]
        header.metadata_size = raw_header[7]
        header.data_offset = raw_header[8]
        header.data_size = raw_header[9]
        # currently, total_size gets padded to the next 4k boundary, so that
        # fallocate can reclaim the block when hole punching.
        header.total_size = raw_header[10]
        header.state = raw_header[11]
        header.metastr_md5 = raw_header[12]

        return header


class VolumeHeader(object):
    """
    Version 1:
        Magic string (8 bytes)
        Header version (1 byte)
        Volume index (8 bytes)
        Partition index (8 bytes)
        Volume type (8 bytes)
        First object offset (8 bytes)
        Volume state (4 bytes) (enum from fmgr.proto)
        Volume compaction target (8 bytes) (only valid if state is STATE_COMPACTION_SRC)
    """
    FORMATS_V1 = {
        'base': '8sBQQQQLQ'
    }

    def __init__(self):
        self.magic_string = bytes(VOLUME_START_MARKER)
        self.version = 1
        self.state = 0
        self.compaction_target = 0

    def __str__(self):
        prop_list = ['volume_idx', 'partition', 'type', 'state', 'compaction_target']
        h_str = ""
        for prop in prop_list:
            h_str += "{}: {}\n".format(prop, getattr(self, prop))
        return h_str[:-1]

    @staticmethod
    # def __get_format(version, flags):
    def __get_format(version):
        formats = None
        if version == 1:
            formats = VolumeHeader.FORMATS_V1

            return formats['base']

    def __len__(self):
        fmt = VolumeHeader.__get_format(self.version)
        return struct.calcsize(fmt)

    def pack(self):
        if self.version == 1:
            return self.__pack_v1()
        else:
            raise IOError('Unsupported header version')

    def __pack_v1(self):
        fmt = VolumeHeader.__get_format(1)

        args = (self.magic_string, self.version,
                self.volume_idx, self.partition, self.type,
                self.first_obj_offset, self.state,
                self.compaction_target)

        return struct.pack(fmt, *args)

    @staticmethod
    def unpack(buf):
        if buf[0:8] != VOLUME_START_MARKER:
            raise IOError('Not a header')
        version = struct.unpack('<B', buf[8])[0]
        if version == 1:
            return VolumeHeader.__unpack_v1(buf)
        else:
            raise IOError('Unsupported header version')

    @staticmethod
    def __unpack_v1(buf):
        fmt = VolumeHeader.__get_format(1)
        raw_header = struct.unpack(fmt, buf[0:struct.calcsize(fmt)])
        header = VolumeHeader()
        header.magic_string = raw_header[0]
        header.version = raw_header[1]
        header.volume_idx = raw_header[2]
        header.partition = raw_header[3]
        header.type = raw_header[4]
        header.first_obj_offset = raw_header[5]
        header.state = raw_header[6]
        header.compaction_target = raw_header[7]

        return header


# Read volume header. Expects fp to be positionned at header offset
def read_volume_header(fp):
    buf = fp.read(128)
    header = VolumeHeader.unpack(buf)
    return header


def read_header(fp):
    """
    Read object header
    :param fp: opened file, positioned at header start
    :return: an ObjectHeader
    """
    buf = fp.read(512)
    header = ObjectHeader.unpack(buf)
    return header


def write_header(header, fp, offset):
    """
    Rewrites header in open file, at offset
    :param header: header to write
    :param fp: opened volume
    :param offset: offset
    """
    fp.seek(offset)
    fp.write(header.pack())
    fdatasync(fp.fileno())


