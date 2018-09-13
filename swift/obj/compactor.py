import argparse
import logging
import os
import sys
import fcntl
import glob
from os.path import dirname

import rpc_grpc as rpc
import vfile
from header import ALIGNMENT, read_volume_header, read_header
from swift.common import utils
from swift.obj.fmgr_pb2 import STATE_RW, STATE_COMPACTION_SRC, \
    STATE_COMPACTION_TARGET
from vfile_utils import get_socket_path_from_volume_path, get_volume_index, \
    next_aligned_offset, change_user

log_levels = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG
}

# Create a new volume to be used as a compaction target
def create_compaction_target(socket_path, partition, volume_type, volume_dir,
                             conf, logger):
    return vfile.create_writable_volume(socket_path, partition, volume_type,
                                        volume_dir, conf, logger,
                                        state=STATE_COMPACTION_TARGET)


# iterator over a range in a file
def read_range_chunks(f, start, end):
    # TODO: figure out what chunk size to use
    chunk_size = 4096
    f.seek(start)
    while end - f.tell() >= chunk_size:
        yield f.read(chunk_size)
    yield (f.read(end - f.tell()))


# Wip - use that for batching
# Holds state about a vfile being copied
class VfileCopy(object):
    def __init__(self, name, new_offset, new_next_offset):
        self.name = name
        self.new_offset = new_offset
        self.new_next_offset = new_next_offset


def copy_vfile(object, source_file, target_fd):
    """
    Copies a vfile from a source volume to a target volume
    "object" is defined in fmgr.proto.
    source_volume_file is a python file
    target_volume_fd is a file descriptor, and must be at the desired position in the target file
    """
    source_file.seek(object.offset)
    header = read_header(source_file)
    logger.debug(
        "copy_vfile, header name: {} kv name: {}".format(header.filename, object.name))
    # absolute offset of the vfile within the volume
    end_offset = object.offset + header.total_size
    write_offset = next_aligned_offset(os.lseek(target_fd, 0, os.SEEK_CUR),
                                       ALIGNMENT)
    os.lseek(target_fd, write_offset, os.SEEK_SET)
    total_written = 0
    for chunk in read_range_chunks(source_file, object.offset, end_offset):
        # seek to next aligned block
        while chunk:
            written = os.write(target_fd, chunk)
            total_written += written
            chunk = chunk[written:]

    next_offset = next_aligned_offset(write_offset + total_written, ALIGNMENT)

    return write_offset, next_offset


# Updates vfile position in the KV (new volume_idx, offset, next_offset)
def update_vfile_kv(socket_path, name, volume_idx, offset, next_offset):
    # update position in RPC.
    # If there is a power loss after the unregister, it will be fixed after
    # the reboot check. Still, we may want a dedicated RPC call that will
    # atomically update the key. (it could take multiple objects and batch update)
    logger.debug("update RPC for {}".format(name))
    rpc.unregister_object(socket_path, name, 0, 0, 0)
    rpc.register_object(socket_path, name, volume_idx, offset, next_offset)


# Take a lock on the volume lock file
# This is different from what is done in vfile, as it may block until the lock is obtained.
# Returns the fd that should be closed to release the lock.
def lock_file(lock_path):
    lock_fd = os.open(lock_path, os.O_WRONLY)
    fcntl.flock(lock_fd, fcntl.LOCK_EX)
    return lock_fd


# WIP
# todo : add fallocate() to reserve contiguous space for the volume
def compact_volume(source_volume_path, conf, batchsize, logger):
    socket_path = get_socket_path_from_volume_path(source_volume_path)
    source_volume_name = os.path.basename(source_volume_path)
    volume_dir = os.path.dirname(source_volume_path)
    source_volume_index = get_volume_index(source_volume_name)

    # Lock and open source volume
    logger.info("Locking the source volume (may block)")
    lock_path = "{}.writelock".format(source_volume_path)
    lock_fd = lock_file(lock_path)
    logger.info("Obtained lock")

    logger.debug("open source volume {}".format(source_volume_path))
    # review the buffer size, is this useful?
    source_volume_file = open(source_volume_path, "rb", 4096)
    source_vh = read_volume_header(source_volume_file)

    source_volume_info = rpc.get_volume(socket_path, source_volume_index)
    if source_volume_info.volume_state != STATE_RW and source_volume_info.volume_state != STATE_COMPACTION_SRC:
        logger.warn("Volume state is not RW or COMPACTION_SRC, exiting")
        sys.exit(0)

    # Create target volume, unless we are resuming a compaction
    if source_volume_info.volume_state == STATE_RW:
        partition = source_volume_info.partition
        volume_type = source_volume_info.volume_type

        # It would be good to reserve the whole space for the volume now. (fallocate)
        # However that information is currently not available in the KV.
        # Maybe add space accounting when creating/deleting an object ?
        logger.info("creating new target volume, partition: {}".format(partition))
        target_volume_fd, target_lock_fd, target_volume_path = create_compaction_target(
            socket_path, partition, volume_type,
            volume_dir, conf, logger)
        target_volume_index = get_volume_index(target_volume_path)
        logger.info("created target volume {}".format(target_volume_path))
        logger.debug("target_volume_fd: {}".format(target_lock_fd))
        logger.debug("type: {}".format(type(target_volume_fd)))

        # Change volume state in header and KV
        logger.info("Update KV state for source volume to STATE_COMPACTION_SRC")
        vfile.change_volume_state(source_volume_path, STATE_COMPACTION_SRC, compaction_target=target_volume_index)
        rpc.update_volume_state(socket_path, source_volume_index,
                                STATE_COMPACTION_SRC)
    else:
        # resuming a compaction
        volume_dir = dirname(source_volume_path)
        target_volume_index = source_vh.compaction_target
        target_name = vfile.get_volume_name(target_volume_index)
        target_volume_path = os.path.join(volume_dir, target_name)
        logger.info("open existing target volume: {}".format(target_volume_path))
        target_volume_fd, target_lock_fd = vfile.open_volume(target_volume_path)
        target_volume_info = rpc.get_volume(socket_path, target_volume_index)
        os.lseek(target_volume_fd, target_volume_info.next_offset, os.SEEK_SET)

    # Iterate over objects in volume and copy to KV
    # currently this only goes over vfiles. we need to handle quarantined file (same rpc call ?)
    objects = rpc.list_objects_by_volume(socket_path, source_volume_index)

    # Copy objects to the new volume. the target volume is fsynced after each object copy,
    # before updating the KV.
    # "objects" is an iterator over "Object", as defined in fmgr.proto
    # This would be worth optimizing, we could copy a bunch of file before syncing and registering them
    # to the KV.
    no_error = True
    # holds objects that have been copied, but not yet fsynced
    # keep objects in a list, not a dict, so that the RPC is updated in the same order as they've been written
    # to the target volume: keys will already be sorted in the KV, and resuming a compaction after a crash is easier.
    batch_objects = []
    # current batch size
    batch_bytes = 0
    for object in objects:
        # copy object to new
        logger.debug("copy object {}".format(object.name))
        try:
            new_offset, new_next_offset = copy_vfile(object, source_volume_file,
                                                     target_volume_fd)
            batch_bytes += new_next_offset - new_offset
            batch_objects.append({'name': object.name, 'new_offset': new_offset, 'new_next_offset': new_next_offset})

            if batch_bytes > batchsize:
                # sync and update the KV for this batch
                logger.debug("sync batch")
                os.fsync(target_volume_fd)
                update_objects_kv(socket_path, target_volume_index, batch_objects)
                batch_bytes = 0
                batch_objects = []

        except Exception as e:
            no_error = False
            logger.warn("failed to copy: {}".format(object.name))
            logger.exception(e)

    # sync and update last batch
    os.fsync(target_volume_fd)
    update_objects_kv(socket_path, target_volume_index, batch_objects)

    # Remove source volume
    if no_error:
        logger.info("delete source volume {}".format(source_volume_path))
        vfile.delete_volume(socket_path, source_volume_path, logger)
    else:
        # if we failed ot copy any object, do not remove the volume, and leave it the "COMPACTION_SRC" state
        # another tool or an option will be needed to handle the case (delete volume from kv)
        logger.warn("Errors while copying some objects, not deleting source volume {}".format(source_volume_path))
        logger.warn("Review errors before removing the volume")

    logger.info(
        "change target volume state to STATE_RW ({})".format(target_volume_path))
    vfile.change_volume_state(target_volume_path, STATE_RW)
    rpc.update_volume_state(socket_path, target_volume_index, STATE_RW)


def update_objects_kv(socket_path, target_volume_index, batch_objects):
    for object in batch_objects:
        update_vfile_kv(socket_path, object['name'], target_volume_index, object['new_offset'], object['new_next_offset'])


# TODO: check how it's done in swift CLI tools
def get_conf(file, section='app:object-server'):
    defaults = {
        'volume_alloc_chunk_size': 16 * 1024,
        'volume_low_free_space': 4 * 1024,
        'metadata_reserve': 700,
        'max_volume_count': 100,
        'max_volume_size': 10 * 1024 * 1024 * 1024
    }

    # configparser expects string
    for k, v in defaults.iteritems():
        defaults[k] = str(v)

    conf = utils.readconf(file, section, defaults=defaults)

    # vfile expects int..
    for k in defaults.keys():
        conf[k] = int(conf[k])
    return conf


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_path", default="/etc/swift/object-server.conf",
                        help="path to object-server configuration file")
    parser.add_argument("--log_level", help="logging level, defaults to info")
    parser.add_argument("--volume", help="path to volume")
    parser.add_argument("--volume_topdir", help="volume top level directory")
    parser.add_argument("--compact", action="store_true", help="compact volume")
    parser.add_argument("--state_rw", action="store_true",
                        help="change volume state to RW")
    parser.add_argument("--keepuser", type=bool, default=False, help="Do not attempt to switch to swift user")
    parser.add_argument("--batchsize", type=int, default=10*1024*1024, help="max amount of bytes to copy before fsync, default 10MB")
    args = parser.parse_args()

    log_level = "info"
    if args.log_level:
        log_level = args.log_level

    logger = logging.getLogger(__name__)
    logger.setLevel(log_levels[log_level])
    handler = logging.handlers.SysLogHandler(address = '/dev/log')
    formatter = logging.Formatter('%(module)s.%(funcName)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if not args.keepuser:
        change_user("swift")

    if (not args.volume and not args.volume_topdir) or (args.volume and
                                                        args.volume_topdir):
        parser.print_help()
        sys.exit(0)

    if args.compact:
        # read object-server configuration
        conf = get_conf(args.config_path)

        if args.volume:
            compact_volume(args.volume, conf, args.batchsize, logger)

        if args.volume_topdir:
            lock_pattern = "{}/*.writelock"
            for lock_path in glob.iglob(lock_pattern.format(args.volume_topdir)):
                volume_path = lock_path.replace(".writelock", "")
                compact_volume(volume_path, conf, args.batchsize, logger)

    # used for debugging
    if args.state_rw:
        vfile.change_volume_state(args.volume, STATE_RW)
        socket_path = get_socket_path_from_volume_path(args.volume)
        volume_index = get_volume_index(args.volume)
        rpc.update_volume_state(socket_path, volume_index, STATE_RW)
