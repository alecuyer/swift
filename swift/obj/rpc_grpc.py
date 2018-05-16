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

import grpc
import fmgr_pb2
import logging

log = logging.getLogger(__name__)


# log.addHandler(logging.NullHandler())

# Holds RPC connections by disk-policy
class Connections(object):
    connections = dict()


class Connection(object):
    def __init__(self, channel, stub):
        self.channel = channel
        self.stub = stub

    @classmethod
    def create(cls, socket_path):
        # log.debug("Creating new RPC connection")
        channel = grpc.insecure_channel("unix:{}".format(socket_path))
        stub = fmgr_pb2.FileMgrStub(channel)
        # channel.subscribe(channel_cb, try_to_connect=True)
        return cls(channel, stub)


connections = Connections()


def rpc(func):
    """
    Decorator to setup the rpc channel and stub if they don't exist
    """

    def func_wrapper(socket_path, *args, **kwargs):
        if socket_path not in connections.connections:
            connections.connections[socket_path] = Connection.create(
                socket_path)

        # retry twice if the KV is unavailable.
        for i in range(3):
            try:
                return func(socket_path, *args, **kwargs)
            except grpc.RpcError as e:
                # That should be logged but eventlet will hang often if you do
                # log.debug("Failed to connect to: {}".format(socket_path))

                # If the error is anything but "unavailable", raise immediately
                if e.code() != grpc.StatusCode.UNAVAILABLE:
                    raise(e)

        if e:
            # log.debug("Got multiple Unavailable Rendezvous exceptions, giving up ({})".format(socket_path))
            raise (e)

    return func_wrapper


@rpc
def get_next_offset(socket_path, datafile_index, repair_tool=False):
    """
    Returns the next offset to use in the datafile
    """
    datafile = fmgr_pb2.GetNextOffsetInfo(datafile_index=int(datafile_index), repair_tool=repair_tool)
    response = connections.connections[socket_path].stub.GetNextOffset(datafile)
    return response.offset


@rpc
def register_volume(socket_path, partition, datafile_type, datafile_index,
                    first_obj_offset, state, repair_tool=False):
    datafile = fmgr_pb2.NewDataFileInfo(partition=int(partition),
                                        type=int(datafile_type),
                                        datafile_index=int(datafile_index),
                                        offset=first_obj_offset, state=state, repair_tool=repair_tool)
    response = connections.connections[socket_path].stub.RegisterDataFile(
        datafile)
    return response


@rpc
def unregister_volume(socket_path, datafile_index):
    index = fmgr_pb2.DataFileIndex(index=datafile_index)
    response = connections.connections[socket_path].stub.UnregisterDataFile(
        index)
    return response


@rpc
def update_volume_state(socket_path, datafile_index, new_state, repair_tool=False):
    state_update = fmgr_pb2.NewDataFileState(datafile_index=int(datafile_index),
                                             state=new_state, repair_tool=repair_tool)

    response = connections.connections[socket_path].stub.UpdateDataFileState(
        state_update)
    return response


@rpc
def register_object(socket_path, name, datafile_index, offset, next_offset, repair_tool=False):
    """
    register a vfile
    """
    obj = fmgr_pb2.NewObjectInfo(name=str(name), datafile_index=int(datafile_index),
                                 offset=int(offset),
                                 next_offset=int(next_offset),
                                 repair_tool=repair_tool)
    response = connections.connections[socket_path].stub.RegisterObject(obj)
    return response


@rpc
def unregister_object(socket_path, name, datafile_index, offset, length):
    obj = fmgr_pb2.UnregisterObjectInfo(
        name=str(name),
        datafile_index=int(datafile_index),
        offset=int(offset),
        length=int(length))
    response = connections.connections[socket_path].stub.UnregisterObject(obj)
    return response


@rpc
def rename_object(socket_path, name, new_name, repair_tool=False):
    rename_info = fmgr_pb2.RenameInfo(name=str(name), new_name=str(new_name), repair_tool=repair_tool)
    response = connections.connections[socket_path].stub.RenameObject(
        rename_info)
    return response


@rpc
def quarantine_object(socket_path, name, repair_tool=False):
    objname = fmgr_pb2.ObjectName(name=str(name), repair_tool=repair_tool)
    connections.connections[socket_path].stub.QuarantineObject(objname)


@rpc
def unquarantine_object(socket_path, name, repair_tool=False):
    objname = fmgr_pb2.ObjectName(name=str(name), repair_tool=repair_tool)
    connections.connections[socket_path].stub.UnquarantineObject(objname)


@rpc
def list_quarantined_ohashes(socket_path):
    ohashes = []
    from_entry = ""
    while True:
        info = fmgr_pb2.ListQuarantinedOHashesInfo(from_entry=from_entry)
        response = connections.connections[socket_path].stub.ListQuarantinedOHashes(info)
        for entry in response.entry:
            ohashes.append(entry)
        # stop if we got all entries
        if response.last_entry == "":
            break
        else:
            from_entry = response.last_entry
    return response

@rpc
def list_quarantined_ohash(socket_path, prefix, repair_tool=False):
    len_prefix = len(prefix)
    prefix = fmgr_pb2.ObjectPrefix(prefix=prefix, repair_tool=repair_tool)
    response = connections.connections[socket_path].stub.ListQuarantinedOHash(prefix)

    # Caller expects object names without the prefix, similar
    # to os.listdir, not actual objects.
    objnames = []
    for obj in response.objects:
        objnames.append(obj.name[len_prefix:])

    return objnames

@rpc
def list_partition(socket_path, partition, partition_bits):
    list_partition_info = fmgr_pb2.ListPartitionInfo(partition=partition,
                                                     partition_bits=partition_bits)
    response = connections.connections[socket_path].stub.ListPartition(
        list_partition_info)
    return response.entry


# listdir like function for the KV
@rpc
def list_prefix(socket_path, prefix, repair_tool=False):
    len_prefix = len(prefix)
    prefix = str(prefix)
    prefix = fmgr_pb2.ObjectPrefix(prefix=prefix, repair_tool=repair_tool)
    response = connections.connections[socket_path].stub.LoadObjectsByPrefix(
        prefix)
    # response.objets is an iterable
    # TBD, caller expects object names without the prefix, similar
    # to os.listdir, not actual objects.
    # Fix this in the rpc server
    # return response.objects
    objnames = []
    for obj in response.objects:
        objnames.append(obj.name[len_prefix:])

    return objnames


@rpc
def list_objects_by_volume(socket_path, volume_index, repair_tool=False):
    index = fmgr_pb2.DataFileIndex(index=volume_index, repair_tool=repair_tool)
    response = connections.connections[socket_path].stub.LoadObjectsByDataFile(
        index)
    return response


@rpc
def get_object(socket_path, name, is_quarantined=False, repair_tool=False):
    """
    returns an object given its whole key
    """
    object_name = fmgr_pb2.LoadObjectInfo(name=str(name),
                                          is_quarantined=is_quarantined,
                                          repair_tool=repair_tool)
    response = connections.connections[socket_path].stub.LoadObject(object_name)
    return response


@rpc
def list_partitions(socket_path, partition_bits):
    list_partitions_info = fmgr_pb2.ListPartitionsInfo(
        partition_bits=partition_bits)
    response = connections.connections[socket_path].stub.ListPartitions(
        list_partitions_info)
    return response.entry


@rpc
def list_partition(socket_path, partition, partition_bits):
    list_partition_info = fmgr_pb2.ListPartitionInfo(partition=partition,
                                                     partition_bits=partition_bits)
    response = connections.connections[socket_path].stub.ListPartition(
        list_partition_info)
    return response.entry

@rpc
def list_partition_recursive(socket_path, partition, partition_bits):
    list_partition_info = fmgr_pb2.ListPartitionInfo(partition=int(partition),
                                                     partition_bits=partition_bits)
    # It would be nice to use a streaming call here, but that hangs with eventlet
    response = connections.connections[socket_path].stub.ListPartitionRecursive(
        list_partition_info)
    return response.file_entries

@rpc
def list_suffix(socket_path, partition, suffix, partition_bits):
    # vreview : Fix this, a caller is passing unicode in replicator.py, hard to find because of tpool_reraise
    suffix = str(suffix)
    list_suffix_info = fmgr_pb2.ListSuffixInfo(partition=partition,
                                               suffix=suffix,
                                               partition_bits=partition_bits)
    response = connections.connections[socket_path].stub.ListSuffix(
        list_suffix_info)
    return response.entry


# WIP: list volumes
@rpc
def list_volumes(socket_path, partition, type, repair_tool=False):
    list_info = fmgr_pb2.ListDataFilesInfo(partition=int(partition), type=type, repair_tool=repair_tool)
    response = connections.connections[socket_path].stub.ListDataFiles(
        list_info)
    return response.datafiles


# get volume by index
@rpc
def get_volume(socket_path, index, repair_tool=False):
    dfIndex = fmgr_pb2.DataFileIndex(index=index, repair_tool=repair_tool)
    response = connections.connections[socket_path].stub.GetDataFile(dfIndex)
    return response


@rpc
def get_stats(socket_path):
    stats_info = fmgr_pb2.GetStatsInfo()
    response = connections.connections[socket_path].stub.GetStats(stats_info)
    return response.stats

@rpc
def set_kv_state(socket_path, isClean):
    newKvState = fmgr_pb2.KvState(isClean=isClean)
    response = connections.connections[socket_path].stub.SetKvState(newKvState)
    return response

@rpc
def get_kv_state(socket_path):
    empty = fmgr_pb2.Empty()
    response = connections.connections[socket_path].stub.GetKvState(empty)
    return response

def channel_cb(connectivity):
    # log.debug("Channel connectivity changed: {}".format(connectivity))
    pass
