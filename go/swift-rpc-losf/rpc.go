// Copyright (c) 2010-2012 OpenStack Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// RPC functions
//
// TODO: The naming of things is not consistent with the python code.

package main

import (
	"bytes"
	"fmt"
	pb "github.com/openstack/swift-rpc-losf/proto"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/alexcesaro/statsd.v2"
	"net"
	"os"
	"path"
	"sync"
	"time"
)

type server struct {
	kv         KV
	grpcServer *grpc.Server

	// DB state (is it in sync with the volumes state)
	isClean bool

	diskPath   string // full path to mountpoint
	diskName   string // without the path
	socketPath string // full path to the socket

	// statsd used as is done in swift
	statsd_c *statsd.Client

	// channel to signal server should stop
	stopChan chan os.Signal
}

// The following consts are used as a key prefix for different types in the KV

// DataFile should be called "Volume" to avoid confusion with swift's .data files
const datafilePrefix = 'd'

// prefix for "objects" ("vfile" in the python code, would be a POSIX file on a regular backend)
const objectPrefix = 'o'

// This is meant to be used when a new file is created with the same name as an existing file.
const deleteQueuePrefix = 'q'

// Quarantined objects
const quarantinePrefix = 'r'

// stats stored in the KV
const statsPrefix = 's'

// max key length in ascii format.
const maxObjKeyLen = 96

// RegisterDataFile registers a new datafile (volume) to the KV, given its index number and starting offset.
// Will return an error if the datafile index already exists.
func (s *server) RegisterDataFile(ctx context.Context, in *pb.NewDataFileInfo) (*pb.NewDataFileReply, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "RegisterDatafile", "Partition": in.Partition, "Type": in.Type,
		"DatafileIndex": in.DatafileIndex, "Offset": in.Offset, "State": in.State})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeDataFileKey(in.DatafileIndex)

	// Does the datafile already exist ?
	value, err := s.kv.Get(datafilePrefix, key)
	if err != nil {
		reqlog.Error("unable to check for existing datafile key")
		s.statsd_c.Increment("register_datafile.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to check for existing datafile key")
	}

	if value != nil {
		reqlog.Info("datafile index already exists in db")
		s.statsd_c.Increment("register_datafile.ok")
		return nil, status.Errorf(codes.AlreadyExists, "datafile index already exists in db")
	}

	// Register the datafile
	usedSpace := int64(0)
	value = EncodeDataFileValue(int64(in.Partition), int32(in.Type), int64(in.Offset), usedSpace, int64(in.State))

	err = s.kv.Put(datafilePrefix, key, value)
	if err != nil {
		reqlog.Error("failed to Put new datafile entry")
		s.statsd_c.Increment("register_datafile.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to register new datafile")
	}
	s.statsd_c.Increment("register_datafile.ok")

	return &pb.NewDataFileReply{}, nil
}

// UnregisterDataFile will delete a datafile entry from the kv.
func (s *server) UnregisterDataFile(ctx context.Context, in *pb.DataFileIndex) (*pb.Empty, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "UnregisterDataFile", "DatafileIndex": in.Index})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeDataFileKey(in.Index)

	// Check for key
	value, err := s.kv.Get(datafilePrefix, key)
	if err != nil {
		reqlog.Error("unable to check for datafile key")
		s.statsd_c.Increment("unregister_datafile.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to check for datafile key")
	}

	if value == nil {
		reqlog.Info("datafile index does not exist in db")
		s.statsd_c.Increment("unregister_datafile.ok")
		return nil, status.Errorf(codes.NotFound, "datafile index does not exist in db")
	}

	// Key exists, delete it
	err = s.kv.Delete(datafilePrefix, key)
	if err != nil {
		reqlog.Error("failed to Delete datafile entry")
		s.statsd_c.Increment("unregister_datafile.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to delete datafile entry")
	}

	s.statsd_c.Increment("unregister_datafile.ok")
	return &pb.Empty{}, nil
}

// UpdateDataFileState will modify an existing datafile state
func (s *server) UpdateDataFileState(ctx context.Context, in *pb.NewDataFileState) (*pb.Empty, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "UpdateDataFileState", "DatafileIndex": in.DatafileIndex, "State": in.State})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeDataFileKey(in.DatafileIndex)
	value, err := s.kv.Get(datafilePrefix, key)
	if err != nil {
		reqlog.Error("unable to retrieve datafile key")
		s.statsd_c.Increment("update_datafile_state.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve datafile key")
	}

	if value == nil {
		reqlog.Info("datafile index does not exist in db")
		s.statsd_c.Increment("update_datafile_state.ok")
		return nil, status.Errorf(codes.NotFound, "datafile index does not exist in db")
	}

	partition, dfType, offset, usedSpace, state, err := DecodeDataFileValue(value)
	reqlog.WithFields(logrus.Fields{"current_state": state}).Info("updating state")
	if err != nil {
		reqlog.Error("failed to decode DataFile value")
		s.statsd_c.Increment("update_datafile_state.fail")
		return nil, status.Errorf(codes.Internal, "failed to decode DataFile value")
	}

	value = EncodeDataFileValue(partition, dfType, offset, usedSpace, int64(in.State))
	err = s.kv.Put(datafilePrefix, key, value)
	if err != nil {
		reqlog.Error("failed to Put updated datafile entry")
		s.statsd_c.Increment("update_datafile_state.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to update datafile state")
	}
	s.statsd_c.Increment("update_datafile_state.ok")

	return &pb.Empty{}, nil
}

// GetDataFile will return a datafile information
func (s *server) GetDataFile(ctx context.Context, in *pb.DataFileIndex) (*pb.DataFile, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "GetDataFile", "Volume index": in.Index})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeDataFileKey(in.Index)
	value, err := s.kv.Get(datafilePrefix, key)
	if err != nil {
		reqlog.Errorf("Failed to get datafile key in KV: %s", err)
		s.statsd_c.Increment("get_datafile.fail")
		return nil, status.Errorf(codes.Internal, "Failed to get datafile key in KV")
	}

	if value == nil {
		reqlog.Info("No such DataFile")
		s.statsd_c.Increment("get_datafile.ok")
		return nil, status.Errorf(codes.NotFound, "No such DataFile")
	}

	partition, dfType, nextOffset, _, state, err := DecodeDataFileValue(value)
	if err != nil {
		reqlog.Error("Failed to decode DataFile value")
		s.statsd_c.Increment("get_datafile.fail")
		return nil, status.Errorf(codes.Internal, "Failed to decode DataFile value")
	}

	s.statsd_c.Increment("get_datafile.ok")
	return &pb.DataFile{DatafileIndex: in.Index, DatafileType: uint32(dfType), DatafileState: uint32(state),
		Partition: uint32(partition), NextOffset: uint64(nextOffset)}, nil
}

// ListDataFiles will return all datafiles of the given type, for the given partition.
// Currently this scans all volumes in the KV. Likely fast enough as long as the KV is cached.
// If it becomes a performance issue, we may want to add an in-memory cache indexed by partition.
func (s *server) ListDataFiles(ctx context.Context, in *pb.ListDataFilesInfo) (*pb.DataFiles, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "ListDataFiles", "Partition": in.Partition, "Type": in.Type})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	response := &pb.DataFiles{}

	// Iterate over datafiles and return the ones that match the request
	it := s.kv.NewIterator(datafilePrefix)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		idx, err := DecodeDataFileKey(it.Key())
		if err != nil {
			reqlog.Error("failed to decode datafile key")
			s.statsd_c.Increment("list_datafiles.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode datafile value")
		}

		partition, dfType, nextOffset, _, state, err := DecodeDataFileValue(it.Value())
		if err != nil {
			reqlog.Error("failed to decode datafile value")
			s.statsd_c.Increment("list_datafiles.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode datafile value")
		}
		if uint32(partition) == in.Partition && pb.DataFileType(dfType) == in.Type {
			response.Datafiles = append(response.Datafiles, &pb.DataFile{DatafileIndex: idx,
				DatafileType: uint32(in.Type), DatafileState: uint32(state),
				Partition: uint32(partition), NextOffset: uint64(nextOffset)})
		}
	}

	s.statsd_c.Increment("list_datafiles.ok")
	return response, nil
}

// RegisterObject registers a new object to the kv.
func (s *server) RegisterObject(ctx context.Context, in *pb.NewObjectInfo) (*pb.NewObjectReply, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "RegisterObject",
		"Name":          fmt.Sprintf("%s", in.Name),
		"DiskPath":      s.diskPath,
		"DatafileIndex": in.DatafileIndex,
		"Offset":        in.Offset,
		"NextOffset":    in.NextOffset,
		"Length":        in.NextOffset - in.Offset, // debug
	})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	// Check if datafile exists
	dataFileKey := EncodeDataFileKey(in.DatafileIndex)
	dataFileValue, err := s.kv.Get(datafilePrefix, dataFileKey)
	if err != nil {
		reqlog.Error("unable to check for existing datafile key")
		s.statsd_c.Increment("register_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to check for existing datafile key")
	}

	if dataFileValue == nil {
		reqlog.Info("datafile index does not exist in db")
		s.statsd_c.Increment("register_object.ok")
		return nil, status.Errorf(codes.FailedPrecondition, "datafile index does not exist in db")
	}

	partition, dataFileType, _, currentUsedSpace, state, err := DecodeDataFileValue(dataFileValue)

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Error("unable to encode object key")
		s.statsd_c.Increment("register_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	objectValue := EncodeObjectValue(in.DatafileIndex, in.Offset)

	// If an object exists with the same name, we need to move it to the delete queue before overwriting the key.
	// On the regular file backend, this would happen automatically with the rename operation. In our case,
	// we would leak space. (The space will be reclaimed on compaction, but it shouldn't happen).

	var objMutex = &sync.Mutex{}
	objMutex.Lock()

	existingValue, err := s.kv.Get(objectPrefix, objectKey)
	if err != nil {
		reqlog.Error("unable to check for existing object")
		s.statsd_c.Increment("register_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve object")
	}

	if existingValue != nil {
		reqlog.Info("Object exists, move to delete queue")
		// Move it to delete queue.
		// TODO: there may be multiple objects with the same name in the delete queue, handle it. (-version?)
		// Currently, the delete queue is not processed.
		err = s.kv.Put(deleteQueuePrefix, objectKey, existingValue)
		if err != nil {
			reqlog.Error("failed to Put object to delete queue")
			s.statsd_c.Increment("register_object.fail")
			return nil, status.Errorf(codes.Unavailable, "unable to move object to delete queue")
		}
	}

	// Update datafile offset
	dataFileNewValue := EncodeDataFileValue(int64(partition), dataFileType, int64(in.NextOffset), int64(currentUsedSpace), state)
	wb := s.kv.NewWriteBatch()
	defer wb.Close()
	wb.Put(datafilePrefix, dataFileKey, dataFileNewValue)
	wb.Put(objectPrefix, objectKey, objectValue)

	err = wb.Commit()
	if err != nil {
		reqlog.Error("failed to Put new datafile value and new object entry")
		s.statsd_c.Increment("register_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to update datafile and register new object")
	}
	objMutex.Unlock()

	s.statsd_c.Increment("register_object.ok")
	return &pb.NewObjectReply{}, nil
}

// UnregisterObject removes an an object entry from the kv.
func (s *server) UnregisterObject(ctx context.Context, in *pb.UnregisterObjectInfo) (*pb.DelObjectReply, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "UnregisterObject",
		"Name":          fmt.Sprintf("%s", in.Name),
		"DiskPath":      s.diskPath,
		"DatafileIndex": in.DatafileIndex,
		"Offset":        in.Offset,
		"Length":        in.Length,
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Error("unable to encode object key")
		s.statsd_c.Increment("unregister_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	value, err := s.kv.Get(objectPrefix, objectKey)
	if err != nil {
		reqlog.Error("unable to retrieve object")
		s.statsd_c.Increment("unregister_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve object")
	}

	if value == nil {
		reqlog.Debug("object does not exist")
		s.statsd_c.Increment("unregister_object.ok")
		return nil, status.Errorf(codes.NotFound, "%s", in.Name)
	}

	// Delete key
	err = s.kv.Delete(objectPrefix, objectKey)
	if err != nil {
		reqlog.Error("failed to Delete key")
		s.statsd_c.Increment("unregister_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to unregister object")
	}

	s.statsd_c.Increment("unregister_object.ok")
	return &pb.DelObjectReply{}, nil
}

// RenameObject changes an object key in the kv. (used for erasure code)
func (s *server) RenameObject(ctx context.Context, in *pb.RenameInfo) (*pb.RenameReply, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function": "RenameObject",
		"Name":     fmt.Sprintf("%s", in.Name),
		"NewName":  fmt.Sprintf("%s", in.NewName),
	})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Error("unable to encode object key")
		s.statsd_c.Increment("rename_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	objectNewKey, err := EncodeObjectKey(in.NewName)
	if err != nil {
		reqlog.Error("unable to encode new object key")
		s.statsd_c.Increment("rename_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	value, err := s.kv.Get(objectPrefix, objectKey)
	if err != nil {
		reqlog.Error("unable to retrieve object")
		s.statsd_c.Increment("rename_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve object")
	}

	if value == nil {
		reqlog.Debug("object does not exist")
		s.statsd_c.Increment("rename_object.ok")
		return nil, status.Errorf(codes.NotFound, "%s", in.Name)
	}

	// Delete old entry and create a new one
	wb := s.kv.NewWriteBatch()
	defer wb.Close()
	wb.Delete(objectPrefix, objectKey)
	wb.Put(objectPrefix, objectNewKey, value)

	err = wb.Commit()
	if err != nil {
		reqlog.Error("failed to commit WriteBatch for rename")
		s.statsd_c.Increment("rename_object.fail")
		return nil, status.Errorf(codes.Unavailable, "failed to commit WriteBatch for rename")
	}

	s.statsd_c.Increment("rename_object.ok")
	return &pb.RenameReply{}, nil
}

// LoadObject returns an object information
func (s *server) LoadObject(ctx context.Context, in *pb.LoadObjectInfo) (*pb.Object, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "LoadObject",
		"Name":          fmt.Sprintf("%s", in.Name),
		"IsQuarantined": fmt.Sprintf("%t", in.IsQuarantined),
	})
	reqlog.Debug("RPC Call")

	var prefix byte

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Error("unable to encode object key")
		s.statsd_c.Increment("load_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	if in.IsQuarantined {
		prefix = quarantinePrefix
	} else {
		prefix = objectPrefix
	}
	reqlog.Debugf("is quarantined: %v", in.IsQuarantined)
	value, err := s.kv.Get(prefix, objectKey)
	if err != nil {
		reqlog.Error("unable to retrieve object")
		s.statsd_c.Increment("load_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve object")
	}

	if value == nil {
		reqlog.Debug("object does not exist")
		s.statsd_c.Increment("load_object.ok")
		return nil, status.Errorf(codes.NotFound, "%s", in.Name)
	}

	dataFileIndex, offset, err := DecodeObjectValue(value)
	if err != nil {
		reqlog.Error("failed to decode object value")
		s.statsd_c.Increment("load_object.fail")
		return nil, status.Errorf(codes.Internal, "unable to read object")
	}

	s.statsd_c.Increment("load_object.ok")
	return &pb.Object{Name: in.Name, DatafileIndex: dataFileIndex, Offset: offset}, nil
}

// QuarantineDir will change all keys below a given prefix to mark objects as quarantined. (the whole "directory")
// DEPRECATED. To remove once the matching python code is deployed everywhere
func (s *server) QuarantineDir(ctx context.Context, in *pb.ObjectPrefix) (*pb.DelObjectReply, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function": "QuarantineDir",
		"Prefix":   in.Prefix,
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	// prefix must be 32 characters for this to work (because we now encode the md5 hash, see
	// EncodeObjectKey in encoding.go
	if len(in.Prefix) != 32 {
		reqlog.Error("prefix len != 32")
		s.statsd_c.Increment("quarantine_dir.fail")
		return nil, status.Errorf(codes.Internal, "prefix len != 32")
	}

	prefix, err := EncodeObjectKey(in.Prefix)
	if err != nil {
		reqlog.Error("unable to encode object prefix")
		s.statsd_c.Increment("quarantine_dir.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object prefix")
	}

	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	// TODO: Wrap in a WriteBatch. Still async, so we will still need to rely on the volume
	for it.Seek(prefix); it.Valid() && len(prefix) <= len(it.Key()) && bytes.Equal(prefix, it.Key()[:len(prefix)]); it.Next() {
		// Register quarantined object
		// TODO: may already be present with that name, in which case, append a unique suffix (1, 2, 3..), or we will
		// leak space in the volume file
		reqlog.WithFields(logrus.Fields{
			"Key": it.Key(),
		}).Debug("Quarantine")
		err := s.kv.Put(quarantinePrefix, it.Key(), it.Value())
		if err != nil {
			reqlog.Error("failed to Put new quarantined object entry")
			s.statsd_c.Increment("quarantine_dir.fail")
			return nil, status.Errorf(codes.Unavailable, "unable to register new quarantined object")
		}
		reqlog.Debug("registered new quarantined object")

		// Delete object key
		err = s.kv.Delete(objectPrefix, it.Key())
		if err != nil {
			reqlog.Error("failed to delete key")
			s.statsd_c.Increment("quarantine_dir.fail")
			return nil, status.Errorf(codes.Unavailable, "unable to unregister object")
		}
	}

	s.statsd_c.Increment("quarantine_dir.ok")
	return &pb.DelObjectReply{}, nil
}

// QuarantineObject
func (s *server) QuarantineObject(ctx context.Context, in *pb.ObjectName) (*pb.Empty, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function": "QuarantineObject",
		"Name":     fmt.Sprintf("%s", in.Name),
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Error("unable to encode object key")
		s.statsd_c.Increment("quarantine_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	value, err := s.kv.Get(objectPrefix, objectKey)
	if err != nil {
		reqlog.Error("unable to retrieve object")
		s.statsd_c.Increment("quarantine_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve object")
	}

	if value == nil {
		reqlog.Debug("object does not exist")
		s.statsd_c.Increment("quarantine_object.ok")
		return nil, status.Errorf(codes.NotFound, "%s", in.Name)
	}

	// Add quarantine key, delete obj key
	wb := s.kv.NewWriteBatch()
	defer wb.Close()
	// TODO: check here if an ohash already exists with the same name. Put files in the same dir, or make a new one ? (current swift code
	// appears to add an extension in that case. This will require a new format (encode/decode) in the KV)
	// Also check if full key already exists.
	wb.Put(quarantinePrefix, objectKey, value)
	wb.Delete(objectPrefix, objectKey)
	err = wb.Commit()
	if err != nil {
		reqlog.Error("failed to quarantine object")
		s.statsd_c.Increment("quarantine_object.fail")
		return nil, status.Error(codes.Unavailable, "unable to quarantine object")
	}

	s.statsd_c.Increment("quarantine_object.ok")
	return &pb.Empty{}, nil
}

// UnquarantineObject
func (s *server) UnquarantineObject(ctx context.Context, in *pb.ObjectName) (*pb.Empty, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function": "UnquarantineObject",
		"Name":     fmt.Sprintf("%s", in.Name),
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Error("unable to encode object key")
		s.statsd_c.Increment("unquarantine_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	value, err := s.kv.Get(quarantinePrefix, objectKey)
	if err != nil {
		reqlog.Error("unable to retrieve object")
		s.statsd_c.Increment("unquarantine_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve object")
	}

	if value == nil {
		reqlog.Debug("object does not exist")
		s.statsd_c.Increment("unquarantine_object.ok")
		return nil, status.Errorf(codes.NotFound, "%s", in.Name)
	}

	// Add object key, delete quarantine key
	wb := s.kv.NewWriteBatch()
	defer wb.Close()
	wb.Put(objectPrefix, objectKey, value)
	wb.Delete(quarantinePrefix, objectKey)
	err = wb.Commit()
	if err != nil {
		reqlog.Error("failed to unquarantine object")
		s.statsd_c.Increment("unquarantine_object.fail")
		return nil, status.Error(codes.Unavailable, "unable to unquarantine object")
	}

	s.statsd_c.Increment("unquarantine_object.ok")
	return &pb.Empty{}, nil
}

// LoadObjectsByPrefix returns list of objects with the given prefix.
// In practice this is used to emulate the object hash directory that swift
// would create with the regular diskfile backend.
func (s *server) LoadObjectsByPrefix(ctx context.Context, in *pb.ObjectPrefix) (*pb.LoadObjectsResponse, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function": "LoadObjectsByPrefix",
		"Prefix":   fmt.Sprintf("%s", in.Prefix),
	})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	// prefix must be 32 characters for this to work (because we now encode the md5 hash, see
	// EncodeObjectKey in encoding.go
	if len(in.Prefix) != 32 {
		reqlog.Error("prefix len != 32")
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Internal, "prefix len != 32")
	}

	prefix, err := EncodeObjectKey(in.Prefix)
	if err != nil {
		reqlog.Error("unable to encode object prefix")
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object prefix")
	}

	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	response := &pb.LoadObjectsResponse{}

	// adds one byte because of prefix. Otherwise len(prefix) would be len(prefix)-1
	for it.Seek(prefix); it.Valid() && len(prefix) <= len(it.Key()) && bytes.Equal(prefix, it.Key()[:len(prefix)]); it.Next() {

		// Decode value
		dataFileIndex, offset, err := DecodeObjectValue(it.Value())
		if err != nil {
			reqlog.Error("failed to decode object value")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to read object")
		}

		key := make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key(), key)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		response.Objects = append(response.Objects, &pb.Object{Name: key, DatafileIndex: dataFileIndex, Offset: offset})
	}

	s.statsd_c.Increment("load_objects_by_prefix.ok")
	return response, nil
}

// LoadObjectsByDataFile returns a list of all objects within a datafile.
// TODO: add an option to list quarantined objects
func (s *server) LoadObjectsByDataFile(in *pb.DataFileIndex, stream pb.FileMgr_LoadObjectsByDataFileServer) error {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "LoadObjectsByDataFile",
		"DataFileIndex": in.Index})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	// Need an option to return quarantined files
	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	// Objects are not indexed by volume. We have to scan the whole KV and examine each value.
	// It shouldn't matter as this is only used for compaction, and each object will have to be copied.
	// Disk activity dwarfs CPU usage. (for spinning rust anyway, but SSDs?)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		dataFileIndex, offset, err := DecodeObjectValue(it.Value())
		if err != nil {
			reqlog.Error("failed to decode object value")
			s.statsd_c.Increment("load_objects_by_datafile.fail")
			return status.Errorf(codes.Internal, "unable to read object")
		}

		if dataFileIndex == in.Index {
			key := make([]byte, 32+len(it.Key()[16:]))
			err = DecodeObjectKey(it.Key(), key)
			if err != nil {
				reqlog.Error("failed to decode object key")
				s.statsd_c.Increment("load_objects_by_prefix.fail")
				return status.Errorf(codes.Internal, "unable to decode object key")
			}
			object := &pb.Object{Name: key, DatafileIndex: dataFileIndex, Offset: offset}
			err = stream.Send(object)
			if err != nil {
				reqlog.Error("failed to send streamed response")
				s.statsd_c.Increment("load_objects_by_datafile.fail")
				return status.Errorf(codes.Internal, "failed to send streamed response")
			}
		}
	}
	s.statsd_c.Increment("load_objects_by_datafile.ok")
	return nil
}

// ListPartitions returns a list of partitions for which we have objects.
// This is used to emulate a listdir() of partitions below the "objects" directory.
func (s *server) ListPartitions(ctx context.Context, in *pb.ListPartitionsInfo) (*pb.DirEntries, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "ListPartitions",
		"PartitionBits": in.PartitionBits,
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	var currentPartition uint64
	var err error
	var ohash []byte

	// Partition bits
	pBits := int(in.PartitionBits)

	response := &pb.DirEntries{}

	// Seek to first object key
	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()
	it.SeekToFirst()

	// No object in the KV.
	if !it.Valid() {
		s.statsd_c.Increment("list_partitions.ok")
		return response, nil
	}

	// Extract the md5 hash
	reqlog.WithFields(logrus.Fields{"key": it.Key()}).Debug("Raw first KV key")
	if len(it.Key()) < 16 {
		reqlog.WithFields(logrus.Fields{"key": it.Key()}).Error("object key < 16")
	} else {
		ohash = make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key()[:16], ohash)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		currentPartition, err = getPartitionFromOhash(ohash, pBits)
		if err != nil {
			s.statsd_c.Increment("list_partitions.fail")
			return response, err
		}
	}

	response.Entry = append(response.Entry, fmt.Sprintf("%d", currentPartition))
	if err != nil {
		s.statsd_c.Increment("list_partitions.fail")
		return response, err
	}

	maxPartition, err := getLastPartition(pBits)

	for currentPartition < maxPartition {
		currentPartition++
		firstKey, err := getEncodedObjPrefixFromPartition(currentPartition, pBits)
		if err != nil {
			s.statsd_c.Increment("list_partitions.fail")
			return response, err
		}
		nextFirstKey, err := getEncodedObjPrefixFromPartition(currentPartition+1, pBits)
		if err != nil {
			s.statsd_c.Increment("list_partitions.fail")
			return response, err
		}

		// key logging is now wrong, as it's not the ascii form
		reqlog.WithFields(logrus.Fields{"currentPartition": currentPartition,
			"maxPartition": maxPartition,
			"firstKey":     firstKey,
			"ohash":        ohash,
			"nextFirstKey": nextFirstKey}).Debug("In loop")

		it.Seek(firstKey)
		if !it.Valid() {
			s.statsd_c.Increment("list_partitions.ok")
			return response, nil
		}

		if len(it.Key()) < 16 {
			reqlog.WithFields(logrus.Fields{"key": it.Key()}).Error("object key < 16")
		} else {
			ohash = make([]byte, 32+len(it.Key()[16:]))
			err = DecodeObjectKey(it.Key()[:16], ohash)
			if err != nil {
				reqlog.Error("failed to decode object key")
				s.statsd_c.Increment("load_objects_by_prefix.fail")
				return nil, status.Errorf(codes.Internal, "unable to decode object key")
			}
			// nextFirstKey is encoded, compare with encoded hash (16 first bits of the key)
			if bytes.Compare(it.Key()[:16], nextFirstKey) > 0 {
				// There was no key in currentPartition, find in which partition we are
				currentPartition, err = getPartitionFromOhash(ohash, pBits)
				if err != nil {
					s.statsd_c.Increment("list_partitions.fail")
					return response, err
				}
			}
			response.Entry = append(response.Entry, fmt.Sprintf("%d", currentPartition))
		}
	}

	reqlog.Debug("ListPartitions done")
	s.statsd_c.Increment("list_partitions.ok")
	return response, nil
}

// ListPartitionRecursive returns a list of files with structured path info (suffix, object hash) within a partition
// The response should really be streamed, but that makes eventlet hang on the python side...
// This is used to optimize REPLICATE on the object server.
func (s *server) ListPartitionRecursive(ctx context.Context, in *pb.ListPartitionInfo) (*pb.PartitionContent, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "ListPartitionRecursive",
		"Partition":     in.Partition,
		"PartitionBits": in.PartitionBits,
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	// Partition bits
	pBits := int(in.PartitionBits)
	partition := uint64(in.Partition)

	firstKey, err := getEncodedObjPrefixFromPartition(partition, pBits)
	if err != nil {
		s.statsd_c.Increment("list_partition_recursive.fail")
		return nil, status.Errorf(codes.Internal, "failed to calculate encoded object prefix from partition")
	}

	// Seek to first key in partition, if any
	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	response := &pb.PartitionContent{}

	it.Seek(firstKey)
	// No object in the KV
	if !it.Valid() {
		s.statsd_c.Increment("list_partition_recursive.ok")
		return response, nil
	}

	key := make([]byte, 32+len(it.Key()[16:]))
	err = DecodeObjectKey(it.Key(), key)
	if err != nil {
		reqlog.Error("failed to decode object key")
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Internal, "unable to decode object key")
	}
	currentPartition, err := getPartitionFromOhash(key, pBits)
	if err != nil {
		s.statsd_c.Increment("list_partition_recursive.fail")
		return nil, status.Errorf(codes.Internal, "unable to extract partition from object hash")
	}

	// Iterate over all files within the partition
	for currentPartition == partition {
		reqlog.Debug("Sending an entry")
		entry := &pb.FullPathEntry{Suffix: key[29:32], Ohash: key[:32], Filename: key[32:]}
		response.FileEntries = append(response.FileEntries, entry)

		it.Next()
		// Check if we're at the end of the KV
		if !it.Valid() {
			break
		}
		key = make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key(), key)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		currentPartition, err = getPartitionFromOhash(key, pBits)
	}

	s.statsd_c.Increment("list_partition_recursive.ok")
	return response, nil
}

// ListPartition returns a list of suffixes within a partition
func (s *server) ListPartition(ctx context.Context, in *pb.ListPartitionInfo) (*pb.DirEntries, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "ListPartition",
		"Partition":     in.Partition,
		"PartitionBits": in.PartitionBits,
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	// Set to hold suffixes within partition
	suffixSet := make(map[[3]byte]bool)
	var suffix [3]byte

	// Partition bits
	pBits := int(in.PartitionBits)
	partition := uint64(in.Partition)

	response := &pb.DirEntries{}

	firstKey, err := getEncodedObjPrefixFromPartition(partition, pBits)
	if err != nil {
		s.statsd_c.Increment("list_partition.fail")
		return response, err
	}

	// Seek to first key in partition, if any
	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	it.Seek(firstKey)
	// No object in the KV
	if !it.Valid() {
		s.statsd_c.Increment("list_partition.ok")
		return response, nil
	}

	key := make([]byte, 32+len(it.Key()[16:]))
	err = DecodeObjectKey(it.Key(), key)
	if err != nil {
		reqlog.Error("failed to decode object key")
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Internal, "unable to decode object key")
	}
	currentPartition, err := getPartitionFromOhash(key, pBits)
	if err != nil {
		s.statsd_c.Increment("list_partition.fail")
		return response, err
	}

	// Get all suffixes in the partition
	for currentPartition == partition {
		// Suffix is the last three bytes of the object hash
		copy(suffix[:], key[29:32])
		suffixSet[suffix] = true
		it.Next()
		if !it.Valid() {
			break
		}
		key = make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key(), key)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		currentPartition, err = getPartitionFromOhash(key, pBits)
	}

	// Build the response from the hashmap
	for suffix := range suffixSet {
		response.Entry = append(response.Entry, fmt.Sprintf("%s", suffix))
	}

	s.statsd_c.Increment("list_partition.ok")
	return response, nil
}

// ListSuffix returns a list of object hashes below the partition and suffix
func (s *server) ListSuffix(ctx context.Context, in *pb.ListSuffixInfo) (*pb.DirEntries, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "ListSuffix",
		"Partition":     in.Partition,
		"Suffix":        fmt.Sprintf("%s", in.Suffix),
		"PartitionBits": in.PartitionBits,
	})

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	execTimeSerie := fmt.Sprintf("list_suffix.runtime.%s", s.diskName)
	defer s.statsd_c.NewTiming().Send(execTimeSerie)
	reqlog.Debug("RPC Call")

	lastOhash := make([]byte, 32)

	pBits := int(in.PartitionBits)
	partition := uint64(in.Partition)
	suffix := in.Suffix

	response := &pb.DirEntries{}

	failSerie := fmt.Sprintf("list_suffix.fail.%s", s.diskName)
	successSerie := fmt.Sprintf("list_suffix.ok.%s", s.diskName)
	firstKey, err := getEncodedObjPrefixFromPartition(partition, pBits)
	if err != nil {
		s.statsd_c.Increment(failSerie)
		return response, err
	}

	// Seek to first key in partition, if any
	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	it.Seek(firstKey)
	// No object in the KV
	if !it.Valid() {
		s.statsd_c.Increment(successSerie)
		return response, nil
	}

	// Allocate the slice with a capacity matching the length of the longest possible key
	// We can then reuse it in the loop below. (avoid heap allocations, profiling showed it was an issue)
	curKey := make([]byte, 32+len(firstKey[16:]), maxObjKeyLen)
	err = DecodeObjectKey(firstKey, curKey)
	if err != nil {
		reqlog.Error("failed to decode object key")
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Internal, "unable to decode object key")
	}
	currentPartition, err := getPartitionFromOhash(curKey, pBits)
	if err != nil {
		s.statsd_c.Increment(failSerie)
		return response, err
	}

	for currentPartition == partition {
		// Suffix is the last three bytes of the object hash
		// key := make([]byte, 32+len(it.Key()[16:]))
		curKey = curKey[:32+len(it.Key()[16:])]
		err = DecodeObjectKey(it.Key(), curKey)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		if bytes.Compare(curKey[29:32], suffix) == 0 {
			ohash := make([]byte, 32)
			ohash = curKey[:32]
			// Only add to the list if we have not already done so
			if !bytes.Equal(ohash, lastOhash) {
				response.Entry = append(response.Entry, (fmt.Sprintf("%s", ohash)))
				copy(lastOhash, ohash)
			}
		}
		it.Next()
		if !it.Valid() {
			break
		}
		curKey = curKey[:32+len(it.Key()[16:])]
		err = DecodeObjectKey(it.Key(), curKey)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		currentPartition, err = getPartitionFromOhash(curKey, pBits)
	}

	s.statsd_c.Increment(successSerie)
	return response, nil
}

// ListQuarantineOHashes returns the list of quarantined object hashes
// THe list may be large (over 500k entries seen in production). We don't want/can't use too large
// messages. We can't use streaming until we get rid of eventlet. For now, use a cursor to return
// results in multiple messages, if needed.
func (s *server) ListQuarantinedOHashes(ctx context.Context, in *pb.ListQuarantinedOHashesInfo) (*pb.QuarantinedObjects, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":  "ListQuarantineOHashes",
		"FromEntry": in.FromEntry,
	})

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	reqlog.Debug("RPC Call")

	maxEntriesPerMessage := 10000
	it := s.kv.NewIterator(quarantinePrefix)
	it.SeekToFirst()
	defer it.Close()
	response := &pb.QuarantinedObjects{}
	var count int

	// seek to the FromEntry requested, if set
	if !bytes.Equal(in.FromEntry, []byte("")) {
		reqlog.Error("FromEntry not empty")
		firstObj, err := EncodeObjectKey(in.FromEntry)
		if err != nil {
			reqlog.Error("unable to encode FromEntry")
			s.statsd_c.Increment("list_quarantined_ohashes.fail")
			return nil, status.Errorf(codes.Unavailable, "unable to encode FromEntry")
		}
		it.Seek(firstObj)

		// skip to the next item, if any
		if it.Valid() {
			it.Next()
		}
	}

	curKey := make([]byte, maxObjKeyLen)
	lastOhash := make([]byte, 32)
	// Iterate over all quarantined files, extracting unique object hashes
	for ; it.Valid() && count < maxEntriesPerMessage; it.Next() {
		curKey = curKey[:32+len(it.Key()[16:])]
		err := DecodeObjectKey(it.Key(), curKey)
		if err != nil {
			reqlog.Error("failed to decode quarantined object key")
			s.statsd_c.Increment("list_quarantined_ohashes.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode quarantined object key")
		}
		if !bytes.Equal(curKey[:32], lastOhash) {
			response.Entry = append(response.Entry, fmt.Sprintf("%s", curKey[:32]))
			copy(lastOhash, curKey[:32])
			count++
		}
	}

	// TODO: this is untested
	// Set last entry if we have hit the limit
	if count == maxEntriesPerMessage-1 {
		response.LastEntry = fmt.Sprintf("%s", lastOhash)
	}

	s.statsd_c.Increment("list_quarantined_ohashes.ok")
	return response, nil
}

func (s *server) ListQuarantinedOHash(ctx context.Context, in *pb.ObjectPrefix) (*pb.LoadObjectsResponse, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function": "ListQuarantineOHash",
		"Prefix":   fmt.Sprintf("%s", in.Prefix),
	})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	if len(in.Prefix) != 32 {
		reqlog.Error("prefix len != 32")
		s.statsd_c.Increment("list_quarantined_ohash.fail")
		return nil, status.Errorf(codes.Internal, "prefix len != 32")
	}

	prefix, err := EncodeObjectKey(in.Prefix)
	if err != nil {
		reqlog.Error("unable to encode object prefix")
		s.statsd_c.Increment("list_quarantined_ohash.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object prefix")
	}

	it := s.kv.NewIterator(quarantinePrefix)
	defer it.Close()

	response := &pb.LoadObjectsResponse{}

	// adds one byte because of prefix. Otherwise len(prefix) would be len(prefix)-1
	for it.Seek(prefix); it.Valid() && len(prefix) <= len(it.Key()) && bytes.Equal(prefix, it.Key()[:len(prefix)]); it.Next() {

		// Decode value
		dataFileIndex, offset, err := DecodeObjectValue(it.Value())
		if err != nil {
			reqlog.Error("failed to decode object value")
			s.statsd_c.Increment("list_quarantined_ohash.fail")
			return nil, status.Errorf(codes.Internal, "unable to read object")
		}

		key := make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key(), key)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("list_quarantined_ohash.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		response.Objects = append(response.Objects, &pb.Object{Name: key, DatafileIndex: dataFileIndex, Offset: offset})
	}

	s.statsd_c.Increment("list_quarantined_ohash.ok")
	return response, nil
}

func (s *server) GetNextOffset(ctx context.Context, in *pb.GetNextOffsetInfo) (*pb.DataFileNextOffset, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "GetNextOffset", "DatafileIndex": in.DatafileIndex})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeDataFileKey(in.DatafileIndex)

	value, err := s.kv.Get(datafilePrefix, key)
	if err != nil {
		reqlog.Error("unable to retrieve datafile key")
		s.statsd_c.Increment("get_next_offset.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve datafile key")
	}

	if value == nil {
		reqlog.Info("datafile index does not exist in db")
		s.statsd_c.Increment("get_next_offset.fail")
		return nil, status.Errorf(codes.FailedPrecondition, "datafile index does not exist in db")
	}

	_, _, nextOffset, _, _, err := DecodeDataFileValue(value)
	if err != nil {
		reqlog.WithFields(logrus.Fields{"value": value}).Error("failed to decode data file value")
		s.statsd_c.Increment("get_next_offset.fail")
		return nil, status.Errorf(codes.Internal, "failed to decode data file value")
	}

	s.statsd_c.Increment("get_next_offset.ok")
	return &pb.DataFileNextOffset{Offset: uint64(nextOffset)}, nil
}

// GetStats returns stats for the KV. used for initial debugging, remove?
func (s *server) GetStats(ctx context.Context, in *pb.GetStatsInfo) (kvstats *pb.KVStats, err error) {
	kvstats = new(pb.KVStats)

	m := CollectStats(s)
	kvstats.Stats = m

	return
}

// Sets KV state (is in sync with volumes, or not)
func (s *server) SetKvState(ctx context.Context, in *pb.KvState) (*pb.Empty, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "SetClean", "IsClean": in.IsClean})
	reqlog.Info("RPC Call")

	s.isClean = in.IsClean
	return &pb.Empty{}, nil
}

// Gets KV state (is in sync with volumes, or not)
func (s *server) GetKvState(ctx context.Context, in *pb.Empty) (*pb.KvState, error) {
	state := new(pb.KvState)
	state.IsClean = s.isClean
	return state, nil
}

// Stops serving RPC requests and closes KV if we receive SIGTERM/SIGINT
func shutdownHandler(s *server, wg *sync.WaitGroup) {
	<-s.stopChan
	rlog := log.WithFields(logrus.Fields{"socket": s.socketPath})
	rlog.Info("Shutting down")

	// Stop serving RPC requests
	// If the graceful shutdown does not complete in 5s, call Stop(). (it is safe to do, will broadcast to the
	// running GracefulStop)
	rlog.Debug("Stopping RPC")
	t := time.AfterFunc(time.Second*5, func() { s.grpcServer.Stop() })
	s.grpcServer.GracefulStop()
	t.Stop()

	// Mark DB as clean
	if s.isClean == true {
		rlog.Debug("Mark DB as closed")
		err := MarkDbClosed(s.kv)
		if err != nil {
			rlog.Warn("Failed to mark db as clean when shutting down")
		}
	} else {
		rlog.Warn("State is not clean, not marking DB as closed (still out of sync with volumes)")
	}

	// Close KV
	rlog.Debug("Closing KV")
	s.kv.Close()
	wg.Done()
}

func runServer(kv KV, diskPath string, socketPath string, stopChan chan os.Signal, isClean bool) (err error) {
	var wg sync.WaitGroup

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return
	}
	os.Chmod(socketPath, 0660)

	grpcServer := grpc.NewServer()
	_, diskName := path.Split(path.Clean(diskPath))
	fs := &server{kv: kv, grpcServer: grpcServer, diskPath: diskPath, diskName: diskName, socketPath: socketPath,
		isClean: isClean, stopChan: stopChan}

	// Initialize statsd client
	statsdPrefix := "kv"
	fs.statsd_c, err = statsd.New(statsd.Prefix(statsdPrefix))
	if err != nil {
		return
	}

	// Start shutdown handler
	wg.Add(1)
	go shutdownHandler(fs, &wg)

	pb.RegisterFileMgrServer(grpcServer, fs)

	// Ignore the error returned by Serve on termination
	// (the doc : Serve always returns non-nil error.)
	grpcServer.Serve(lis)

	wg.Wait()

	return
}
