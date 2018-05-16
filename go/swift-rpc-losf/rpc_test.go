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

package main

import (
	"bytes"
	pb "github.com/openstack/swift-rpc-losf/proto"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/alexcesaro/statsd.v2"
	"io/ioutil"
	"net"
	"os"
	"path"
	"strings"
	"testing"
	"time"
)

func runTestServer(kv KV, diskPath string, addr string) (err error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return
	}

	s := grpc.NewServer()
	_, diskName := path.Split(path.Clean(diskPath))
	fs := &server{kv: kv, diskPath: diskPath, diskName: diskName, isClean: true}

	statsdPrefix := "kv"
	fs.statsd_c, err = statsd.New(statsd.Prefix(statsdPrefix))
	if err != nil {
		return
	}

	pb.RegisterFileMgrServer(s, fs)
	s.Serve(lis)
	return
}

func teardown(tempdir string) {
	if strings.HasPrefix(tempdir, "/tmp/") {
		os.RemoveAll(tempdir)
	}
}

var client pb.FileMgrClient

func populateKV() (err error) {
	dataFiles := []pb.NewDataFileInfo{
		{9, 0, 20, 8192, 0, false},
		{10, 0, 35, 8192, 0, false},
		{40, 0, 24, 8192, 0, false},
		{63, 0, 27, 8192, 0, false},
		{65, 0, 33, 8192, 0, false},
		{71, 0, 19, 8192, 0, false},
		{111, 0, 47, 8192, 0, false},
		{127, 0, 43, 8192, 0, false},
		{139, 0, 50, 8192, 0, false},
		{171, 0, 49, 8192, 0, false},
		{195, 0, 12, 8192, 0, false},
		{211, 0, 16, 8192, 0, false},
		{213, 0, 14, 8192, 0, false},
		{243, 0, 17, 8192, 0, false},
		{271, 0, 8, 24576, 0, false},
		{295, 0, 28, 8192, 0, false},
		{327, 0, 48, 8192, 0, false},
		{360, 0, 15, 12288, 0, false},
		{379, 0, 25, 8192, 0, false},
		{417, 0, 22, 8192, 0, false},
		{420, 0, 32, 8192, 0, false},
		{421, 0, 46, 8192, 0, false},
		{428, 0, 21, 12288, 0, false},
		{439, 0, 38, 8192, 0, false},
		{453, 0, 44, 8192, 0, false},
		{466, 0, 40, 8192, 0, false},
		{500, 0, 39, 8192, 0, false},
		{513, 0, 26, 8192, 0, false},
		{530, 0, 4, 8192, 0, false},
		{530, 1, 5, 8192, 0, false},
		{535, 0, 1, 20480, 0, false},
		{535, 0, 2, 4096, 0, false},
		{535, 1, 3, 12288, 0, false},
		{559, 0, 30, 8192, 0, false},
		{602, 0, 41, 8192, 0, false},
		{604, 0, 29, 8192, 0, false},
		{673, 0, 11, 8192, 0, false},
		{675, 0, 42, 8192, 0, false},
		{710, 0, 37, 8192, 0, false},
		{765, 0, 36, 8192, 0, false},
		{766, 0, 45, 8192, 0, false},
		{786, 0, 23, 8192, 0, false},
		{809, 0, 31, 8192, 0, false},
		{810, 0, 13, 8192, 0, false},
		{855, 0, 18, 8192, 0, false},
		{974, 0, 9, 8192, 0, false},
		{977, 0, 6, 8192, 0, false},
		{977, 1, 7, 8192, 0, false},
		{1009, 0, 34, 8192, 0, false},
		{1019, 0, 10, 8192, 0, false},
	}

	objects := []pb.NewObjectInfo{
		{[]byte("85fd12f8961e33cbf7229a94118524fa1515589781.45671.ts"), 3, 8192, 12288, false},
		{[]byte("84afc1659c7e8271951fe370d6eee0f81515590332.51834.ts"), 5, 4096, 8192, false},
		{[]byte("f45bf9000f39092b9de5a74256e3eebe1515590648.06511.ts"), 7, 4096, 8192, false},
		{[]byte("43c8adc53dbb40d27add4f614fc49e5e1515595691.35618#0#d.data"), 8, 20480, 24576, false},
		{[]byte("f3804523d91d294dab1500145b43395b1515596136.42189#4#d.data"), 9, 4096, 8192, false},
		{[]byte("fefe1ba1120cd6cd501927401d6b2ecc1515750800.13517#2#d.data"), 10, 4096, 8192, false},
		{[]byte("a8766d2608b77dc6cb0bfe3fe6782c731515750800.18975#0#d.data"), 11, 4096, 8192, false},
		{[]byte("30f12368ca25d11fb1a80d10e64b15431515750800.19224#4#d.data"), 12, 4096, 8192, false},
		{[]byte("ca9576ada218f74cb8f11648ecec439c1515750800.21553#2#d.data"), 13, 4096, 8192, false},
		{[]byte("3549df7ef11006af6852587bf16d82971515750800.22096#2#d.data"), 14, 4096, 8192, false},
		{[]byte("5a0a70e36a057a9982d1dc9188069b511515750803.50544#0#d.data"), 15, 8192, 12288, false},
		{[]byte("5a1801fea97614f8c5f58511905773d01515750800.40035#0#d.data"), 15, 4096, 8192, false},
		{[]byte("34c46ce96897a24374d126d7d7eab2fb1515750800.42545#0#d.data"), 16, 4096, 8192, false},
		{[]byte("3cf60143ea488c84da9e1603158203a11515750800.93160#0#d.data"), 17, 4096, 8192, false},
		{[]byte("d5c64e9cb0b093441fb6b500141aa0531515750800.94069#2#d.data"), 18, 4096, 8192, false},
		{[]byte("11f5db768b6f9a37cf894af99b15c0d11515750801.05135#4#d.data"), 19, 4096, 8192, false},
		{[]byte("02573d31b770cda8e0effd7762e8a0751515750801.09785#2#d.data"), 20, 4096, 8192, false},
		{[]byte("6b08eabf5667557c72dc6570aa1fb8451515750801.08639#4#d.data"), 21, 4096, 8192, false},
		{[]byte("6b08eabf5667557c72dc6570aa1fb8451515750856.77219.meta"), 21, 8192, 12288, false},
		{[]byte("6b08eabf5667557c72dc6570abcfb8451515643210.72429#4#d.data"), 22, 8192, 12288, false},
		{[]byte("687ba0410f4323c66397a85292077b101515750801.10244#0#d.data"), 22, 4096, 8192, false},
		{[]byte("c4aaea9b28c425f45eb64d4d5b0b3f621515750801.19478#2#d.data"), 23, 4096, 8192, false},
		{[]byte("0a0898eb861579d1240adbb1c9f0c92b1515750801.20636#2#d.data"), 24, 4096, 8192, false},
		{[]byte("5efd43142db5913180ba865ef529eccd1515750801.64704#4#d.data"), 25, 4096, 8192, false},
		{[]byte("806a35f1e974f93161b2da51760f22701515750801.68309#2#d.data"), 26, 4096, 8192, false},
		{[]byte("0fdceb7af49cdd0cb1262acbdc88ae881515750801.93565#0#d.data"), 27, 4096, 8192, false},
		{[]byte("49d4fa294d2c97f08596148bf4615bfa1515750801.93739#4#d.data"), 28, 4096, 8192, false},
		{[]byte("971b4d05733f475d447d7f8b050bb0071515750802.09721#2#d.data"), 29, 4096, 8192, false},
		{[]byte("8bc66b3ae033db15ceb3729d89a07ece1515750802.51062#0#d.data"), 30, 4096, 8192, false},
		{[]byte("ca53beae1aeb4deacd17409e32305a2c1515750802.63996#2#d.data"), 31, 4096, 8192, false},
		{[]byte("69375433763d9d511114e8ac869c916c1515750802.63846#0#d.data"), 32, 4096, 8192, false},
		{[]byte("105de5f388ab4b72e56bc93f36ad388a1515750802.73393#2#d.data"), 33, 4096, 8192, false},
		{[]byte("105de5f388ab4b72e56bc93f36ad388a1515873948.27383#2#d.meta"), 33, 8192, 12288, false},
		{[]byte("fc6916fd1e6a0267afac88c395b876ac1515750802.83459#2#d.data"), 34, 4096, 8192, false},
		{[]byte("02b10d6bfb205fe0f34f9bd82336dc711515750802.93662#2#d.data"), 35, 4096, 8192, false},
		{[]byte("bf43763a98208f15da803e76bf52e7d11515750803.01357#0#d.data"), 36, 4096, 8192, false},
		{[]byte("b1abadfed91b1cb4392dd2ec29e171ac1515750803.07767#4#d.data"), 37, 4096, 8192, false},
		{[]byte("6de30d74634d088f1f5923336af2b3ae1515750803.36199#4#d.data"), 38, 4096, 8192, false},
		{[]byte("7d234bbd1137d509105245ac78427b9f1515750803.49022#4#d.data"), 39, 4096, 8192, false},
		{[]byte("749057975c1bac830360530bdcd741591515750803.49647#0#d.data"), 40, 4096, 8192, false},
		{[]byte("9692991e77c9742cbc24469391d499981515750803.56295#0#d.data"), 41, 4096, 8192, false},
		{[]byte("a8dbd473e360787caff0b97aca33373f1515750803.68428#2#d.data"), 42, 4096, 8192, false},
		{[]byte("1ff88cb2b6b64f1fd3b6097f20203ee01515750803.73746#4#d.data"), 43, 4096, 8192, false},
		{[]byte("71572f46094d7ac440f5e2a3c72da17b1515750803.75628#2#d.data"), 44, 4096, 8192, false},
		{[]byte("bf8e83d954478d66ac1dba7eaa832c721515750803.81141#4#d.data"), 45, 4096, 8192, false},
		{[]byte("69724f682fe12b4a4306bceeb75825431515750804.10112#2#d.data"), 46, 4096, 8192, false},
		{[]byte("1bf38645ccc5f158c96480f1e0861a141515750804.31472#0#d.data"), 47, 4096, 8192, false},
		{[]byte("51fecf0e0bb30920fd0d83ee8fba29f71515750804.32492#2#d.data"), 48, 4096, 8192, false},
		{[]byte("2acbf85061e46b3bb3adb8930cb7414d1515750804.46622#2#d.data"), 49, 4096, 8192, false},
		{[]byte("22e4a97f1d4f2b6d4150bb9b481e4c971515750804.51987#0#d.data"), 50, 4096, 8192, false},
	}

	// Register datafiles (volumes)
	for _, df := range dataFiles {
		_, err = client.RegisterDataFile(context.Background(), &df)
		if err != nil {
			return
		}
	}

	// Register objects
	for _, obj := range objects {
		_, err = client.RegisterObject(context.Background(), &obj)
		if err != nil {
			return
		}
	}
	return
}

func TestMain(m *testing.M) {
	log.Info("RPC test setup")
	log.SetLevel(logrus.ErrorLevel)
	diskPath, err := ioutil.TempDir("/tmp", "losf-test")
	if err != nil {
		log.Fatal(err)
	}
	rootDir := path.Join(diskPath, "losf")
	dbDir := path.Join(rootDir, "db")

	err = os.MkdirAll(rootDir, 0700)
	if err != nil {
		log.Fatal(err)
	}

	kv, err := openLevigoDB(dbDir)
	if err != nil {
		log.Fatal("failed to create leveldb")
	}
	addr := "127.0.0.1:22345"
	go runTestServer(kv, diskPath, addr)

	// test client
	opts := []grpc.DialOption{grpc.WithBlock(), grpc.WithTimeout(time.Second), grpc.WithInsecure()}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		log.Fatal(err)
	}

	client = pb.NewFileMgrClient(conn)

	err = populateKV()
	if err != nil {
		log.Error(err)
		log.Fatal("failed to populate test KV")
	}

	ret := m.Run()

	teardown(diskPath)

	os.Exit(ret)
}

// TODO, add more tests:
//   - prefix with no objects
//   - single object
//   - first and last elements of the KV
func TestLoadObjectsByPrefix(t *testing.T) {
	prefix := &pb.ObjectPrefix{Prefix: []byte("105de5f388ab4b72e56bc93f36ad388a")}

	expectedObjects := []pb.Object{
		{[]byte("105de5f388ab4b72e56bc93f36ad388a1515750802.73393#2#d.data"), 33, 4096},
		{[]byte("105de5f388ab4b72e56bc93f36ad388a1515873948.27383#2#d.meta"), 33, 8192},
	}

	r, err := client.LoadObjectsByPrefix(context.Background(), prefix)
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}

	for i, obj := range r.Objects {
		expected := expectedObjects[i]
		if !bytes.Equal(obj.Name, expected.Name) {
			t.Errorf("\ngot     : %s\nexpected: %s", string(obj.Name), string(expected.Name))
		}
	}
}

func TestListPartitions(t *testing.T) {
	partPower := uint32(10)

	expectedPartitions := []string{"9", "10", "40", "63", "65", "71", "111", "127", "139", "171", "195", "211", "213", "243", "271", "295", "327", "360", "379", "417", "420", "421", "428", "439", "453", "466", "500", "513", "530", "535", "559", "602", "604", "673", "675", "710", "765", "766", "786", "809", "810", "855", "974", "977", "1009", "1019"}

	r, err := client.ListPartitions(context.Background(), &pb.ListPartitionsInfo{partPower})
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}

	if len(r.Entry) != len(expectedPartitions) {
		t.Fatalf("\ngot: %v\nwant: %v", r.Entry, expectedPartitions)
	}

	for i, obj := range r.Entry {
		if obj != expectedPartitions[i] {
			t.Fatalf("checking individual elements\ngot: %v\nwant: %v", r.Entry, expectedPartitions)
		}
	}
}

func TestListPartitionRecursive(t *testing.T) {
	partition := uint32(428)
	partPower := uint32(10)

	expEntries := []pb.FullPathEntry{
		{[]byte("845"), []byte("6b08eabf5667557c72dc6570aa1fb845"), []byte("1515750801.08639#4#d.data")},
		{[]byte("845"), []byte("6b08eabf5667557c72dc6570aa1fb845"), []byte("1515750856.77219.meta")},
		{[]byte("845"), []byte("6b08eabf5667557c72dc6570abcfb845"), []byte("1515643210.72429#4#d.data")},
	}

	r, err := client.ListPartitionRecursive(context.Background(), &pb.ListPartitionInfo{partition, partPower})
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}

	if len(r.FileEntries) != len(expEntries) {
		t.Fatalf("\ngot: %v\nwant: %v", r.FileEntries, expEntries)
	}

	for i, e := range r.FileEntries {
		if !bytes.Equal(e.Suffix, expEntries[i].Suffix) || !bytes.Equal(e.Ohash, expEntries[i].Ohash) || !bytes.Equal(e.Filename, expEntries[i].Filename) {
			t.Fatalf("checking individual elements\ngot: %v\nwant: %v", r.FileEntries, expEntries)
		}
	}
}

// TODO: add more tests, have a suffix with multiple entries
func TestListSuffix(t *testing.T) {
	partition := uint32(428)
	partPower := uint32(10)
	suffix := []byte("845")

	expectedHashes := []string{"6b08eabf5667557c72dc6570aa1fb845", "6b08eabf5667557c72dc6570abcfb845"}

	r, err := client.ListSuffix(context.Background(), &pb.ListSuffixInfo{partition, suffix, partPower})
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}

	if len(r.Entry) != len(expectedHashes) {
		t.Fatalf("\ngot: %v\nwant: %v", r.Entry, expectedHashes)
	}

	for i, obj := range r.Entry {
		if obj != expectedHashes[i] {
			t.Fatalf("checking individual elements\ngot: %v\nwant: %v", r.Entry, expectedHashes)
		}
	}
}

func TestState(t *testing.T) {
	// Mark dirty and check
	_, err := client.SetKvState(context.Background(), &pb.KvState{false})
	if err != nil {
		t.Fatalf("Failed to change KV state")
	}
	resp, err := client.GetKvState(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}
	if resp.IsClean != false {
		t.Fatal("isClean true, should be false")
	}

	// Mark clean and check
	_, err = client.SetKvState(context.Background(), &pb.KvState{true})
	if err != nil {
		t.Fatalf("Failed to change KV state")
	}
	resp, err = client.GetKvState(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}
	if resp.IsClean != true {
		t.Fatal("isClean false, should be true")
	}

}

func TestQuarantineObject(t *testing.T) {
	// Quarantine an existing object
	name := []byte("bf43763a98208f15da803e76bf52e7d11515750803.01357#0#d.data")
	objName := &pb.ObjectName{name, false}
	_, err := client.QuarantineObject(context.Background(), objName)
	if err != nil {
		t.Fatal("Failed to quarantine object")
	}
	// We shouldn't be able to find it
	objInfo := &pb.LoadObjectInfo{name, false, false}
	_, err = client.LoadObject(context.Background(), objInfo)
	if err != nil {
		grpcStatus, ok := status.FromError(err)
		if !ok {
			t.Fatal("failed to convert error to grpc status")
		}
		if grpcStatus.Code() != codes.NotFound {
			t.Fatalf("Getting quarantined object, expected NotFound, got: %s", err)
		}
	} else {
		t.Fatal("Quarantined object can still be found")
	}

	// TODO, need to test that the quarantined object exists
	// then try to quarantine non existent object
}

func TestUnquarantineObject(t *testing.T) {
	// Unuarantine an existing quarantined object (check that)
	name := []byte("bf43763a98208f15da803e76bf52e7d11515750803.01357#0#d.data")
	objName := &pb.ObjectName{name, false}
	_, err := client.UnquarantineObject(context.Background(), objName)
	if err != nil {
		t.Fatal("Failed to quarantine object")
	}
	// We should be able to find it
	objInfo := &pb.LoadObjectInfo{name, false, false}
	_, err = client.LoadObject(context.Background(), objInfo)
	if err != nil {
		t.Fatal("cannot find unquarantined object")
	}

	// TODO, need to test that the quarantined object exists
	// then try to quarantine non existent object
}

// This test modifies the DB
func TestListQuarantinedOHashes(t *testing.T) {
	nextEntry := &pb.ListQuarantinedOHashesInfo{[]byte("")}
	qList, err := client.ListQuarantinedOHashes(context.Background(), nextEntry)
	if err != nil {
		t.Fatalf("failed to list quarantined objects: %s", err)
	}
	if len(qList.Entry) != 0 {
		t.Fatalf("expected no quarantined objects, got %d", len(qList.Entry))
	}
	if qList.LastEntry != "" {
		t.Fatalf("expected LastEntry to be \"\", got: %s", qList.LastEntry)
	}

	objectsToQuarantine := []pb.ObjectName{
		{[]byte("02573d31b770cda8e0effd7762e8a0751515750801.09785#2#d.data"), false},
		{[]byte("6b08eabf5667557c72dc6570aa1fb8451515750801.08639#4#d.data"), false},
		{[]byte("6b08eabf5667557c72dc6570aa1fb8451515750856.77219.meta"), false},
		{[]byte("6b08eabf5667557c72dc6570abcfb8451515643210.72429#4#d.data"), false},
		{[]byte("687ba0410f4323c66397a85292077b101515750801.10244#0#d.data"), false},
		{[]byte("c4aaea9b28c425f45eb64d4d5b0b3f621515750801.19478#2#d.data"), false},
		{[]byte("0a0898eb861579d1240adbb1c9f0c92b1515750801.20636#2#d.data"), false},
	}

	expectedOhashes := []string{
		"02573d31b770cda8e0effd7762e8a075",
		"0a0898eb861579d1240adbb1c9f0c92b",
		"687ba0410f4323c66397a85292077b10",
		"6b08eabf5667557c72dc6570aa1fb845",
		"6b08eabf5667557c72dc6570abcfb845",
		"c4aaea9b28c425f45eb64d4d5b0b3f62",
	}

	for _, qObj := range objectsToQuarantine {
		if _, err := client.QuarantineObject(context.Background(), &qObj); err != nil {
			t.Fatalf("failed to quarantine object: %s", err)
		}
	}

	qList, err = client.ListQuarantinedOHashes(context.Background(), nextEntry)
	if err != nil {
		t.Fatalf("failed to list quarantined objects: %s", err)
	}

	if len(qList.Entry) != len(expectedOhashes) {
		t.Fatalf("expected %d quarantined objects, got %d", len(expectedOhashes), len(qList.Entry))
	}

	if !testEqSliceString(qList.Entry, expectedOhashes) {
		t.Fatalf("\nexpected %v\ngot      %v", expectedOhashes, qList.Entry)
	}

}

func TestListQuarantinedOHash(t *testing.T) {
	ohash := pb.ObjectPrefix{[]byte("6b08eabf5667557c72dc6570aa1fb845"), false}
	qList, err := client.ListQuarantinedOHash(context.Background(), &ohash)
	if err != nil {
		t.Fatalf("error listing quarantined object files: %s", err)
	}

	expectedFiles := []string{
		"6b08eabf5667557c72dc6570aa1fb8451515750801.08639#4#d.data",
		"6b08eabf5667557c72dc6570aa1fb8451515750856.77219.meta",
	}

	if len(qList.Objects) != len(expectedFiles) {
		t.Fatalf("got %d objects, expected %d", len(qList.Objects), len(expectedFiles))
	}

	objNames := make([]string, len(qList.Objects))
	for i, obj := range qList.Objects {
		objNames[i] = string(obj.Name)
	}

	if !testEqSliceString(objNames, expectedFiles) {
		t.Fatalf("\nexpected %v\ngot      %v", expectedFiles, objNames)
	}

	// Add test, non existent ohash
	// Add test, unquarantine one file, list again
}

func testEqSliceString(a, b []string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// func (s * server) ListQuarantinedOHashes(ctx context.Context, in *pb.ListQuarantinedOHashesInfo) (*pb.QuarantinedObjects, error) {
