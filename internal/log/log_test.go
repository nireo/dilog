package log

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/nireo/dilog/api/v1"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncaate":                         testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			if err != nil {
				t.Fatal(err)
			}

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	append := &api.Record{Value: []byte("hello world")}
	off, err := log.Append(append)
	if err != nil {
		t.Error(err)
	}

	if uint64(0) != off {
		t.Error("offset not 0")
	}

	read, err := log.Read(off)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(append.Value, read.Value) {
		t.Error("values not equal")
	}
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	if err == nil {
		t.Error("error should not be nil")
	}

	if read != nil {
		t.Error("read should be nil")
	}

	apiErr := err.(api.ErrOffsetOutOfRange)
	if uint64(1) != apiErr.Offset {
		t.Error("offsets don't match")
	}
}

func testInitExisting(t *testing.T, o *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := o.Append(append)
		if err != nil {
			t.Error(err)
		}
	}

	if err := o.Close(); err != nil {
		t.Fatal(err)
	}

	off, err := o.LowestOffset()
	if err != nil {
		t.Error(err)
	}

	if uint64(0) != off {
		t.Error("offset is not 0")
	}

	off, err = o.HighestOffset()
	if err != nil {
		t.Error(err)
	}

	if uint64(2) != off {
		t.Error("offset is not 0")
	}

	n, err := NewLog(o.Dir, o.Config)
	if err != nil {
		t.Error(err)
	}

	off, err = n.LowestOffset()
	if err != nil {
		t.Error(err)
	}

	if uint64(0) != off {
		t.Error("offset is not 0")
	}

	off, err = n.HighestOffset()
	if err != nil {
		t.Error(err)
	}

	if uint64(2) != off {
		t.Error("offset is not 0")
	}
}

func testReader(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	off, err := log.Append(append)
	if err != nil {
		t.Error(err)
	}

	if uint64(0) != off {
		t.Error("offset is not 0")
	}

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Error(err)
	}

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(append.Value, read.Value) {
		t.Error("byte values not equal")
	}
}

func testTruncate(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		if err != nil {
			t.Error(err)
		}
	}

	err := log.Truncate(1)
	if err != nil {
		t.Error(err)
	}

	_, err = log.Read(0)
	if err == nil {
		t.Error("could read value after truncating")
	}
}
