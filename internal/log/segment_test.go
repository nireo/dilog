package log

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/nireo/dilog/api/v1"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entryWidth * 3

	s, err := newSegment(dir, 16, c)
	if err != nil {
		t.Fatal(err)
	}

	if uint64(16) != s.nextOffset {
		t.Error("values not equal")
	}

	if s.IsMaxed() {
		t.Error("is maxed even though it shouldn't be")
	}

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		if err != nil {
			t.Error(err)
		}

		if (16 + i) != off {
			t.Error("offset is wrong")
		}

		got, err := s.Read(off)
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(want.Value, got.Value) {
			t.Error("values are not equal")
		}
	}

	_, err = s.Append(want)
	if err != io.EOF {
		t.Error("error should be io.EOF")
	}

	if !s.IsMaxed() {
		t.Error("not maxed")
	}

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	if err != nil {
		t.Error(err)
	}

	if !s.IsMaxed() {
		t.Error("not maxed")
	}

	err = s.Remove()
	if err != nil {
		t.Error("remove failed")
	}

	s, err = newSegment(dir, 16, c)
	if err != nil {
		t.Error(err)
	}

	if s.IsMaxed() {
		t.Error("is maxed")
	}
}
