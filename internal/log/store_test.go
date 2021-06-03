package log

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		if err != nil {
			t.Error(err)
		}

		if (pos + n) != (width * i) {
			t.Error("append not equal")
		}
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64

	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(read, write) {
			t.Error("values are not equal")
		}

		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		if err != nil {
			t.Error(err)
		}

		if n != lenWidth {
			t.Error("not equal widths")
		}
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(write, b) {
			t.Error("values are not equal")
		}

		if int(size) != n {
			t.Error("values are not equal")
		}
		off += int64(n)
	}
}

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	if err != nil {
		t.Fatalf("could not create temp file: %s", err)
	}
	defer os.Remove(f.Name())

	s, err := newStore(f)
	if err != nil {
		t.Fatalf("could not create store: %s", err)
	}

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	if err != nil {
		t.Fatalf("could not open store again")
	}
	testRead(t, s)
}

func TestStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	if err != nil {
		t.Fatalf("could not create temp file: %s", err)
	}
	defer os.Remove(f.Name())

	s, err := newStore(f)
	if err != nil {
		t.Fatalf("could not create store: %s", err)
	}

	_, _, err = s.Append(write)
	if err != nil {
		t.Error(err)
	}

	f, beforeSize, err := openFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, afterSize, err := openFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	if !(afterSize > beforeSize) {
		t.Error("after size is less than before size")
	}
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, fi.Size(), nil
}
