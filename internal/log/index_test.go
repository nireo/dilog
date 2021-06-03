package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = idx.Read(-1)
	if err == nil {
		t.Fatal(err)
	}

	if f.Name() != idx.Name() {
		t.Error("file names are not equal")
	}

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		if err != nil {
			t.Errorf("error writing: %s", err)
		}

		_, pos, err := idx.Read(int64(want.Off))
		if err != nil {
			t.Errorf("error reading: %s", err)
		}

		if want.Pos != pos {
			t.Error("positions not the same")
		}
	}

	_, _, err = idx.Read(int64(len(entries)))
	if err != io.EOF {
		t.Errorf("error reading: %s", err)
	}

	_ = idx.Close()

	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	if err != nil {
		t.Error(err)
	}

	off, pos, err := idx.Read(-1)
	if err != nil {
		t.Error(err)
	}

	if uint32(1) != off {
		t.Error("offset is wrong")
	}

	if entries[1].Pos != pos {
		t.Error("positions not equal")
	}
}
