package server

import (
	"errors"
	"sync"
)

var (
	ErrOffsetNotFound = errors.New("offset not found")
)

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

type Log struct {
	sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

// return the offset
func (l *Log) Append(rec Record) (uint64, error) {
	l.Lock()
	defer l.Unlock()

	rec.Offset = uint64(len(l.records))
	l.records = append(l.records, rec)

	return rec.Offset, nil
}

func (l *Log) Read(offset uint64) (Record, error) {
	l.Lock()
	defer l.Unlock()
	if offset >= uint64(len(l.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return l.records[offset], nil
}
