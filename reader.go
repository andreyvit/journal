package journal

import (
	"errors"
	"io"
	"iter"
	"os"
	"slices"
	"strings"
	"time"
)

var ErrInternal = errors.New("journal: internal error")

type Filter struct {
	MinRecordID  uint64
	MinTimestamp uint64
}

type segmentInfo struct {
	name     string
	startTS  uint64
	startRec uint64
	segment  uint32
}

type Record struct {
	ID        uint64
	Timestamp uint64
	Data      []byte
}

func (rec *Record) Time() time.Time {
	return ToTime(rec.Timestamp)
}

type Cursor struct {
	Record

	closed           bool
	j                *Journal
	filter           Filter
	err              error
	segmentFileNames []string
	file             *os.File
	reader           *segmentReader
}

func (j *Journal) Read(filter Filter) *Cursor {
	return &Cursor{
		j:      j,
		filter: filter,
		file:   nil,
		reader: nil,
	}
}

func (j *Journal) FindSegments(filter Filter) ([]string, error) {
	dirf, err := os.Open(j.dir)
	if err != nil {
		return nil, err
	}
	defer dirf.Close()

	var names []string
	for {
		ents, err := dirf.ReadDir(16)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		for _, ent := range ents {
			if !ent.Type().IsRegular() {
				continue
			}
			name := ent.Name()
			if !strings.HasPrefix(name, j.fileNamePrefix) || !strings.HasSuffix(name, j.fileNameSuffix) {
				continue
			}

			_, ts, rec, err := parseSegmentName(j.fileNamePrefix, j.fileNameSuffix, name)
			if err != nil {
				return nil, err
			}

			if rec >= filter.MinRecordID && ts >= filter.MinTimestamp {
				names = append(names, name)
			}
		}
	}

	slices.Sort(names)
	return names, nil
}

func (c *Cursor) Close() {
	if c.closed {
		return
	}
	c.closed = true
	c.closeFile()
}

func (c *Cursor) Err() error {
	if c.err == io.EOF {
		return nil
	}
	return c.err
}

func (c *Cursor) Next() bool {
	if c.err != nil {
		return false
	}
	c.err = c.next()
	return c.err == nil
}

func (c *Cursor) closeFile() {
	if c.file != nil {
		c.file.Close()
		c.file = nil
		c.reader = nil
	}
}

func (c *Cursor) next() error {
	if c.segmentFileNames == nil {
		var err error
		c.segmentFileNames, err = c.j.FindSegments(c.filter)
		if err != nil {
			return err
		}
	}

	for {
		if c.reader == nil {
			if len(c.segmentFileNames) == 0 {
				return io.EOF
			}
			nextFileName := c.segmentFileNames[0]
			c.segmentFileNames = c.segmentFileNames[1:]

			var err error
			c.file, c.reader, err = openSegment(c.j, nextFileName)
			if err != nil {
				return err
			}
		}

		err := c.reader.next()
		if err == io.EOF {
			c.closeFile()
			continue
		} else if err != nil {
			c.closeFile()
			return err
		}
		c.Record = Record{
			ID:        c.reader.rec,
			Timestamp: c.reader.ts,
			Data:      c.reader.data,
		}
		return nil
	}
}

func (j *Journal) Records(filter Filter, fail func(error)) iter.Seq[Record] {
	return func(yield func(Record) bool) {
		c := j.Read(filter)
		defer c.Close()
		for c.Next() {
			cont := yield(c.Record)
			if !cont {
				break
			}
		}
		err := c.Err()
		if err != nil {
			fail(err)
		}
	}
}
