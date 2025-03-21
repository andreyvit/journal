package journal

import (
	"errors"
	"io"
	"iter"
	"os"
	"time"
)

var ErrInternal = errors.New("journal: internal error")

type Record struct {
	ID        uint64
	Timestamp uint64
	Data      []byte
}

func (rec *Record) Time() time.Time {
	return ToTime(rec.Timestamp)
}

type Meta struct {
	ID        uint64
	Timestamp uint64
}

func (meta Meta) IsZero() bool { return meta.ID == 0 && meta.Timestamp == 0 }

func (meta Meta) IsNonZero() bool { return !meta.IsZero() }

func (meta Meta) Time() time.Time { return ToTime(meta.Timestamp) }

type Cursor struct {
	Record

	closed   bool
	j        *Journal
	filter   Filter
	err      error
	segments []Segment
	file     *os.File
	reader   *segmentReader
}

func (j *Journal) Read(filter Filter) *Cursor {
	return &Cursor{
		j:      j,
		filter: filter,
		file:   nil,
		reader: nil,
	}
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
	if c.segments == nil {
		var err error
		c.segments, err = c.j.FindSegments(c.filter)
		if err != nil {
			return err
		}
	}

	for {
		if c.reader == nil {
			if len(c.segments) == 0 {
				return io.EOF
			}
			seg := c.segments[0]
			c.segments = c.segments[1:]

			var err error
			c.file, c.reader, err = openSegment(c.j, seg)
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
