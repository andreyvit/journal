package journal

import (
	"errors"
	"io"
	"iter"
	"os"
	"sync"
	"time"
)

var ErrInternal = errors.New("journal internal error")

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

	mu       sync.Mutex
	head     *readHead
	closed   bool
	j        *Journal
	filter   Filter
	err      error
	segments []Segment
	segment  Segment
	file     *os.File
	reader   *segmentReader
}

func (j *Journal) Read(filter Filter) *Cursor {
	head, err := j.newReadHead(filter.MinRecordID)
	if head != nil && filter.MinRecordID == 0 {
		filter.MinRecordID = head.recordID
	}
	return &Cursor{
		head:   head,
		err:    err,
		j:      j,
		filter: filter,
		file:   nil,
		reader: nil,
	}
}

func (c *Cursor) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	c.closeFile()
	c.head.close()
	c.head = nil
}

func (c *Cursor) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == io.EOF {
		return nil
	}
	return c.err
}

func (c *Cursor) Next() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.err != nil {
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

func (j *Journal) segmentForRead(segment Segment) (Segment, error) {
	j.writer.writeLock.Lock()
	defer j.writer.writeLock.Unlock()
	if j.writer.writeErr != nil {
		return Segment{}, j.writer.writeErr
	}
	segments, err := j.FindSegments(Filter{MinRecordID: segment.recnum, MaxRecordID: segment.recnum})
	if err != nil {
		return Segment{}, err
	}
	for _, current := range segments {
		if current.segnum == segment.segnum {
			return current, nil
		}
	}
	return Segment{}, ErrTailGap
}

func (c *Cursor) next() error {
	var lim uint64
	if c.filter.Limit > 0 {
		lim = uint64(c.filter.Limit)
	}

	if c.segments == nil {
		if lim > 0 {
			sum, err := c.j.Summary()
			if err != nil {
				return err
			}
			if c.filter.Latest {
				if sum.LastCommitted.ID >= lim {
					c.filter.MinRecordID = max(c.filter.MinRecordID, sum.LastCommitted.ID-lim+1)
				}
			} else {
				first := max(sum.FirstRecord().ID, c.filter.MinRecordID)
				c.filter.MinRecordID = first
				c.filter.MaxRecordID = first + lim - 1
			}
		}

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
			if len(c.segments) > 1 && c.segments[1].recnum <= c.filter.MinRecordID {
				c.segments = c.segments[1:]
				continue
			}
			seg := c.segments[0]
			c.segment = seg
			c.segments = c.segments[1:]

			var err error
			c.file, c.reader, err = openSegment(c.j, seg)
			if err == errFileGone {
				seg, err = c.j.segmentForRead(seg)
				if err == nil {
					c.segment = seg
					c.file, c.reader, err = openSegment(c.j, seg)
				}
				if err == errFileGone {
					err = ErrTailGap
				}
			}
			if err != nil {
				return err
			}
		}

		if c.filter.MaxRecordID != 0 && c.reader.rec >= c.filter.MaxRecordID {
			if c.j.updateSegmentMetadata != nil && !c.segment.status.IsDraft() && c.reader.rec == c.reader.h.LastRecordNumber {
				err := c.reader.next()
				if err != io.EOF {
					if err == nil {
						return ErrInternal
					}
					return err
				}
			}
			return io.EOF
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
		c.head.advance(c.Record.ID, c.segment.segnum)
		if c.filter.MinRecordID != 0 && c.Record.ID < c.filter.MinRecordID {
			continue
		}
		if c.filter.MaxRecordID != 0 && c.Record.ID > c.filter.MaxRecordID {
			continue
		}
		if c.filter.MinTimestamp != 0 && c.Record.Timestamp < c.filter.MinTimestamp {
			continue
		}
		if c.filter.MaxTimestamp != 0 && c.Record.Timestamp > c.filter.MaxTimestamp {
			continue
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
