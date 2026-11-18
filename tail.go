package journal

import (
	"context"
	"errors"
	"io"
	"sync"
)

var ErrTailGap = errors.New("journal tail history gap")

func (j *Journal) notifyReaders() {
	j.state.lock.Lock()
	defer j.state.lock.Unlock()
	j.notifyReadersLocked()
}

func (j *Journal) notifyReadersLocked() {
	close(j.changed)
	j.changed = make(chan struct{})
}

type TailCursor struct {
	Record
	mu        sync.Mutex
	closeOnce sync.Once
	done      chan struct{}
	j         *Journal
	nextID    uint64
	cursor    *Cursor
	head      *readHead
	err       error
}

func (j *Journal) Tail(minRecordID uint64) *TailCursor {
	minRecordID = max(minRecordID, 1)
	head, err := j.newReadHead(minRecordID)
	return &TailCursor{j: j, nextID: minRecordID, done: make(chan struct{}), head: head, err: err}
}

func (c *TailCursor) Close() {
	c.closeOnce.Do(func() { close(c.done) })
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeCursor()
	c.head.close()
	c.head = nil
}

func (c *TailCursor) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == io.EOF {
		return nil
	}
	return c.err
}

func (c *TailCursor) closeCursor() {
	if c.cursor != nil {
		c.cursor.Close()
		c.cursor = nil
	}
}

func (c *TailCursor) Next(ctx context.Context) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return false
	}

	for {
		select {
		case <-c.done:
			c.closeCursor()
			c.err = io.EOF
			return false
		default:
		}
		if err := ctx.Err(); err != nil {
			c.closeCursor()
			c.err = err
			return false
		}
		if c.cursor != nil {
			if c.cursor.Next() {
				if c.cursor.ID != c.nextID {
					c.err = ErrTailGap
					c.closeCursor()
					return false
				}
				c.Record = c.cursor.Record
				c.head.advance(c.Record.ID, c.cursor.segment.segnum)
				c.nextID++
				return true
			}
			var (
				err           = c.cursor.Err()
				missingRecord = c.nextID <= c.cursor.filter.MaxRecordID
			)
			c.closeCursor()
			if err == nil && missingRecord {
				c.err = ErrTailGap
				return false
			}
			if err != nil {
				c.err = err
				return false
			}
		}
		c.j.state.lock.Lock()
		changed := c.j.changed
		c.j.state.lock.Unlock()
		summary, err := c.j.Summary()
		if err != nil {
			c.err = err
			return false
		}
		if first := summary.FirstRecord().ID; first > c.nextID {
			c.err = ErrTailGap
			return false
		}
		if c.nextID <= summary.LastCommitted.ID {
			c.cursor = c.j.Read(Filter{MinRecordID: c.nextID, MaxRecordID: summary.LastCommitted.ID})
			c.j.retention.mu.Lock()
			if c.cursor.head != nil {
				c.cursor.head.hidden = true
			}
			c.j.retention.mu.Unlock()
			continue
		}
		select {
		case <-ctx.Done():
			c.err = ctx.Err()
			return false
		case <-c.done:
			c.err = io.EOF
			return false
		case <-changed:
		}
	}
}
