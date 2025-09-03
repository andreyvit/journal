package journal

import (
	"container/heap"
	"iter"
)

type RecordWithSource struct {
	Record
	Source uint64
}

type cursorItem struct {
	source uint64
	cursor *Cursor
	index  int // heap index
}

type cursorHeap []*cursorItem

func (h cursorHeap) Len() int { return len(h) }

func (h cursorHeap) Less(i, j int) bool {
	aRec := &h[i].cursor.Record
	bRec := &h[j].cursor.Record

	if aRec.Timestamp != bRec.Timestamp {
		return aRec.Timestamp < bRec.Timestamp
	}

	if h[i].source != h[j].source {
		return h[i].source < h[j].source
	}

	return aRec.ID < bRec.ID
}

func (h cursorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *cursorHeap) Push(x any) {
	n := len(*h)
	item := x.(*cursorItem)
	item.index = n
	*h = append(*h, item)
}

func (h *cursorHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// MergedRecords returns an iterator that merges records from multiple journals,
// sorted by timestamp, then by source (for stability), then by record ID.
func MergedRecords(journals map[uint64]*Journal, filter Filter, fail func(error)) iter.Seq[RecordWithSource] {
	return func(yield func(RecordWithSource) bool) {
		h := make(cursorHeap, 0, len(journals))
		heap.Init(&h)

		for source, j := range journals {
			c := j.Read(filter)
			if c.Next() {
				item := &cursorItem{
					source: source,
					cursor: c,
				}
				heap.Push(&h, item)
			} else {
				c.Close()
				if err := c.Err(); err != nil {
					fail(err)
					return
				}
			}
		}

		for h.Len() > 0 {
			item := heap.Pop(&h).(*cursorItem)

			rec := RecordWithSource{
				Record: item.cursor.Record,
				Source: item.source,
			}

			continueIterating := yield(rec)

			if item.cursor.Next() {
				heap.Push(&h, item)
			} else {
				item.cursor.Close()
				if err := item.cursor.Err(); err != nil {
					fail(err)
				}
			}

			if !continueIterating {
				break
			}
		}

		for h.Len() > 0 {
			item := heap.Pop(&h).(*cursorItem)
			item.cursor.Close()
		}
	}
}
