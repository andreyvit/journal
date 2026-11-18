package journal

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestTailMissingCommittedHistoryFailsWithoutRetryLoop(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name := "empty_segment"
		if remove {
			name = "missing_segment"
		}
		t.Run(name, func(t *testing.T) {
			j := New(t.TempDir(), Options{SyncOnCommit: true})
			t.Cleanup(func() {
				if err := j.AbortWriting(); err != nil {
					t.Error(err)
				}
			})
			if err := j.WriteRecord(1, []byte("committed")); err != nil {
				t.Fatal(err)
			}
			if err := j.Commit(); err != nil {
				t.Fatal(err)
			}
			segment, err := j.lastSegment()
			if err != nil {
				t.Fatal(err)
			}
			path := j.filePath(segment.fileName(j))
			if remove {
				err = os.Remove(path)
			} else {
				err = os.Truncate(path, segmentHeaderSize)
			}
			if err != nil {
				t.Fatal(err)
			}
			tail := j.Tail(1)
			defer tail.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if tail.Next(ctx) || !errors.Is(tail.Err(), ErrTailGap) {
				t.Fatalf("missing history: %v", tail.Err())
			}
		})
	}
}

func TestCursorKeepsOriginalReadError(t *testing.T) {
	var (
		failure = errors.New("read failed")
		cursor  = &Cursor{Record: Record{ID: 1}, filter: Filter{MaxRecordID: 1}, err: failure}
	)
	if cursor.Next() || !errors.Is(cursor.Err(), failure) {
		t.Fatalf("read error masked: %v", cursor.Err())
	}
}
