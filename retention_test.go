package journal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type retentionClock struct {
	mu    sync.Mutex
	value time.Time
}

func (c *retentionClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

func (c *retentionClock) advance(delta time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = c.value.Add(delta)
}

func retentionTestJournal(t *testing.T, records int) (*Journal, *retentionClock) {
	t.Helper()
	var (
		clock = &retentionClock{value: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
		j     = New(t.TempDir(), Options{Now: clock.now, SyncOnCommit: true, MaxSegmentRecords: 1})
	)
	t.Cleanup(func() {
		if err := j.FinishWriting(); err != nil {
			t.Error(err)
		}
	})
	for i := range records {
		if err := j.WriteRecord(0, []byte(fmt.Sprint(i+1))); err != nil {
			t.Fatal(err)
		}
		if err := j.Commit(); err != nil {
			t.Fatal(err)
		}
		clock.advance(time.Second)
	}
	return j, clock
}

func TestPrunePinsUnreadRecordsAndHonorsAccessGrace(t *testing.T) {
	var (
		j, clock = retentionTestJournal(t, 3)
		cursor   = j.Read(Filter{MinRecordID: 1})
		opt      = RetentionOptions{TTL: time.Hour, IdleTTL: time.Minute}
	)
	defer cursor.Close()
	clock.advance(2 * time.Hour)
	result, err := j.Prune(opt, nil)
	if err != nil || result.Deleted != 0 || !result.Blocked {
		t.Fatalf("unread pin: %+v %v", result, err)
	}
	if !cursor.Next() || cursor.ID != 1 {
		t.Fatal("first record missing")
	}
	if !cursor.Next() || cursor.ID != 2 {
		t.Fatal("second record missing")
	}
	result, err = j.Prune(opt, nil)
	if err != nil || result.Deleted != 0 || !result.Blocked {
		t.Fatalf("access grace: %+v %v", result, err)
	}
	clock.advance(2 * time.Minute)
	result, err = j.Prune(opt, func(boundary PruneBoundary) error {
		if boundary.LastRecord.ID != 1 || string(boundary.LastRecord.Data) != "1" {
			t.Fatalf("boundary: %+v", boundary)
		}
		return nil
	})
	if err != nil || result.Deleted != 1 || result.Through != 1 || !result.Blocked {
		t.Fatalf("current record pin: %+v %v", result, err)
	}
	if !cursor.Next() || cursor.ID != 3 {
		t.Fatalf("reader lost unread record: %v", cursor.Err())
	}
	result, err = j.Prune(opt, nil)
	if err != nil || result.Deleted != 1 || result.Through != 2 {
		t.Fatalf("old segment not released: %+v %v", result, err)
	}
	segments, err := j.FindSegments(Filter{})
	if err != nil || len(segments) != 1 {
		t.Fatalf("newest segment not retained: %v %v", segments, err)
	}
	old := j.Tail(1)
	defer old.Close()
	if old.Next(context.Background()) || !errors.Is(old.Err(), ErrTailGap) {
		t.Fatalf("expired history exposed: %v", old.Err())
	}
}

func TestPruneAuthorizationFailureDeletesNothing(t *testing.T) {
	var (
		j, clock = retentionTestJournal(t, 3)
		failure  = errors.New("checkpoint sync failed")
	)
	clock.advance(time.Hour)
	result, err := j.Prune(RetentionOptions{MaxFiles: 1}, func(PruneBoundary) error {
		reader := j.Read(Filter{})
		defer reader.Close()
		if !errors.Is(reader.Err(), ErrTailGap) {
			t.Errorf("default read skipped an uncommitted prune plan: %v", reader.Err())
		}
		return failure
	})
	if !errors.Is(err, failure) || result.Deleted != 0 {
		t.Fatalf("authorization failure: %+v %v", result, err)
	}
	cursor := j.Read(Filter{MinRecordID: 1})
	if !cursor.Next() || cursor.ID != 1 {
		t.Fatalf("failed prune advanced floor: %v", cursor.Err())
	}
	cursor.Close()
	result, err = j.Prune(RetentionOptions{MaxFiles: 1}, nil)
	if err != nil || result.Deleted != 2 || result.Through != 2 {
		t.Fatalf("retry: %+v %v", result, err)
	}
}

func TestTailPinSurvivesEndOfCommittedWindow(t *testing.T) {
	var (
		j, clock = retentionTestJournal(t, 2)
		tail     = j.Tail(1)
	)
	if !tail.Next(context.Background()) || !tail.Next(context.Background()) {
		t.Fatal(tail.Err())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if tail.Next(ctx) || !errors.Is(tail.Err(), context.DeadlineExceeded) {
		t.Fatalf("tail wait: %v", tail.Err())
	}
	if err := j.WriteRecord(0, []byte("3")); err != nil {
		t.Fatal(err)
	}
	if err := j.Commit(); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Hour)
	result, err := j.Prune(RetentionOptions{MaxFiles: 1}, nil)
	if err != nil || result.Deleted != 1 || !result.Blocked {
		t.Fatalf("tail pin lost between windows: %+v %v", result, err)
	}
	tail.Close()
	result, err = j.Prune(RetentionOptions{MaxFiles: 1}, nil)
	if err != nil || result.Deleted != 1 {
		t.Fatalf("closed tail pin leaked: %+v %v", result, err)
	}
}

func TestRecordCountRotationAndDisabledRetention(t *testing.T) {
	j := New(t.TempDir(), Options{MaxSegmentRecords: 2})
	for range 5 {
		if err := j.WriteRecord(1, []byte("data")); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.FinishWriting(); err != nil {
		t.Fatal(err)
	}
	segments, err := j.FindSegments(Filter{})
	if err != nil || len(segments) != 3 {
		t.Fatalf("record limit: %v %v", segments, err)
	}
	result, err := j.Prune(RetentionOptions{}, nil)
	if err != nil || result.Deleted != 0 {
		t.Fatalf("disabled retention: %+v %v", result, err)
	}
}
