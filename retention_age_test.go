package journal

import (
	"testing"
	"time"
)

func TestPruneTTLUsesLastRecordTimestamp(t *testing.T) {
	var (
		clock = &retentionClock{value: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
		j     = New(t.TempDir(), Options{Now: clock.now, SyncOnCommit: true, MaxSegmentRecords: 2})
	)
	t.Cleanup(func() {
		if err := j.FinishWriting(); err != nil {
			t.Error(err)
		}
	})
	if err := j.WriteRecord(0, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := j.Commit(); err != nil {
		t.Fatal(err)
	}
	clock.advance(50 * time.Minute)
	if err := j.WriteRecord(0, []byte("last in first file")); err != nil {
		t.Fatal(err)
	}
	if err := j.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := j.WriteRecord(0, []byte("new file")); err != nil {
		t.Fatal(err)
	}
	if err := j.Commit(); err != nil {
		t.Fatal(err)
	}
	clock.advance(20 * time.Minute)
	result, err := j.Prune(RetentionOptions{TTL: time.Hour}, nil)
	if err != nil || result.Deleted != 0 {
		t.Fatalf("expired by first rather than last record: %+v %v", result, err)
	}
	clock.advance(41 * time.Minute)
	result, err = j.Prune(RetentionOptions{TTL: time.Hour}, nil)
	if err != nil || result.Deleted != 1 || result.Through != 2 {
		t.Fatalf("last-record TTL did not expire: %+v %v", result, err)
	}
}
