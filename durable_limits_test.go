package journal

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestReadLimitDoesNotTruncateValidJournal(t *testing.T) {
	var (
		dir    = t.TempDir()
		writer = New(dir, Options{SyncOnCommit: true})
	)
	if err := writer.WriteRecord(1, []byte("larger than new limit")); err != nil {
		t.Fatal(err)
	}
	if err := writer.FinishWriting(); err != nil {
		t.Fatal(err)
	}
	limited := New(dir, Options{MaxRecordSize: 3})
	if _, err := limited.Summary(); !errors.Is(err, ErrRecordSize) {
		t.Fatalf("read limit: %v", err)
	}
	recovered := New(dir, Options{})
	summary, err := recovered.Summary()
	if err != nil || summary.LastCommitted.ID != 1 {
		t.Fatalf("valid data changed: %+v %v", summary, err)
	}
	if err = recovered.FinishWriting(); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedCursorStopsBeforeUncommittedTail(t *testing.T) {
	j := New(t.TempDir(), Options{})
	if err := j.WriteRecord(1, []byte("committed")); err != nil {
		t.Fatal(err)
	}
	if err := j.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := j.WriteRecord(2, []byte("uncommitted")); err != nil {
		t.Fatal(err)
	}
	cursor := j.Read(Filter{MaxRecordID: 1})
	defer cursor.Close()
	if !cursor.Next() || cursor.ID != 1 {
		t.Fatal("missing committed record")
	}
	if cursor.Next() || cursor.Err() != nil {
		t.Fatalf("read beyond committed bound: %v", cursor.Err())
	}
	if err := j.AbortWriting(); err != nil {
		t.Fatal(err)
	}
	if summary, err := j.Summary(); err != nil || summary.LastCommitted.ID != 1 {
		t.Fatalf("abort: %+v %v", summary, err)
	}
	if err := j.FinishWriting(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultCommitDoesNotSync(t *testing.T) {
	var (
		j     = New(t.TempDir(), Options{})
		calls int
	)
	j.syncFile = func(file *os.File) error { calls++; return file.Sync() }
	if err := j.WriteRecord(1, []byte("record")); err != nil {
		t.Fatal(err)
	}
	if err := j.Commit(); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("default commit unexpectedly synced")
	}
	if err := j.FinishWriting(); err != nil {
		t.Fatal(err)
	}
	if calls == 0 {
		t.Fatal("close did not sync")
	}
}

func TestTailWakesOnSyncFailure(t *testing.T) {
	var (
		j       = New(t.TempDir(), Options{SyncOnCommit: true})
		failure = errors.New("sync fault")
		result  = make(chan error, 1)
		tail    = j.Tail(1)
	)
	j.syncFile = func(*os.File) error { return failure }
	defer tail.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		if tail.Next(ctx) {
			result <- errors.New("failed record exposed")
			return
		}
		result <- tail.Err()
	}()
	if err := j.WriteRecord(1, []byte("record")); err != nil {
		t.Fatal(err)
	}
	if err := j.Commit(); !errors.Is(err, failure) {
		t.Fatalf("commit: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, failure) {
			t.Fatalf("tail failure: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := j.FinishWriting(); !errors.Is(err, failure) {
		t.Fatalf("finish masked fault: %v", err)
	}
}
