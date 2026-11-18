package journal

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestSyncOnCommitBeforePublishingHead(t *testing.T) {
	var (
		j     = New(t.TempDir(), Options{SyncOnCommit: true})
		files int
		dirs  int
	)
	j.syncFile = func(file *os.File) error {
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if info.IsDir() {
			dirs++
		} else {
			files++
		}
		j.state.lock.Lock()
		id := j.state.lastCommitted.ID
		j.state.lock.Unlock()
		if id != 0 {
			t.Error("head published before sync")
		}
		return file.Sync()
	}
	if err := j.WriteRecord(1, []byte("record")); err != nil {
		t.Fatal(err)
	}
	if err := j.Commit(); err != nil {
		t.Fatal(err)
	}
	if files != 1 || dirs != 1 {
		t.Fatalf("sync calls: files=%d dirs=%d", files, dirs)
	}
	summary, err := j.Summary()
	if err != nil || summary.LastCommitted.ID != 1 {
		t.Fatalf("summary: %+v %v", summary, err)
	}
	j.syncFile = (*os.File).Sync
	if err = j.FinishWriting(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncFailureIsSticky(t *testing.T) {
	for _, failDir := range []bool{false, true} {
		t.Run(map[bool]string{false: "file", true: "directory"}[failDir], func(t *testing.T) {
			var (
				j       = New(t.TempDir(), Options{SyncOnCommit: true})
				failure = errors.New("sync fault")
			)
			j.syncFile = func(file *os.File) error {
				info, err := file.Stat()
				if err != nil {
					return err
				}
				if info.IsDir() == failDir {
					return failure
				}
				return file.Sync()
			}
			if err := j.WriteRecord(1, []byte("record")); err != nil {
				t.Fatal(err)
			}
			if err := j.Commit(); !errors.Is(err, failure) {
				t.Fatalf("commit: %v", err)
			}
			if err := j.Commit(); !errors.Is(err, failure) {
				t.Fatalf("repeated commit: %v", err)
			}
			if err := j.WriteRecord(2, []byte("later")); !errors.Is(err, failure) {
				t.Fatalf("write: %v", err)
			}
		})
	}
}

func TestTailWaitsForCommitAndFollowsRotation(t *testing.T) {
	var (
		j      = New(t.TempDir(), Options{SyncOnCommit: true, MaxFileSize: 180})
		tail   = j.Tail(1)
		got    = make(chan Record, 2)
		result = make(chan error, 1)
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer tail.Close()
	go func() {
		for range 2 {
			if !tail.Next(ctx) {
				result <- tail.Err()
				return
			}
			record := tail.Record
			record.Data = append([]byte(nil), record.Data...)
			got <- record
		}
		result <- nil
	}()
	if err := j.WriteRecord(1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
		t.Fatal("uncommitted record exposed")
	case <-time.After(20 * time.Millisecond):
	}
	if err := j.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case record := <-got:
		if record.ID != 1 || string(record.Data) != "first" {
			t.Fatalf("record: %+v", record)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := j.Rotate(); err != nil {
		t.Fatal(err)
	}
	if err := j.WriteRecord(2, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := j.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case record := <-got:
		if record.ID != 2 || string(record.Data) != "second" {
			t.Fatalf("record: %+v", record)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := j.FinishWriting(); err != nil {
		t.Fatal(err)
	}
}

func TestTailCloseAndContext(t *testing.T) {
	var (
		j    = New(t.TempDir(), Options{})
		tail = j.Tail(1)
		done = make(chan bool, 1)
	)
	go func() { done <- tail.Next(context.Background()) }()
	tail.Close()
	select {
	case next := <-done:
		if next || tail.Err() != nil {
			t.Fatal("closed tail returned a record or error")
		}
	case <-time.After(time.Second):
		t.Fatal("tail close blocked")
	}
	tail = j.Tail(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if tail.Next(ctx) || !errors.Is(tail.Err(), context.Canceled) {
		t.Fatal("context not honored")
	}
	tail.Close()
}

func TestRecordSizeLimit(t *testing.T) {
	j := New(t.TempDir(), Options{MaxRecordSize: 3})
	if err := j.WriteRecord(1, []byte("four")); err == nil {
		t.Fatal("oversized record accepted")
	}
	if err := j.WriteRecord(1, []byte("yes")); err != nil {
		t.Fatal(err)
	}
	if err := j.FinishWriting(); err != nil {
		t.Fatal(err)
	}
	cursor := j.Read(Filter{})
	defer cursor.Close()
	if !cursor.Next() || string(cursor.Data) != "yes" {
		t.Fatal("size rejection poisoned journal")
	}
}
