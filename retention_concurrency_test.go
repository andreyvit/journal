package journal

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPruneDoesNotHoldReaderAdmissionLockDuringCheckpoint(t *testing.T) {
	var (
		j, _    = retentionTestJournal(t, 3)
		entered = make(chan struct{})
		resume  = make(chan struct{})
		release = sync.OnceFunc(func() { close(resume) })
		result  = make(chan error, 1)
	)
	defer release()
	go func() {
		_, err := j.Prune(RetentionOptions{MaxFiles: 1}, func(PruneBoundary) error {
			close(entered)
			<-resume
			return nil
		})
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint callback did not start")
	}
	admitted := make(chan error, 1)
	go func() {
		cursor := j.Read(Filter{MinRecordID: 3, MaxRecordID: 3})
		defer cursor.Close()
		if !cursor.Next() || cursor.ID != 3 {
			admitted <- errors.New("retained reader blocked or lost data")
			return
		}
		stale := j.Read(Filter{MinRecordID: 1})
		defer stale.Close()
		if !errors.Is(stale.Err(), ErrTailGap) {
			admitted <- errors.New("pruning prefix admitted a new reader")
			return
		}
		admitted <- nil
	}()
	select {
	case err := <-admitted:
		release()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("retention state lock held during checkpoint I/O")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
