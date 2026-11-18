package journal

import (
	"context"
	"errors"
	"maps"
	"testing"
)

func TestInspectSegmentsCommittedHeadsAndNoSideEffects(t *testing.T) {
	j := New(t.TempDir(), Options{SyncOnCommit: true, UpdateSegmentMetadata: inspectionTestMetadata})
	t.Cleanup(func() { j.AbortWriting() })
	inspectionTestWrite(t, j, 101, true)
	inspectionTestWrite(t, j, 303, false)
	infos, err := j.InspectSegments()
	if err != nil || len(infos) != 1 {
		t.Fatalf("inspection: %+v %v", infos, err)
	}
	if info := infos[0]; info.Number != 1 || info.Records != 1 || info.Writers != 1 || info.Metadata != (SegmentMetadata{101 % 256}) || info.Readers != 0 {
		t.Fatalf("uncommitted state exposed: %+v", info)
	}
	if err = j.Rotate(); err != nil {
		t.Fatal(err)
	}
	inspectionTestWrite(t, j, 505, true)
	tail := j.Tail(2)
	defer tail.Close()
	if !tail.Next(context.Background()) {
		t.Fatal(tail.Err())
	}
	j.retention.mu.Lock()
	accessed := maps.Clone(j.retention.accessed)
	j.retention.mu.Unlock()
	infos, err = j.InspectSegments()
	if err != nil || len(infos) != 2 {
		t.Fatalf("inspection: %+v %v", infos, err)
	}
	if info := infos[0]; info.Status != Finalized || info.Writers != 0 || info.Readers != 1 || !info.Pinned || info.Records != 2 || info.Metadata != (SegmentMetadata{303 % 256}) {
		t.Fatalf("closed file: %+v", info)
	}
	if info := infos[1]; info.Writers != 1 || info.Readers != 0 || !info.Pinned || info.Records != 1 || info.Metadata != (SegmentMetadata{505 % 256}) {
		t.Fatalf("active file: %+v", info)
	}
	j.retention.mu.Lock()
	unchanged := maps.Equal(accessed, j.retention.accessed)
	j.retention.mu.Unlock()
	if !unchanged {
		t.Fatal("inspection refreshed access grace")
	}
	if err = j.FinishWriting(); err != nil {
		t.Fatal(err)
	}
	infos, err = j.InspectSegments()
	if err != nil || infos[1].Writers != 0 || infos[1].Records != 1 || infos[1].Metadata != (SegmentMetadata{505 % 256}) {
		t.Fatalf("closed draft: %+v %v", infos, err)
	}
}

func TestInspectSegmentsBusyAndUninitialized(t *testing.T) {
	j := New(t.TempDir(), Options{})
	if _, err := j.InspectSegments(); err == nil {
		t.Fatal("uninitialized inspection changed journal state")
	}
	j.writer.writeLock.Lock()
	_, err := j.InspectSegments()
	j.writer.writeLock.Unlock()
	if !errors.Is(err, ErrInspectionBusy) {
		t.Fatalf("writer busy: %v", err)
	}
	j.state.lock.Lock()
	_, err = j.InspectSegments()
	j.state.lock.Unlock()
	if !errors.Is(err, ErrInspectionBusy) {
		t.Fatalf("state busy: %v", err)
	}
	j.retention.mu.Lock()
	_, err = j.InspectSegments()
	j.retention.mu.Unlock()
	if !errors.Is(err, ErrInspectionBusy) {
		t.Fatalf("retention busy: %v", err)
	}
}

func inspectionTestMetadata(_ SegmentMetadata, data []byte) (SegmentMetadata, error) {
	return SegmentMetadata{data[0]}, nil
}
func inspectionTestWrite(t *testing.T, j *Journal, value int, commit bool) {
	t.Helper()
	if err := j.WriteRecord(1, []byte{byte(value)}); err != nil {
		t.Fatal(err)
	}
	if commit {
		if err := j.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}
