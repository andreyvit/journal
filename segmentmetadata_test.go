package journal

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/andreyvit/sealer"
)

func TestSegmentMetadataIsNotAcknowledgedOnSyncFailure(t *testing.T) {
	var (
		j       = New(t.TempDir(), Options{SyncOnCommit: true, UpdateSegmentMetadata: inspectionTestMetadata})
		failure = errors.New("sync failure")
	)
	inspectionTestWrite(t, j, 101, true)
	segment, err := j.lastSegment()
	if err != nil {
		t.Fatal(err)
	}
	j.syncFile = func(*os.File) error { return failure }
	inspectionTestWrite(t, j, 202, false)
	if err := j.Commit(); !errors.Is(err, failure) {
		t.Fatalf("sync failure: %v", err)
	}
	if _, err := j.ReadSegmentMetadata(segment); !errors.Is(err, failure) {
		t.Fatalf("failed sync exposed a committed range: %v", err)
	}
}

func TestSegmentMetadataSurvivesSealing(t *testing.T) {
	var (
		key = &sealer.Key{ID: [sealer.IDSize]byte{1}, Key: [sealer.KeySize]byte{2}}
		j   = New(t.TempDir(), Options{SyncOnCommit: true, UpdateSegmentMetadata: inspectionTestMetadata, SealKeys: []*sealer.Key{key}})
	)
	t.Cleanup(func() {
		if err := j.FinishWriting(); err != nil {
			t.Error(err)
		}
	})
	inspectionTestWrite(t, j, 0, true)
	inspectionTestWrite(t, j, 101, true)
	inspectionTestWrite(t, j, 303, true)
	if err := j.Rotate(); err != nil {
		t.Fatal(err)
	}
	inspectionTestWrite(t, j, 505, true)
	sealed, err := j.Seal(context.Background())
	if err != nil || sealed.IsZero() {
		t.Fatalf("seal: %v %v", sealed, err)
	}
	r, err := j.ReadSegmentMetadata(sealed)
	if err != nil || r != (SegmentMetadata{303 % 256}) {
		t.Fatalf("sealed header lost range: %+v %v", r, err)
	}
	r, err = j.scanSegmentMetadata(sealed)
	if err != nil || r != (SegmentMetadata{303 % 256}) {
		t.Fatalf("sealed payload disagrees: %+v %v", r, err)
	}
}

func TestSegmentMetadataRejectionDoesNotAppend(t *testing.T) {
	var (
		failure = errors.New("bad record")
		j       = New(t.TempDir(), Options{UpdateSegmentMetadata: func(SegmentMetadata, []byte) (SegmentMetadata, error) { return SegmentMetadata{}, failure }})
	)
	if err := j.WriteRecord(1, []byte("invalid")); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	segments, err := j.FindSegments(Filter{})
	if err != nil || len(segments) != 0 {
		t.Fatal("extractor rejection created a segment")
	}
	if err = j.FinishWriting(); err != nil {
		t.Fatal(err)
	}
}
