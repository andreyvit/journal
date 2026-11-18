package journal

import (
	"os"
	"testing"
)

func TestInspectSegmentsFileErrorDoesNotHideOtherFiles(t *testing.T) {
	j := New(t.TempDir(), Options{SyncOnCommit: true, UpdateSegmentMetadata: inspectionTestMetadata})
	defer j.AbortWriting()
	inspectionTestWrite(t, j, 101, true)
	if err := j.Rotate(); err != nil {
		t.Fatal(err)
	}
	inspectionTestWrite(t, j, 303, true)
	infos, err := j.InspectSegments()
	if err != nil || len(infos) != 2 {
		t.Fatalf("initial files: %+v %v", infos, err)
	}
	if err = os.Remove(j.filePath(infos[0].Name)); err != nil {
		t.Fatal(err)
	}
	infos, err = j.InspectSegments()
	if err != nil || len(infos) != 2 || infos[0].Err == nil || infos[1].Err != nil || infos[1].Metadata != (SegmentMetadata{303 % 256}) {
		t.Fatalf("partial files: %+v %v", infos, err)
	}
}
