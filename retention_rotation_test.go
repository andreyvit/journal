package journal

import "testing"

func TestFiniteCursorFollowsRotationWithoutReopeningWriter(t *testing.T) {
	j := New(t.TempDir(), Options{SyncOnCommit: true})
	if err := j.WriteRecord(1, []byte("record")); err != nil {
		t.Fatal(err)
	}
	if err := j.Commit(); err != nil {
		t.Fatal(err)
	}
	cursor := j.Read(Filter{MaxRecordID: 1})
	defer cursor.Close()
	var err error
	cursor.segments, err = j.FindSegments(Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Rotate(); err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || cursor.ID != 1 || string(cursor.Data) != "record" {
		t.Fatalf("finite rotation read: %v", cursor.Err())
	}
	if j.writer.writable || j.writer.segWriter != nil {
		t.Fatal("reader reopened the writer while resolving a rename")
	}
	if err = j.FinishWriting(); err != nil {
		t.Fatal(err)
	}
}
