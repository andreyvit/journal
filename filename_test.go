package journal

import (
	"testing"
)

func TestParseName(t *testing.T) {
	seg, err := parseSegmentName("", "", "F123-20230101T000000123-444")
	if err != nil {
		t.Fatal(err)
	}
	if e := uint64(123); seg.segnum != e {
		t.Errorf("seq = %v, expected %v", seg.segnum, e)
	}
	if e := uint64(1672531200123); seg.ts != e {
		t.Errorf("ts = %v, expected %v", seg.ts, e)
	}
	if e := uint64(444); seg.recnum != e {
		t.Errorf("rec = %d, expected %d", seg.recnum, e)
	}
	if e := Finalized; seg.status != e {
		t.Errorf("status = %d, expected %d", seg.status, e)
	}
}

func TestFormatName(t *testing.T) {
	seg := Segment{
		ts:     1672531260987,
		recnum: 444,
		segnum: 123,
		status: Sealed,
	}
	name := formatSegmentName("x", "y", seg)
	exp := "xS0000000123-20230101T000100987-000000000444y"
	if name != exp {
		t.Errorf("name = %q, expected %q", name, exp)
	}
}
