package journal_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/andreyvit/journal"
)

const draft = "'JOURNLAD"
const finalized = "'JOURNLAF"
const sealed = "'JOURNLAS"
const filler = "0*32/journal_inv 0*32/seg_inv 0.../reserved"

func TestJournalFlow_simple(t *testing.T) {
	clock := newClock()
	j := setupWritable(t, clock, journal.Options{
		MaxFileSize: 165,
	})
	ensure(j.WriteRecord(0, []byte("hello")))
	clock.Advance(1 * time.Second)
	ensure(j.WriteRecord(0, []byte("w")))
	ensure(j.FinishWriting())

	deepEq(t, j.FileNames(), []string{
		"jW0000000001-20240101T000000000-000000000001.wal",
	})

	ensure(j.Rotate())

	deepEq(t, j.FileNames(), []string{
		"jF0000000001-20240101T000000000-000000000001.wal",
	})

	recs := j.All(journal.Filter{})
	eq(t, len(recs), 2)
	eqstr(t, recs[0].Data, []byte("hello"))
	eq(t, recs[0].ID, 1)
	eq(t, recs[0].Timestamp, journal.ToTimestamp(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))
	eqstr(t, recs[1].Data, []byte("w"))
	eq(t, recs[1].ID, 2)
	eq(t, recs[1].Timestamp, journal.ToTimestamp(time.Date(2024, 1, 1, 0, 0, 1, 0, time.UTC)))
}

func TestJournalFlow_autorotate(t *testing.T) {
	clock := newClock()
	j := setupWritable(t, clock, journal.Options{
		MaxFileSize: 165,
		Autorotate: journal.AutorotateOptions{
			Interval: 1 * time.Hour,
		},
	})
	ensure(j.WriteRecord(0, []byte("hello")))
	clock.Advance(1 * time.Second)

	deepEq(t, j.FileNames(), []string{
		"jW0000000001-20240101T000000000-000000000001.wal",
	})
	eq(t, must(j.Autorotate(j.Now())), false)

	clock.Advance(1 * time.Hour)
	eq(t, must(j.Autorotate(j.Now())), true)

	deepEq(t, j.FileNames(), []string{
		"jF0000000001-20240101T000000000-000000000001.wal",
	})
}

func TestJournalFlow_summary(t *testing.T) {
	clock := newClock()
	j1 := setupWritable(t, clock, journal.Options{MaxFileSize: 165})
	ensure(j1.WriteRecord(0, []byte("hello")))
	clock.Advance(time.Second)
	ensure(j1.WriteRecord(0, []byte("w")))
	clock.Advance(time.Second)
	ensure(j1.WriteRecord(0, []byte(strings.Repeat("huge", 100))))
	clock.Advance(time.Second)
	ensure(j1.WriteRecord(0, []byte(strings.Repeat("another", 100))))
	clock.Advance(time.Second)
	ensure(j1.WriteRecord(0, []byte("x")))
	clock.Advance(time.Second)
	ensure(j1.WriteRecord(0, []byte("y")))
	ensure(j1.FinishWriting())
	s := must(j1.Summary())
	eq(t, s.FirstUnsealedSegment.SegmentNumber(), 1)
	eq(t, s.FirstUnsealedSegment.RecordNumber(), 1)
	eq(t, s.LastUnsealedSegment.SegmentNumber(), 4)
	eq(t, s.LastUnsealedSegment.RecordNumber(), 5)
	eq(t, s.LastCommitted.ID, 6)
	eq(t, s.LastCommitted.Timestamp, at("20240101T000005000"))

	j2 := open(t, clock, j1.Dir, journal.Options{MaxFileSize: 165})
	s = must(j2.Summary())
	eq(t, s.FirstUnsealedSegment.SegmentNumber(), 1)
	eq(t, s.FirstUnsealedSegment.RecordNumber(), 1)
	eq(t, s.LastUnsealedSegment.SegmentNumber(), 4)
	eq(t, s.LastUnsealedSegment.RecordNumber(), 5)
	eq(t, s.LastCommitted.ID, 6)
	eq(t, s.LastCommitted.Timestamp, at("20240101T000005000"))

}

func TestJournalFlow_large(t *testing.T) {
	clock := newClock()
	j := setupWritable(t, clock, journal.Options{
		MaxFileSize: 165,
	})
	ensure(j.WriteRecord(0, []byte("hello")))
	clock.Advance(1 * time.Millisecond)
	ensure(j.WriteRecord(0, []byte("w")))
	clock.Advance(1000 * time.Second)
	ensure(j.WriteRecord(0, []byte("foo")))
	ensure(j.WriteRecord(0, []byte("bar boz")))
	huge := strings.Repeat("huge", 100)
	ensure(j.WriteRecord(0, []byte(huge)))
	ensure(j.FinishWriting())

	for i := range 10 {
		clock.Advance(25 * time.Millisecond)
		ensure(j.WriteRecord(0, []byte(fmt.Sprintf("record %d", i))))
		s := must(j.Summary())
		eq(t, s.LastCommitted.ID, uint64(5+i))
		eq(t, s.LastUncommitted.ID, uint64(5+i+1))
		eq(t, s.LastUncommitted.Timestamp, clock.NowTS())
		ensure(j.FinishWriting())
		s = must(j.Summary())
		eq(t, s.LastCommitted.ID, uint64(5+i+1))
		eq(t, s.LastCommitted.Timestamp, clock.NowTS())
	}

	deepEq(t, j.FileNames(), []string{
		"jF0000000001-20240101T000000000-000000000001.wal", "jF0000000002-20240101T001640001-000000000005.wal", "jF0000000003-20240101T001640026-000000000006.wal", "jF0000000004-20240101T001640076-000000000008.wal", "jF0000000005-20240101T001640126-000000000010.wal", "jF0000000006-20240101T001640176-000000000012.wal", "jW0000000007-20240101T001640226-000000000014.wal",
	})

	expected := []string{"hello", "w", "foo", "bar boz", huge, "record 0", "record 1", "record 2", "record 3", "record 4", "record 5", "record 6", "record 7", "record 8", "record 9"}
	actual := j.All(journal.Filter{})
	if a, e := len(actual), len(expected); a != e {
		t.Errorf("** records: got %d, wanted %d", a, e)
	} else {
		t.Logf("✓ got %d records", a)
	}
	for i := range min(len(actual), len(expected)) {
		a := actual[i]
		if string(a.Data) != expected[i] {
			t.Errorf("** record %d: got %q, wanted %q", i, string(a.Data), expected[i])
		}
	}

	s := must(j.Summary())
	eq(t, s.FirstUnsealedSegment.SegmentNumber(), 1)
	eq(t, s.FirstUnsealedSegment.RecordNumber(), 1)
	eq(t, s.LastUnsealedSegment.SegmentNumber(), 7)
	eq(t, s.LastUnsealedSegment.RecordNumber(), 14)
	eq(t, s.LastCommitted.ID, 15)
	eq(t, s.LastCommitted.Timestamp, at("20240101T001640251"))
}

func TestJournalFlow_read_filter(t *testing.T) {
	clock := newClock()
	j1 := setupWritable(t, clock, journal.Options{MaxFileSize: 165})
	ensure(j1.WriteRecord(0, []byte("hello")))
	clock.Advance(time.Second)
	ensure(j1.WriteRecord(0, []byte("w")))
	clock.Advance(time.Second)
	ensure(j1.WriteRecord(0, []byte(strings.Repeat("huge", 100))))
	clock.Advance(time.Second)
	ensure(j1.WriteRecord(0, []byte(strings.Repeat("another", 100))))
	clock.Advance(time.Second)
	ensure(j1.WriteRecord(0, []byte("x")))
	clock.Advance(time.Second)
	ensure(j1.WriteRecord(0, []byte("y")))
	ensure(j1.FinishWriting())

	t.Run("minid", func(t *testing.T) {

	})
}

func TestJournalInternals(t *testing.T) {
	// very useful one-liner for this test:
	//
	//   pbpaste | perl -pe 's/\|.*$//; s/^\s*[\da-fA-F]{6,}\s+//; s/\R//g; s/\s+//g' | pbcopy

	clock := newClock()
	j := setupWritable(t, clock, journal.Options{
		MaxFileSize: 165,
	})
	ensure(j.WriteRecord(0, []byte("hello")))
	ensure(j.WriteRecord(0, []byte("w")))
	clock.Advance(1000 * time.Second)
	ensure(j.WriteRecord(0, []byte("orld")))
	j.FinishWriting()

	files := j.FileNames()
	deepEq(t, files, []string{
		"jW0000000001-20240101T000000000-000000000001.wal",
	})

	draftHeader1 := concat(
		draft,
		"1../seg 0.. 00_f4_51_c2_8c_01.../ts 1.../rec",
		"0.../ts 0.../rec",
		filler, "e5e8c2d95fbf79a1")
	finalHeader1 := concat(
		finalized,
		"1../seg 0.. 00_f4_51_c2_8c_01.../ts 1.../rec",
		"505d61c28c01.../ts 4.../rec",
		filler, "f4fd2d7929b255e4")

	start1 := concat(
		"#10 #0 'hello",
		"#2 #0 'w",
		"#8 #1000000 'orld",
		"9fb570ecfee2ce98",
	)
	j.Eq(files[0], draftHeader1, start1)

	j.StartWriting()
	clock.Advance(10 * time.Second)
	ensure(j.WriteRecord(0, []byte("foo")))
	ensure(j.WriteRecord(0, []byte("boooooooo")))
	ensure(j.Commit())
	ensure(j.WriteRecord(0, []byte("wooo")))
	ensure(j.FinishWriting())

	files = j.FileNames()
	deepEq(t, files, []string{
		"jF0000000001-20240101T000000000-000000000001.wal",
		"jW0000000002-20240101T001650000-000000000005.wal",
	})

	j.Eq(files[0],
		finalHeader1, start1,
		"#6 #10000 'foo",
		"e1c52d99a7ff1647",
	)

	draftHeader2 := concat(
		draft,
		"2../seg 0.. 505d61c28c01.../ts 5.../rec",
		"0.../ts 0.../rec",
		filler, "3b5e5a292ac3d51e")
	start2 := concat(
		"#18 #0 'boooooooo",
		"edad4b702d82aedc",
	)
	j.Eq(files[1],
		draftHeader2, start2,
		"#8 #0 'wooo",
		"c70e16920ec819a6",
	)

	// recovery: missing checksum + continue writing
	j.Put(files[1], draftHeader2, start2,
		"#8 #0 'wooo",
		"5df47af8e96d296f")
	j.StartWriting()
	ensure(j.WriteRecord(0, []byte("x")))
	ensure(j.FinishWriting())
	j.Eq(files[1], draftHeader2, start2,
		"#2 #0 'x",
		"db7c368a2184d721")

	// recovery: no commit
	j.Put(files[1], draftHeader2, start2,
		"#8 #0 'wooo")
	j.StartWriting()
	ensure(j.FinishWriting())
	j.Eq(files[1], draftHeader2, start2)

	// recovery: nonsensical data
	j.Put(files[1],
		draftHeader2, start2,
		"FE FF*100",
	)
	j.StartWriting()
	ensure(j.FinishWriting())
	j.Eq(files[1], draftHeader2, start2)

	// recovery: broken first record (file deleted)
	j.Put(files[1],
		draftHeader2,
		"#18 #0 'boooooooo",
	)
	j.StartWriting()
	ensure(j.FinishWriting())
	files = j.FileNames()
	deepEq(t, files, []string{
		"jF0000000001-20240101T000000000-000000000001.wal",
	})

	// ...and can continue writing
	ensure(j.WriteRecord(0, []byte("boooooooo")))
	ensure(j.Commit())
	ensure(j.WriteRecord(0, []byte("x")))
	ensure(j.FinishWriting())
	files = j.FileNames()
	deepEq(t, files, []string{
		"jF0000000001-20240101T000000000-000000000001.wal",
		"jW0000000002-20240101T001650000-000000000005.wal",
	})
	j.Eq(files[1], draftHeader2, start2,
		"#2 #0 'x",
		"db7c368a2184d721",
	)
}

func TestJournalFilter_latest(t *testing.T) {
	clock := newClock()
	j := setupWritable(t, clock, journal.Options{
		MaxFileSize: 165,
	})
	for i := range 100 {
		ensure(j.WriteRecord(0, fmt.Append(nil, i)))
		clock.Advance(1 * time.Second)
	}
	ensure(j.Commit())

	recs := j.All(journal.Filter{Limit: 5, Latest: true})
	eq(t, len(recs), 5)
	eqstr(t, recs[0].Data, []byte("95"))
	eqstr(t, recs[1].Data, []byte("96"))
	eqstr(t, recs[2].Data, []byte("97"))
	eqstr(t, recs[3].Data, []byte("98"))
	eqstr(t, recs[4].Data, []byte("99"))
}
