package journal_test

import (
	"context"
	"testing"
	"time"

	"github.com/andreyvit/journal"
)

func TestJournalSeal_simple(t *testing.T) {
	j := setupWritable(t, newClock(), journal.Options{
		MaxFileSize: 165,
	})
	ensure(j.WriteRecord(0, []byte("hello")))
	j.clock.Advance(1 * time.Second)
	ensure(j.WriteRecord(0, []byte("w")))
	j.clock.Advance(10 * time.Second)

	deepEq(t, j.FileNames(), []string{
		"jW0000000001-20240101T000000000-000000000001.wal",
	})
	seg := must(j.Seal(context.Background()))
	eq(t, seg.String(), "")

	ensure(j.Rotate())
	ensure(j.WriteRecord(0, []byte("foo")))
	ensure(j.FinishWriting())

	deepEq(t, j.FileNames(), []string{
		"jF0000000001-20240101T000000000-000000000001.wal",
		"jW0000000002-20240101T000011000-000000000003.wal",
	})

	seg = must(j.Seal(context.Background()))
	eq(t, seg.String(), "S0000000001-20240101T000000000-000000000001")
	deepEq(t, j.FileNames(), []string{
		"jF0000000001-20240101T000000000-000000000001.wal",
		"jS0000000001-20240101T000000000-000000000001.wal",
		"jW0000000002-20240101T000011000-000000000003.wal",
	})

	seg = must(j.Trim())
	eq(t, seg.String(), "F0000000001-20240101T000000000-000000000001")
	deepEq(t, j.FileNames(), []string{
		"jS0000000001-20240101T000000000-000000000001.wal",
		"jW0000000002-20240101T000011000-000000000003.wal",
	})

	seg = must(j.Trim())
	eq(t, seg.String(), "")

	t.Logf("Reading journal")
	recsEq(t, j.All(journal.Filter{}), 1,
		"20240101T000000000:hello",
		"20240101T000001000:w",
		"20240101T000011000:foo")
}

func TestJournalSeal_more(t *testing.T) {
	j := setupWritable(t, newClock(), journal.Options{
		MaxFileSize: 165,
	})

	writeSeq(j)

	must(j.SealAndTrimAll(context.Background()))
	deepEq(j.T, j.FileNames(), []string{
		"jS0000000001-20240101T000000000-000000000001.wal",
		"jS0000000002-20240101T000002000-000000000003.wal",
		"jS0000000003-20240101T000022000-000000000005.wal",
		"jS0000000004-20240101T000342000-000000000007.wal",
		"jW0000000005-20240101T020342000-000000000009.wal",
	})
	j.Put("jT0000000005-20240101T020342000-000000000009.wal", "0*24")

	recsEq(t, j.All(journal.Filter{}), 1, seqContent...)

	j2 := open(t, j.clock, j.Dir, journal.Options{MaxFileSize: 165})
	recsEq(t, j2.All(journal.Filter{}), 1, seqContent...)
}

func TestJournalSeal_bug_601(t *testing.T) {
	j := setupWritable(t, newClock(), journal.Options{
		MaxFileSize: 165,
	})
	writeN(j, 10)
	ensure(j.Journal.Rotate())
	must(j.SealAndTrimAll(context.Background()))
	deepEq(j.T, j.FileNames(), []string{
		"jS0000000001-20240101T000000000-000000000001.wal",
		"jS0000000002-20240101T000005000-000000000006.wal",
	})
	s := must(j.Journal.Summary())
	eq(t, s.LastUnsealedSegment.String(), "")
	eq(t, s.LastSealedSegment.String(), "S0000000002-20240101T000005000-000000000006")
	eq(t, s.LastCommitted.ID, 10)

	j = open(t, j.clock, j.Dir, journal.Options{MaxFileSize: 165})
	s = must(j.Journal.Summary())
	eq(t, s.LastUnsealedSegment.String(), "")
	eq(t, s.LastSealedSegment.String(), "S0000000002-20240101T000005000-000000000006")
	eq(t, s.LastCommitted.ID, 10)

	writeN(j, 10)
	ensure(j.Journal.Rotate())
	must(j.SealAndTrimAll(context.Background()))
	deepEq(j.T, j.FileNames(), []string{
		"jS0000000001-20240101T000000000-000000000001.wal",
		"jS0000000002-20240101T000005000-000000000006.wal",
		"jS0000000003-20240101T000010000-000000000011.wal",
		"jS0000000004-20240101T000015000-000000000016.wal",
	})

	j = open(t, j.clock, j.Dir, journal.Options{MaxFileSize: 165})
	writeN(j, 10)
	ensure(j.Journal.FinishWriting())
	deepEq(j.T, j.FileNames(), []string{
		"jS0000000001-20240101T000000000-000000000001.wal",
		"jS0000000002-20240101T000005000-000000000006.wal",
		"jS0000000003-20240101T000010000-000000000011.wal",
		"jS0000000004-20240101T000015000-000000000016.wal",
		"jF0000000005-20240101T000020000-000000000021.wal",
		"jW0000000006-20240101T000025000-000000000026.wal",
	})

	j = open(t, j.clock, j.Dir, journal.Options{MaxFileSize: 165})
	must(j.SealAndTrimAll(context.Background()))
	deepEq(j.T, j.FileNames(), []string{
		"jS0000000001-20240101T000000000-000000000001.wal",
		"jS0000000002-20240101T000005000-000000000006.wal",
		"jS0000000003-20240101T000010000-000000000011.wal",
		"jS0000000004-20240101T000015000-000000000016.wal",
		"jS0000000005-20240101T000020000-000000000021.wal",
		"jW0000000006-20240101T000025000-000000000026.wal",
	})
	ensure(j.Journal.Rotate())

	j = open(t, j.clock, j.Dir, journal.Options{MaxFileSize: 165})
	must(j.SealAndTrimAll(context.Background()))
	deepEq(j.T, j.FileNames(), []string{
		"jS0000000001-20240101T000000000-000000000001.wal",
		"jS0000000002-20240101T000005000-000000000006.wal",
		"jS0000000003-20240101T000010000-000000000011.wal",
		"jS0000000004-20240101T000015000-000000000016.wal",
		"jS0000000005-20240101T000020000-000000000021.wal",
		"jS0000000006-20240101T000025000-000000000026.wal",
	})
}

func BenchmarkJournalSeal(b *testing.B) {
	b.StopTimer()
	for range b.N {
		j := setupWritable(b, newClock(), journal.Options{
			MaxFileSize: 10 * 1024 * 1024,
		}, nonVerbose)

		data := make([]byte, 1024)

		for k := range 10 * 1024 {
			for i := range data {
				data[i] = byte(i ^ k)
			}
			j.WriteRecord(0, data)
		}

		b.StartTimer()
		must(j.SealAndTrimAll(context.Background()))
		b.StopTimer()
		deepEq(j.T, j.FileNames(), []string{
			"jS0000000001-20240101T000000000-000000000001.wal",
			"jW0000000002-20240101T000000000-000000010210.wal",
		})
	}
}

var seqContent = []string{
	"20240101T000000000:one",
	"20240101T000001000:two",
	"20240101T000002000:three",
	"20240101T000012000:four",
	"20240101T000022000:five",
	"20240101T000202000:six",
	"20240101T000342000:seven",
	"20240101T010342000:eight",
	"20240101T020342000:nine",
	"20240101T030342000:ten",
}

func writeSeq(j *testJournal) {
	j.T.Helper()

	ensure(j.WriteRecord(0, []byte("one")))
	j.clock.Advance(1 * time.Second)
	ensure(j.WriteRecord(0, []byte("two")))
	j.clock.Advance(1 * time.Second)
	ensure(j.Rotate())

	ensure(j.WriteRecord(0, []byte("three")))
	j.clock.Advance(10 * time.Second)
	ensure(j.WriteRecord(0, []byte("four")))
	j.clock.Advance(10 * time.Second)
	ensure(j.Rotate())

	ensure(j.WriteRecord(0, []byte("five")))
	j.clock.Advance(100 * time.Second)
	ensure(j.WriteRecord(0, []byte("six")))
	j.clock.Advance(100 * time.Second)
	ensure(j.Rotate())

	ensure(j.WriteRecord(0, []byte("seven")))
	j.clock.Advance(1 * time.Hour)
	ensure(j.WriteRecord(0, []byte("eight")))
	j.clock.Advance(1 * time.Hour)
	ensure(j.Rotate())

	ensure(j.WriteRecord(0, []byte("nine")))
	j.clock.Advance(1 * time.Hour)
	ensure(j.WriteRecord(0, []byte("ten")))
	j.clock.Advance(1 * time.Hour)
	ensure(j.FinishWriting())

	deepEq(j.T, j.FileNames(), []string{
		"jF0000000001-20240101T000000000-000000000001.wal",
		"jF0000000002-20240101T000002000-000000000003.wal",
		"jF0000000003-20240101T000022000-000000000005.wal",
		"jF0000000004-20240101T000342000-000000000007.wal",
		"jW0000000005-20240101T020342000-000000000009.wal",
	})
}

func writeN(j *testJournal, n int) {
	j.T.Helper()
	for range n {
		ensure(j.WriteRecord(0, []byte("test")))
		j.clock.Advance(1 * time.Second)
	}
}
