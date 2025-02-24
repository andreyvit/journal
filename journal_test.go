package journal_test

import (
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/andreyvit/journal"
	"github.com/andreyvit/journal/internal/journaltest"
)

const draft = "'JOURNLAD"
const sealed = "'JOURNLAS"
const filler = "0*32/journal_inv 0*32/seg_inv 0...*3/reserved"

func TestJournalFlow_simple(t *testing.T) {
	j := journaltest.Writable(t, journal.Options{
		MaxFileSize: 165,
	})
	ensure(j.WriteRecord(0, []byte("hello")))
	j.Advance(1 * time.Second)
	ensure(j.WriteRecord(0, []byte("w")))
	ensure(j.FinishWriting())

	deepEq(t, j.FileNames(), []string{
		"jW0000000001-20240101T000000000-000000000001.wal",
	})

	j.StartWriting()
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

func TestJournalFlow_large(t *testing.T) {
	j := journaltest.Writable(t, journal.Options{
		MaxFileSize: 165,
	})
	ensure(j.WriteRecord(0, []byte("hello")))
	j.Advance(1 * time.Millisecond)
	ensure(j.WriteRecord(0, []byte("w")))
	j.Advance(1000 * time.Second)
	ensure(j.WriteRecord(0, []byte("foo")))
	ensure(j.WriteRecord(0, []byte("bar boz")))
	huge := strings.Repeat("huge", 100)
	ensure(j.WriteRecord(0, []byte(huge)))
	ensure(j.FinishWriting())

	for i := range 10 {
		j.Advance(25 * time.Millisecond)
		ensure(j.WriteRecord(0, []byte(fmt.Sprintf("record %d", i))))
		ensure(j.FinishWriting())
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
}

func TestJournalInternals(t *testing.T) {
	j := journaltest.Writable(t, journal.Options{
		MaxFileSize: 165,
	})
	ensure(j.WriteRecord(0, []byte("hello")))
	ensure(j.WriteRecord(0, []byte("w")))
	j.Advance(1000 * time.Second)
	ensure(j.WriteRecord(0, []byte("orld")))
	j.FinishWriting()

	log.Printf("%x", j.Now().UnixMilli())

	files := j.FileNames()
	deepEq(t, files, []string{
		"jW0000000001-20240101T000000000-000000000001.wal",
	})

	start1 := concat(
		"1../seg 0.. 00_f4_51_c2_8c_01.../ts 1.../rec",
		filler, "e5e8c2d95fbf79a1",
		"#10 #0 'hello",
		"#2 #0 'w",
		"#8 #1000000 'orld",
		"b13af5c6fe2b3b 6f",
	)
	j.Eq(files[0], draft, start1)

	j.StartWriting()
	j.Advance(10 * time.Second)
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
		draft, start1,
		"#6 #10000 'foo",
		"550a7e6d7e9ffbfa",
	)

	prestart2 := concat("2../seg 0.. 505d61c28c01.../ts 5.../rec", filler,
		"3b5e5a292ac3d51e")
	start2 := concat(
		prestart2,
		"#18 #0 'boooooooo",
		"4793427cfcf49ae0",
	)
	j.Eq(files[1],
		draft, start2,
		"#8 #0 'wooo",
		"bddd2aaa4d525004",
	)

	// recovery: missing checksum + continue writing
	j.Put(files[1], draft, start2,
		"#8 #0 'wooo",
		"5df47af8e96d296f")
	j.StartWriting()
	ensure(j.WriteRecord(0, []byte("x")))
	ensure(j.FinishWriting())
	j.Eq(files[1], draft, start2,
		"#2 #0 'x",
		"233ced0d2205baa9")

	// recovery: no commit
	j.Put(files[1], draft, start2,
		"#8 #0 'wooo")
	j.StartWriting()
	ensure(j.FinishWriting())
	j.Eq(files[1], draft, start2)

	// recovery: nonsensical data
	j.Put(files[1],
		draft, start2,
		"FE FF*100",
	)
	j.StartWriting()
	ensure(j.FinishWriting())
	j.Eq(files[1], draft, start2)

	// recovery: broken first record (file deleted)
	j.Put(files[1],
		draft,
		prestart2,
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
	j.Eq(files[1], draft, start2,
		"#2 #0 'x",
		"233ced0d2205baa9",
	)
}
