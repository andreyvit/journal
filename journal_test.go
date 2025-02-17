package journal_test

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/andreyvit/journal"
	"github.com/andreyvit/journal/internal/journaltest"
)

const magic = "'JOURNLAT"
const header1 = "0/ver 0/fmt 0_0/flags"
const header2 = "0*32/journal_inv 0*32/seg_inv 0...*2/reserved"

func TestJournalFlow_simple(t *testing.T) {
	j := journaltest.Writable(t, journal.Options{
		MaxFileSize: 165,
	})
	ensure(j.WriteRecord(0, []byte("hello")))
	j.Advance(1 * time.Second)
	ensure(j.WriteRecord(0, []byte("w")))
	ensure(j.FinishWriting())

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
	ensure(j.WriteRecord(0, []byte(strings.Repeat("huge", 100))))
	ensure(j.FinishWriting())

	for i := range 10 {
		j.Advance(25 * time.Millisecond)
		ensure(j.WriteRecord(0, []byte(fmt.Sprintf("record %d", i))))
		ensure(j.FinishWriting())
	}

	deepEq(t, j.FileNames(), []string{
		"j0000000001-20240101T000000000-000000000001.wal", "j0000000002-20240101T001640001-000000000005.wal", "j0000000003-20240101T001640026-000000000006.wal", "j0000000004-20240101T001640076-000000000008.wal", "j0000000005-20240101T001640126-000000000010.wal", "j0000000006-20240101T001640176-000000000012.wal", "j0000000007-20240101T001640226-000000000014.wal",
	})
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
		"j0000000001-20240101T000000000-000000000001.wal",
	})

	start0 := concat(
		shdr("1.. 80_00_92_65 1.../rec 0.../prev",
			"2d 84 3b 7e 1d d5 01 39"),
		"#10 #0 'hello",
		"#2 #0 'w",
		"#8 #1000 'orld",
		"4d f9 dc 24 ee 9d 3e c8",
	)
	j.Eq(files[0], start0)
	// t.Logf("journal:\n%v", journaltest.HexDump(j.Data(files[0]), -1))

	j.StartWriting()
	j.Advance(10 * time.Second)
	ensure(j.WriteRecord(0, []byte("foo")))
	ensure(j.WriteRecord(0, []byte("boooooooo")))
	ensure(j.Commit())
	ensure(j.WriteRecord(0, []byte("wooo")))
	ensure(j.FinishWriting())

	files = j.FileNames()
	deepEq(t, files, []string{
		"j0000000001-20240101T000000-000000000001.wal",
		"j0000000002-20240101T001650-000000000005.wal",
	})

	j.Eq(files[0],
		start0,
		"#6 #10 'foo",
		"d7 b2 72 73 e8 94 15 47",
	)

	header1 := shdr("2.. 72_04_92_65 5.../rec aa_43_0b_79_c4_38_f1_5f/prev",
		"b8 f3 06 71 e6 e8 44 ce")
	start1 := concat(
		header1,
		"#18 #0 'boooooooo",
		"05 6f fd dc 91 9a 37 34",
	)
	j.Eq(files[1],
		start1,
		"#8 #0 'wooo",
		"5d f4 7a f8 e9 6d 29 6e",
	)

	// recovery: missing checksum + continue writing
	j.Put(files[1],
		start1,
		"#8 #0 'wooo",
		"5d f4 7a f8 e9 6d 29 6f",
	)
	j.StartWriting()
	ensure(j.WriteRecord(0, []byte("x")))
	ensure(j.FinishWriting())
	j.Eq(files[1], start1,
		"#2 #0 'x",
		"61 1c ce dd a4 bb 40 43",
	)

	// recovery: no commit
	j.Put(files[1],
		start1,
		"#8 #0 'wooo",
	)
	j.StartWriting()
	ensure(j.FinishWriting())
	j.Eq(files[1], start1)

	// recovery: nonsensical data
	j.Put(files[1],
		start1,
		"FE FF*100",
	)
	j.StartWriting()
	ensure(j.FinishWriting())
	j.Eq(files[1], start1)

	// recovery: broken first record (file deleted)
	j.Put(files[1],
		header1,
		"#18 #0 'boooooooo",
	)
	j.StartWriting()
	ensure(j.FinishWriting())
	files = j.FileNames()
	deepEq(t, files, []string{
		"j0000000001-20240101T000000-000000000001.wal",
	})

	// ...and can continue writing
	ensure(j.WriteRecord(0, []byte("boooooooo")))
	ensure(j.Commit())
	ensure(j.WriteRecord(0, []byte("x")))
	ensure(j.FinishWriting())
	files = j.FileNames()
	deepEq(t, files, []string{
		"j0000000001-20240101T000000-000000000001.wal",
		"j0000000002-20240101T001650-000000000005.wal",
	})
	j.Eq(files[1], start1,
		"#2 #0 'x",
		"61 1c ce dd a4 bb 40 43",
	)
}

func shdr(inside, check string) string {
	return magic + " " + header1 + " " +
		inside + " " + header2 + " " + check
}

func concat(items ...string) string {
	return strings.Join(items, " ")
}

func deepEq[T any](t testing.TB, a, e T) bool {
	if !reflect.DeepEqual(a, e) {
		t.Helper()
		t.Errorf("** got %v, wanted %v", a, e)
		return false
	}
	return true
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func ensure(err error) {
	if err != nil {
		panic(err)
	}
}
